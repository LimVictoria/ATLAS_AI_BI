"""
ATLAS BI — LangGraph tool-calling agent (Path B)

Tools:
  sql_tool   — runs any DuckDB SQL, returns rows + auto-selects chart type
  chart_tool — builds Plotly JSON from rows + chart_type hint
  memory_tool — reads user preferences (future)

Flow:
  User message → agent decides SQL → sql_tool → chart_tool → narrative + ui_actions
  On SQL error → agent retries with corrected SQL (up to 3x)
"""
import json
import re
import pandas as pd
from db.duckdb_session import run_query
from api.query import _build_chart, _to_json, _clean_df

# ── Schema guide injected into system prompt ───────────────────────────────────
# This replaces hardcoded metric picking. LLM reads this to know the data model.

SCHEMA_GUIDE = """
DATABASE VIEW: v_maintenance_full
All queries run against this single view. Use DuckDB SQL syntax.

COLUMNS:
  plate_number       TEXT    — unique vehicle identifier (e.g. "WXY1234")
  brand              TEXT    — truck brand: Scania, Volvo, Mercedes-Benz, MAN, Hino
  fleet_segment      TEXT    — Heavy, Medium, Light
  year_manufactured  INTEGER — year truck was made
  truck_age_years    INTEGER — age of truck in years
  service_date       DATE    — date of maintenance event
  year               INTEGER — year extracted from service_date
  month              INTEGER — month extracted from service_date (1-12)
  year_month         TEXT    — e.g. "2024-03"
  month_name         TEXT    — e.g. "March"
  year_quarter       TEXT    — e.g. "2024-Q1"
  maintenance_type   TEXT    — Scheduled, Unscheduled
  component_category TEXT    — Engine, Transmission, Brakes, Electrical, Tyres, Body, Suspension, Cooling
  component_name     TEXT    — specific component
  failure_type       TEXT    — type of failure
  criticality_level  TEXT    — Critical, High, Medium, Low
  workshop_name      TEXT    — name of workshop
  workshop_type      TEXT    — Authorised, Independent
  workshop_auth      TEXT    — Authorised, Independent (same as workshop_type)
  region             TEXT    — geographic region
  total_cost_myr     FLOAT   — total maintenance cost in MYR
  parts_cost_myr     FLOAT   — parts cost in MYR
  labour_cost_myr    FLOAT   — labour cost in MYR
  downtime_days      FLOAT   — days vehicle was out of service
  is_repeat_failure  BOOLEAN — whether this is a repeat failure

DATE RANGE: 2020-01-01 to 2024-12-31
"This month" = December 2024. "This year" = 2024. "Last year" = 2023.

SQL RULES:
- Always use strftime(service_date, '%Y') for year extraction
- Always use strftime(service_date, '%m') for month extraction  
- Use ROUND(value, 2) for monetary values
- Use NULLIF(denominator, 0) to avoid division by zero
- For date filters: WHERE service_date >= '2024-01-01' AND service_date <= '2024-12-31'
- GROUP BY all non-aggregated columns in SELECT
- ORDER BY the main measure DESC unless time-series (then ASC)
- Limit results to 50 rows max for visualisation: add LIMIT 50

EXAMPLE QUERIES:
-- Cost by brand
SELECT brand, ROUND(SUM(total_cost_myr),2) AS total_cost, COUNT(*) AS events,
       COUNT(DISTINCT plate_number) AS fleet_size
FROM v_maintenance_full GROUP BY brand ORDER BY total_cost DESC

-- Monthly trend
SELECT strftime(service_date,'%Y-%m') AS year_month,
       ROUND(SUM(total_cost_myr),2) AS total_cost
FROM v_maintenance_full GROUP BY year_month ORDER BY year_month ASC

-- Vehicle + component + month breakdown
SELECT plate_number AS vehicle_id, brand, component_category,
       CAST(strftime(service_date,'%Y') AS INTEGER) AS year,
       CAST(strftime(service_date,'%m') AS INTEGER) AS month,
       ROUND(SUM(total_cost_myr),2) AS total_cost
FROM v_maintenance_full
GROUP BY plate_number, brand, component_category, year, month
ORDER BY year, month, total_cost DESC LIMIT 50
"""


# ── Chart type auto-selector ───────────────────────────────────────────────────

def auto_chart_type(columns: list[str], hint: str = None) -> str:
    """Pick the best chart type based on columns returned and optional LLM hint."""
    if hint and hint in ["bar","line","pie","table","pareto","waterfall","heatmap","boxplot","scatter","treemap","histogram","stacked_bar"]:
        return hint
    cols = [c.lower() for c in columns]
    # Time series
    if any(c in cols for c in ["year_month","month_name","year_quarter","service_date"]):
        return "line"
    # Two categorical + one numeric = stacked bar candidate
    if sum(1 for c in cols if c in ["brand","component_category","fleet_segment","region","workshop_type"]) >= 2:
        return "stacked_bar"
    # Many rows → table
    return "bar"


def infer_chart_meta(columns: list[str], chart_type: str) -> dict:
    """Infer x_col, y_col, group_col from column names for _build_chart."""
    numeric = []
    categorical = []
    time_cols = []
    group_candidates = ["component_category","fleet_segment","maintenance_type","workshop_type","region","failure_type"]

    for c in columns:
        cl = c.lower()
        if cl in ["year_month","month_name","year_quarter","service_date","year_month"]:
            time_cols.append(c)
        elif cl in ["brand","plate_number","vehicle_id","workshop_name","component_name"] + group_candidates:
            categorical.append(c)
        elif cl not in ["year","month","id"]:
            numeric.append(c)

    x_col = time_cols[0] if time_cols else (categorical[0] if categorical else columns[0])
    y_col = numeric[0] if numeric else (columns[-1] if len(columns) > 1 else columns[0])
    group_col = next((c for c in columns if c.lower() in group_candidates), None)

    return {
        "x_col": x_col,
        "y_col": y_col,
        "group_col": group_col,
        "category": "Cost",  # default; LLM can override via title
    }


# ── sql_tool ──────────────────────────────────────────────────────────────────

def sql_tool(sql: str, chart_type_hint: str = None, title: str = "Query Result", category: str = "Cost") -> dict:
    """
    Run SQL against DuckDB and return chart JSON + metadata.
    Returns dict with keys: chart, chart_type, row_count, summary, columns, sql, title, category, error
    """
    try:
        df = run_query(sql)
    except Exception as e:
        return {"error": str(e), "sql": sql}

    if df.empty:
        return {"error": "Query returned no rows", "sql": sql}

    columns = df.columns.tolist()
    chart_type = auto_chart_type(columns, chart_type_hint)
    meta = infer_chart_meta(columns, chart_type)
    meta["category"] = category

    # For table type, no x/y needed
    try:
        chart_json = _build_chart(df, meta, chart_type)
    except Exception as e:
        # Fallback to table if chart building fails
        try:
            chart_json = _build_chart(df, meta, "table")
            chart_type = "table"
        except Exception as e2:
            return {"error": f"Chart build failed: {e2}", "sql": sql}

    # Available chart types based on data shape
    available = ["table"]
    if len(df) <= 20:
        available += ["bar", "pie"]
    if any(c.lower() in ["year_month","month_name","year_quarter"] for c in columns):
        available += ["line"]
    if meta.get("group_col"):
        available += ["stacked_bar"]
    if chart_type not in available:
        available.append(chart_type)

    return {
        "chart":            chart_json,
        "chart_type":       chart_type,
        "row_count":        len(df),
        "summary":          df.head(5).to_dict(orient="records"),
        "columns":          columns,
        "sql":              sql,
        "title":            title,
        "category":         category,
        "available_charts": list(set(available)),
        "error":            None,
    }


# ── Agent system prompt ────────────────────────────────────────────────────────

def build_agent_system_prompt(board_context_str: str = "", user_memory: str = "") -> str:
    return f"""You are ATLAS, an expert AI analyst for a Malaysian truck fleet maintenance platform.

{SCHEMA_GUIDE}

{board_context_str}

{f"USER PREFERENCES:{user_memory}" if user_memory else ""}

YOUR JOB:
1. Understand what the user wants to know
2. Write a DuckDB SQL query to get the data
3. Specify the best chart type to visualise it
4. Write a clear 1-3 sentence narrative explaining the insight

RESPONSE FORMAT — return ONLY valid JSON, no markdown, no code blocks:
{{
  "narrative": "Clear English explanation. No JSON, no curly braces, no markdown tables.",
  "sql": "SELECT ... FROM v_maintenance_full ...",
  "chart_type": "bar",
  "title": "Short descriptive chart title",
  "category": "Cost",
  "filters_used": {{}},
  "ui_action": "add_chart",
  "card_id": null
}}

ui_action values:
- "add_chart" — add a new chart to the board (default for data questions)
- "modify_chart" — change an existing card (only when user says change/switch/modify AND a card is SELECTED)
- "none" — conversational answer only, no chart needed

card_id: only set this when ui_action is "modify_chart" — use the selected card's id from BOARD_CONTEXT

CATEGORY values: Cost, Downtime, Failure, Fleet, Workshop, Time

CHART TYPE GUIDE:
- bar: comparisons between categories (default)
- line: trends over time (when query has year_month, year_quarter)
- pie: proportions (when query has 2-5 categories summing to 100%)
- table: detailed row data (when user asks for a table, or many columns)
- stacked_bar: breakdown by sub-group (when query has brand + component_category)
- heatmap: 2D intensity (brand × month)
- scatter: correlation between 2 numeric columns
- pareto: 80/20 analysis
- waterfall: cumulative buildup
- boxplot: distribution with quartiles
- histogram: frequency distribution
- treemap: hierarchical proportions

CONVERSATIONAL RULES:
- "why", "explain", "is it because" → set ui_action to "none", answer in narrative only using BOARD_CONTEXT data_preview
- "show me", "visualise", "chart", "build a table" → set ui_action to "add_chart"
- If board already has a chart for the same data → set ui_action to "none", reference the existing card by title
- NEVER print a markdown table in narrative — use ui_action "add_chart" with chart_type "table" instead
- NEVER say "I've added a chart" if ui_action is "none"

SQL SELF-CORRECTION:
- If you are unsure of a column name, use only column names listed in the SCHEMA GUIDE above
- Always test your SQL mentally — does every column in SELECT appear in GROUP BY (if not aggregated)?
"""
