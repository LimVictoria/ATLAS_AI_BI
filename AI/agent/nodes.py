"""
ATLAS BI — LangGraph Agent Nodes
Graph: intent_node → sql_node → [correction_node] → chart_node → respond_node → memory_node
       intent_node → narrator_node → memory_node
       intent_node → board_node → [chart_node] → memory_node
       intent_node → meta_node → memory_node
"""
import json
import re
import traceback
import pandas as pd
from typing import TypedDict, Annotated, Optional
import operator
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from db.duckdb_session import run_query
from api.query import _build_chart, _to_json, _clean_df

# ── Schema guide ───────────────────────────────────────────────────────────────
def _get_schema_guide() -> str:
    try:
        from db.duckdb_session import get_schema_guide
        return get_schema_guide()
    except Exception as e:
        print(f"[nodes] schema guide fallback: {e}")
        return "SCHEMA: Data loaded in DuckDB. Query tables using SQL."

SCHEMA_GUIDE = property(_get_schema_guide)
TIME_COLS   = {"year_month","month_name","year_quarter","service_date","year","month"}
CAT_COLS    = {"brand","plate_number","vehicle_id","workshop_name","component_name",
               "component_category","fleet_segment","maintenance_type","workshop_type",
               "region","failure_type","criticality_level"}
NUMERIC_TYPES = {"int64","float64","int32","float32"}

# ── ATLAS self-description ─────────────────────────────────────────────────────
ATLAS_IDENTITY = """
ATLAS (Advanced Transport & Logistics Analytics System) is an AI-powered BI platform
for fleet maintenance analytics. Built on Anthropic Claude, running on a LangGraph agent.

WHAT ATLAS CAN DO:
- Visualise fleet maintenance data as bar, line, pie, table, heatmap, scatter, pareto, waterfall, treemap, boxplot, histogram, stacked bar charts
- Answer questions about maintenance costs, downtime, reliability, MTBF, workshop performance
- Apply filters per card (brand, year, month, fleet segment, maintenance type, criticality, workshop, region, component)
- Switch chart types, pivot tables to wide format, compare year-over-year trends
- Show predefined KPI metrics: Total Cost, MTBF, Warranty Recovery Rate, Unscheduled Rate, and more
- Flip any card to see the SQL query and star schema source SQL

DATA AVAILABLE:
- v_maintenance_full: 4,183 maintenance events × 45 columns (brands, components, workshops, costs, downtime)
- Covers: Scania, Volvo, Mercedes-Benz, MAN, Hino trucks
- Date range: 2020–2024
- Metrics: total_cost_myr, parts_cost_myr, labour_cost_myr, downtime_hours, labour_hours, current_mileage_km

HOW TO USE:
- Type any question about fleet maintenance (e.g. "show total cost by brand")
- Select a chart card and ask to modify it
- Use the filter panel on each card to slice data
- Flip a card (code icon) to see the SQL

TAGLINE: Every decision, grounded.
""".strip()

# ── State definition ───────────────────────────────────────────────────────────
class AgentState(TypedDict):
    user_message:    str
    history:         list[dict]
    board_context:   str
    user_memory:     str
    intent:          str
    selected_card_id: Optional[str]
    sql:             str
    sql_error:       str
    sql_retries:     int
    df_rows:         list[dict]
    df_columns:      list[str]
    chart_json:      str
    chart_type:      str
    available_charts: list[str]
    chart_title:     str
    chart_category:  str
    narrative:       str
    ui_actions:      list[dict]
    replace_card_id: Optional[str]
    long_sql:        str
    wide_sql:        str
    is_wide:         bool
    pivot_col:       str

# ── Helpers ────────────────────────────────────────────────────────────────────
def _safe_rows(df) -> list[dict]:
    import math
    result = []
    for row in df.to_dict(orient="records"):
        safe = {}
        for k, v in row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                safe[k] = 0
            elif v is None:
                safe[k] = ""
            else:
                safe[k] = v
        result.append(safe)
    return result

def _detect_pivot_intent(msg: str) -> bool:
    msg = msg.lower()
    pivot_phrases = [
        "columns for", "as columns", "column for each", "column per",
        "pivot", "wide format", "show as columns", "breakdown across",
        "each segment as", "each brand as", "each year as", "each month as",
        "per segment", "per brand", "by column", "column by",
        "wide table", "cross tab", "crosstab",
    ]
    return any(p in msg for p in pivot_phrases)

async def _build_pivot_sql(base_sql: str, pivot_col: str, measure_col: str, row_col: str) -> tuple[str, list]:
    try:
        import re
        table_match = re.search(r'FROM\s+(\w+)', base_sql, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else "v_maintenance_full"
        discovery_sql = f"SELECT DISTINCT {pivot_col} FROM {table_name} WHERE {pivot_col} IS NOT NULL ORDER BY {pivot_col}"
        distinct_df = run_query(discovery_sql)
        distinct_vals = distinct_df.iloc[:, 0].tolist()
        if not distinct_vals or len(distinct_vals) > 20:
            return "", distinct_vals
        def col_alias(v):
            return re.sub(r'[^a-zA-Z0-9]', '_', str(v)).strip('_').lower()
        sep = ",\n       "
        cases = sep.join(
            "ROUND(SUM(CASE WHEN " + pivot_col + " = '" + str(v) + "' THEN " + measure_col + " ELSE 0 END), 2) AS " + col_alias(v)
            for v in distinct_vals
        )
        where_match = re.search(r'WHERE(.+?)(?=GROUP|ORDER|HAVING|LIMIT|$)', base_sql, re.IGNORECASE | re.DOTALL)
        where_clause = f"WHERE {where_match.group(1).strip()}" if where_match else ""
        wide_sql = (
            "SELECT " + row_col + ",\n       " + cases + "\n"
            "FROM " + table_name + "\n"
            + (where_clause + "\n" if where_clause else "")
            + "GROUP BY " + row_col + "\n"
            + "ORDER BY SUM(" + measure_col + ") DESC"
        )
        return wide_sql.strip(), distinct_vals
    except Exception as e:
        print(f"[pivot] build failed: {e}")
        return "", []

def _infer_meta(columns: list[str]) -> dict:
    time_found, cat_found, num_found, group_found = [], [], [], []
    for c in columns:
        cl = c.lower()
        if cl in TIME_COLS:
            time_found.append(c)
        elif cl in CAT_COLS:
            if cl in {"component_category","fleet_segment","maintenance_type",
                      "workshop_type","failure_type","criticality_level","region"}:
                group_found.append(c)
            else:
                cat_found.append(c)
    for c in columns:
        cl = c.lower()
        if c not in time_found + cat_found + group_found and cl not in {"year","month","id"}:
            num_found.append(c)
    if time_found:
        cols_lower_list = [c.lower() for c in time_found]
        if "year_month" in cols_lower_list:
            x_col = time_found[cols_lower_list.index("year_month")]
        elif "year_quarter" in cols_lower_list:
            x_col = time_found[cols_lower_list.index("year_quarter")]
        else:
            x_col = time_found[0]
    else:
        x_col = cat_found[0] if cat_found else columns[0]
    y_col = num_found[0] if num_found else (columns[-1] if len(columns) > 1 else columns[0])
    group_col = group_found[0] if group_found and len(cat_found) > 0 else None
    return {"x_col": x_col, "y_col": y_col, "group_col": group_col}

def _smart_available_charts(columns: list[str], df: pd.DataFrame, chart_type: str) -> list[str]:
    num_cols  = [c for c in columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols  = [c for c in columns if not pd.api.types.is_numeric_dtype(df[c])
                 and not pd.api.types.is_datetime64_any_dtype(df[c])]
    time_cols = [c for c in columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    for c in columns:
        if c.lower() in TIME_COLS and c not in time_cols:
            time_cols.append(c)
    n_num, n_cat, n_time = len(num_cols), len(cat_cols), len(time_cols)
    stat_names = {"q1","q3","median","mean","std","min","max",
                  "cost_q1","cost_median","cost_q3","cost_min","cost_max","cost_mean"}
    has_stats = any(c.lower() in stat_names for c in columns)
    max_cat_unique = max((df[c].nunique() for c in cat_cols), default=0)
    available = ["table"]
    if has_stats:
        available += ["boxplot", "bar"]
    if n_num >= 1 and n_cat >= 1:
        available += ["bar", "pareto", "waterfall"]
        if max_cat_unique <= 12:
            available += ["pie"]
        available += ["treemap"]
    if n_time >= 1 and n_num >= 1:
        available += ["line", "bar"]
    if n_cat >= 2 and n_num >= 1:
        available += ["stacked_bar", "heatmap"]
    if n_num >= 2:
        available += ["scatter"]
    if n_num >= 1:
        available += ["histogram"]
    if chart_type and chart_type not in available:
        available.append(chart_type)
    return list(dict.fromkeys(available))

def _auto_chart_type(columns: list[str], hint: str = None) -> str:
    valid = {"bar","line","pie","table","pareto","waterfall","heatmap",
             "boxplot","scatter","treemap","histogram","stacked_bar"}
    if hint and hint in valid:
        return hint
    cols_lower = [c.lower() for c in columns]
    if any(c in TIME_COLS for c in cols_lower):
        return "line"
    if any(c in {"component_category","fleet_segment"} for c in cols_lower) and \
       any(c in {"brand","workshop_name"} for c in cols_lower):
        return "stacked_bar"
    if any(c in {"q1","median","q3"} for c in cols_lower):
        return "boxplot"
    return "bar"

# ── Groq helper ────────────────────────────────────────────────────────────────
import os, httpx, asyncio
GROQ_API_KEY        = os.getenv("GROQ_API_KEY","")
GROQ_MODEL          = "llama-3.3-70b-versatile"
GROQ_MODEL_FALLBACK = "llama-3.1-8b-instant"
GROQ_URL            = "https://api.groq.com/openai/v1/chat/completions"

async def _groq(messages: list[dict], model: str = None, max_tokens: int = 1500) -> str:
    chosen = model or GROQ_MODEL
    async with httpx.AsyncClient(timeout=45) as client:
        for attempt in range(3):
            resp = await client.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": chosen, "messages": messages, "temperature": 0.1, "max_tokens": max_tokens},
            )
            if resp.status_code == 429:
                wait = min(float(resp.headers.get("retry-after", 2*(attempt+1))), 8)
                print(f"[Groq] 429 on {chosen}, waiting {wait}s")
                await asyncio.sleep(wait)
                if chosen != GROQ_MODEL_FALLBACK:
                    chosen = GROQ_MODEL_FALLBACK
                continue
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
    raise Exception("Groq rate limit exceeded after 3 attempts.")

def _parse_json(raw: str) -> dict:
    clean = raw.strip()
    if clean.startswith("```"):
        lines = clean.split("\n")
        clean = "\n".join(lines[1:])
        if clean.endswith("```"):
            clean = clean[:-3]
    return json.loads(clean.strip())

# ── Node 1: intent_node ────────────────────────────────────────────────────────
async def intent_node(state: AgentState) -> AgentState:
    msg   = state["user_message"].lower()
    board = state.get("board_context", "")

    has_selected = "SELECTED CARD:" in board and "none" not in board.split("SELECTED CARD:")[-1][:20]
    selected_card_id = None
    if has_selected:
        m = re.search(r"id=([a-f0-9\-]+)", board.split("SELECTED CARD:")[-1])
        if m:
            selected_card_id = m.group(1)

    # Meta/identity questions — ATLAS should know about itself
    meta_phrases = {
        "who are you", "what are you", "what is atlas", "what can you do",
        "what is this", "tell me about yourself", "what do you know",
        "help", "how does this work", "what can atlas do", "introduce yourself",
        "are you an ai", "are you a chatbot", "what is your name",
        "do you know that you are", "atlas ai", "atlas bi",
    }
    if any(p in msg for p in meta_phrases) or msg.strip() in {"help", "hi", "hello", "hey"}:
        print(f"[intent_node] intent='meta'")
        return {**state, "intent": "meta", "selected_card_id": None}

    explain_words = {"why","explain","reason","cause","because","how come","what makes"}
    board_words   = {"how many charts","what charts","list my charts","what's on","what is on"}
    modify_words  = {"change","switch","modify","convert","make it","turn into","update","add column",
                     "add columns","another column","more columns","include","can we have","can you add",
                     "pivot","breakdown","split by","group by year","per year","by year"}

    intent = "visualise"
    if any(w in msg for w in explain_words):
        intent = "explain"
    elif any(w in msg for w in board_words):
        intent = "board"
    elif has_selected and any(w in msg for w in modify_words):
        intent = "modify"
    elif has_selected and intent == "visualise":
        intent = "modify"

    print(f"[intent_node] intent={intent!r} selected_card={selected_card_id!r} has_selected={has_selected}")
    return {**state, "intent": intent, "selected_card_id": selected_card_id}

# ── Node 2: sql_node ───────────────────────────────────────────────────────────
async def sql_node(state: AgentState) -> AgentState:
    memory_hint = ""
    raw_memory = state.get("user_memory", "")
    if raw_memory:
        try:
            m = json.loads(raw_memory) if isinstance(raw_memory, str) else raw_memory
            hints = []
            if m.get("preferred_chart"):
                hints.append(f"User prefers {m['preferred_chart']} charts — use unless data clearly needs another")
            if m.get("focus_brands"):
                hints.append(f"User focuses on brands: {', '.join(m['focus_brands'])} — mention in narrative if relevant, do NOT add WHERE unless explicitly asked")
            # focus_years intentionally excluded — never add year WHERE from memory
            if hints:
                memory_hint = "\n\nUSER PREFERENCES (apply subtly):\n" + "\n".join(f"- {h}" for h in hints)
        except Exception:
            pass

    try:
        from api.metrics import get_metrics_guide
        metrics_guide = get_metrics_guide()
    except Exception:
        metrics_guide = ""

    system = f"""You are a DuckDB SQL expert for a fleet maintenance BI platform.
{_get_schema_guide()}
{metrics_guide}
{state.get("board_context", "")}{memory_hint}

Return ONLY valid JSON — no markdown, no code blocks:
{{
  "sql": "SELECT ...",
  "chart_type": "bar",
  "title": "Short descriptive title",
  "category": "Cost",
  "narrative_hint": "One sentence about what this shows"
}}

SQL FILTER RULES - CRITICAL:
- NEVER add WHERE clauses based on memory or assumptions
- ONLY add WHERE if the user EXPLICITLY mentions a filter value (e.g. "show 2023 data", "filter to Scania only")
- "show total cost by brand" = NO WHERE clause — show all data
- "show total cost by brand for 2023" = WHERE year = 2023
- When in doubt, write SQL with NO WHERE — let the user apply filters themselves
- ALWAYS query v_maintenance_full — never source tables directly

CHART TYPE GUIDE:
- line: when query groups by year_month, year_quarter, month_name (time series)
- bar: category comparisons (brand, component, workshop)
- stacked_bar: when query has 2 categorical dims (brand + component_category)
- table: when user asks for a table, or query has many columns (>4)
- scatter: when query returns 2 numeric columns for correlation
- pie: proportions, max 8 categories
- heatmap: when query has brand x month
- pareto: 80/20 failure/cost analysis
- waterfall: cumulative cost buildup
- boxplot: distribution with quartiles (needs q1, median, q3 columns)
- histogram: frequency distribution
- treemap: hierarchical proportions"""

    retry_context = ""
    if state.get("sql_error"):
        retry_context = f"\n\nPREVIOUS SQL FAILED with error: {state['sql_error']}\nPrevious SQL was: {state.get('sql','')}\nPlease fix the SQL — only use column names from the SCHEMA GUIDE above."

    messages = [
        {"role": "system", "content": system + retry_context},
        *[{"role": m["role"], "content": m["content"]} for m in state.get("history", [])[-6:]],
        {"role": "user", "content": state["user_message"]},
    ]
    try:
        raw    = await _groq(messages, max_tokens=800)
        parsed = _parse_json(raw)
        sql    = parsed.get("sql", "").strip()
        if not sql.upper().startswith("SELECT"):
            raise ValueError(f"LLM returned non-SELECT SQL: {sql[:80]}")
        print(f"[sql_node] SQL={sql[:80]}...")

        long_sql = sql; wide_sql = ""; is_wide = False; pivot_col = ""
        # Never auto-pivot when chart type is stacked_bar, heatmap, or line
        # These chart types need the long format — pivoting destroys their data shape
        _no_pivot_types = {"stacked_bar", "heatmap", "line", "scatter", "boxplot"}
        try:
            import re as _re
            gb_match = _re.search(r'GROUP\s+BY\s+(.+?)(?=ORDER|HAVING|LIMIT|$)', sql, _re.IGNORECASE | _re.DOTALL)
            if gb_match and parsed.get("chart_type", "bar") not in _no_pivot_types:
                gb_cols = [c.strip() for c in gb_match.group(1).split(",")]
                if len(gb_cols) == 2:
                    row_col = gb_cols[0].split(".")[-1].strip()
                    pivot_col = gb_cols[1].split(".")[-1].strip()
                    select_match = _re.search(r'SELECT\s+(.+?)\s+FROM', sql, _re.IGNORECASE | _re.DOTALL)
                    if select_match:
                        measure_col = None
                        for part in select_match.group(1).split(","):
                            part = part.strip()
                            if any(agg in part.upper() for agg in ["SUM(","AVG(","COUNT(","MAX(","MIN("]):
                                alias_m = _re.search(r'AS\s+(\w+)\s*$', part, _re.IGNORECASE)
                                measure_col = alias_m.group(1) if alias_m else (_re.search(r'\((.+?)\)', part) or [None,None])[1]
                                if measure_col: measure_col = measure_col.split(".")[-1].strip()
                                break
                        if measure_col:
                            built_wide, distinct_vals = await _build_pivot_sql(sql, pivot_col, measure_col, row_col)
                            if built_wide:
                                wide_sql = built_wide; is_wide = True
                                print(f"[sql_node] Pivot detected — pivot_col={pivot_col!r} vals={distinct_vals}")
        except Exception as pivot_err:
            print(f"[sql_node] Pivot detection error (non-fatal): {pivot_err}")

        active_sql = wide_sql if (is_wide and wide_sql) else long_sql
        return {
            **state,
            "sql": active_sql, "long_sql": long_sql, "wide_sql": wide_sql,
            "is_wide": is_wide, "pivot_col": pivot_col, "sql_error": "",
            "chart_type": parsed.get("chart_type", "bar"),
            "chart_title": parsed.get("title", "Query Result"),
            "chart_category": parsed.get("category", "Cost"),
            "narrative": parsed.get("narrative_hint", ""),
        }
    except Exception as e:
        print(f"[sql_node] ERROR: {e}")
        return {**state, "sql_error": str(e), "sql": ""}

# ── Node 3: correction_node ────────────────────────────────────────────────────
async def correction_node(state: AgentState) -> AgentState:
    retries = state.get("sql_retries", 0) + 1
    error   = state.get("sql_error", "")
    print(f"[correction_node] retry {retries}/3 — error: {error}")
    error_hint = error
    if "not found in FROM clause" in error or "Referenced column" in error:
        error_hint = f"{error}\n-> Use ONLY columns from v_maintenance_full listed in the SCHEMA GUIDE."
    elif "strftime" in error.lower() or "service_date" in error.lower():
        error_hint = f"{error}\n-> Do NOT use strftime() or service_date. Use year (INTEGER) and month (INTEGER) columns."
    elif "syntax error" in error.lower():
        error_hint = f"{error}\n-> Fix the SQL syntax. Check for missing commas or unclosed parentheses."
    elif "ambiguous" in error.lower():
        error_hint = f"{error}\n-> Qualify ambiguous columns: v_maintenance_full.column_name"
    return {**state, "sql_retries": retries, "sql_error": error_hint}

# ── Node 4: chart_node ─────────────────────────────────────────────────────────
async def chart_node(state: AgentState) -> AgentState:
    sql = state.get("sql", "")
    if not sql:
        return {**state, "sql_error": "No SQL generated", "df_rows": [], "df_columns": []}
    try:
        df = run_query(sql)
    except Exception as e:
        print(f"[chart_node] SQL error: {e}")
        return {**state, "sql_error": str(e), "df_rows": [], "df_columns": []}
    if df.empty:
        return {**state, "sql_error": "Query returned no rows", "df_rows": [], "df_columns": []}
    df   = _clean_df(df)
    cols = df.columns.tolist()
    meta = _infer_meta(cols)
    meta["category"] = state.get("chart_category", "Cost")
    chart_type = state.get("chart_type", "bar")
    cols_lower = [c.lower() for c in cols]
    if chart_type == "scatter" and not (sum(1 for c in cols if str(df[c].dtype) in NUMERIC_TYPES) >= 2):
        chart_type = "bar"
    if chart_type == "line" and not any(c in TIME_COLS for c in cols_lower):
        chart_type = "bar"
    if chart_type == "stacked_bar" and not meta.get("group_col"):
        chart_type = "bar"
    try:
        chart_json = _build_chart(df, meta, chart_type)
    except Exception as e:
        print(f"[chart_node] chart build failed ({chart_type}): {e}, falling back to table")
        try:
            chart_json = _build_chart(df, meta, "table"); chart_type = "table"
        except Exception as e2:
            return {**state, "sql_error": f"Chart build failed: {e2}", "df_rows": [], "df_columns": []}
    available = _smart_available_charts(cols, df, chart_type)
    print(f"[chart_node] chart_type={chart_type} rows={len(df)} available={available}")
    return {
        **state,
        "sql_error": "", "df_rows": _safe_rows(df.head(5)), "df_columns": cols,
        "chart_json": chart_json, "chart_type": chart_type, "available_charts": available,
    }

# ── Node 5: narrator_node ──────────────────────────────────────────────────────
async def narrator_node(state: AgentState) -> AgentState:
    system = f"""You are ATLAS, a fleet maintenance analyst. Answer the user's question using the board data below.
Be specific — use actual numbers from data_preview. 2-3 sentences max.
Do NOT suggest adding a chart. Do NOT print a markdown table.
IMPORTANT: If a card has ACTIVE_FILTERS, your answer must reflect those filters.
Always ground your answer in what the user is currently viewing — the filtered data, not all data.
{state.get("board_context", "")}"""
    messages = [
        {"role": "system", "content": system},
        *[{"role": m["role"], "content": m["content"]} for m in state.get("history", [])[-6:]],
        {"role": "user", "content": state["user_message"]},
    ]
    try:
        narrative = await _groq(messages, max_tokens=300)
        if "{" in narrative:
            narrative = narrative.split("{")[0].strip()
    except Exception as e:
        narrative = f"I couldn't retrieve that data right now: {e}"
    return {**state, "narrative": narrative, "ui_actions": []}

# ── Node 5b: meta_node ─────────────────────────────────────────────────────────
async def meta_node(state: AgentState) -> AgentState:
    """Answer identity and capability questions about ATLAS."""
    msg = state["user_message"].lower()

    system = f"""You are ATLAS — an AI-powered Business Intelligence assistant for fleet maintenance analytics.
Here is your full self-description:

{ATLAS_IDENTITY}

Answer the user's question about what you are and what you can do.
Be friendly, concise, and specific. 2-4 sentences.
Do NOT generate SQL. Do NOT add JSON. Plain conversational English only."""

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": state["user_message"]},
    ]
    try:
        narrative = await _groq(messages, max_tokens=300)
        if "{" in narrative:
            narrative = narrative.split("{")[0].strip()
    except Exception as e:
        narrative = f"I'm ATLAS — an AI-powered BI assistant for fleet maintenance analytics. Ask me to visualise costs, downtime, reliability metrics, or anything about your fleet data."

    print(f"[meta_node] answered identity question")
    return {**state, "narrative": narrative, "ui_actions": []}

# ── Node 6: board_node ─────────────────────────────────────────────────────────
async def board_node(state: AgentState) -> AgentState:
    board  = state.get("board_context", "")
    intent = state.get("intent", "board")

    if intent == "modify":
        card_id = state.get("selected_card_id")
        if not card_id:
            return {**state, "narrative": "Please select a card first by clicking on it, then ask me to change it.", "ui_actions": []}

        card_chart_type = "table"; card_title = "Query Result"; card_active_filters = ""
        for line in board.split("\n"):
            if f"id={card_id}" in line:
                ct_m = re.search(r"chart_type=(\w+)", line)
                if ct_m: card_chart_type = ct_m.group(1)
                t_m = re.search(r"title='([^']+)'", line)
                if t_m: card_title = t_m.group(1)
                f_m = re.search(r"ACTIVE_FILTERS: ([^|\n]+)", line)
                if f_m: card_active_filters = f_m.group(1).strip()

        filter_notice = f"\nACTIVE FILTERS ON THIS CARD: {card_active_filters}" if card_active_filters else "\nACTIVE FILTERS ON THIS CARD: none"

        system = f"""You are ATLAS. A card is selected on the BI board.
SELECTED CARD: id={card_id}, title='{card_title}', chart_type={card_chart_type}{filter_notice}
{board}
{_get_schema_guide()}

The user wants to modify this card. Classify their request into exactly one of these:
1. CHART TYPE CHANGE - user wants a different visualisation
   -> return {{"action": "chart_type", "chart_type": "..."}}
2. APPLY FILTER WITH VALUE - user wants to filter by a specific value
   -> return {{"action": "apply_filter", "dim": "year", "values": ["2023"]}}
   Empty list to clear: {{"action": "apply_filter", "dim": "year", "values": []}}
3. FILTER DIMENSION ONLY - user asks to add a filter but no value specified
   -> return {{"action": "filter_ui", "dim": "brand"}}
4. SQL DATA CHANGE - user wants different data (columns, grouping, pivot)
   -> return {{"action": "sql", "sql": "SELECT ...", "chart_type": "table", "title": "..."}}
5. UNCLEAR -> return {{"action": "clarify", "question": "..."}}

Return ONLY valid JSON, no markdown."""

        messages = [{"role": "system", "content": system}, {"role": "user", "content": state["user_message"]}]
        try:
            raw    = await _groq(messages, max_tokens=600)
            parsed = _parse_json(raw)

            if parsed.get("action") == "chart_type":
                return {**state, "narrative": f"Done — switched to {parsed['chart_type']} chart.",
                        "ui_actions": [{"action": "modify_chart", "card_id": card_id, "chart_type": parsed["chart_type"]}]}

            elif parsed.get("action") == "apply_filter":
                dim = parsed.get("dim", ""); values = parsed.get("values", [])
                narrative = f"Done — filtered {dim.replace('_',' ')} to {', '.join(str(v) for v in values)}." if values else f"Done — cleared the {dim.replace('_',' ')} filter."
                return {**state, "narrative": narrative,
                        "ui_actions": [{"action": "apply_filter", "card_id": card_id, "dim": dim, "values": values}]}

            elif parsed.get("action") == "filter_ui":
                dim = parsed.get("dim", "")
                return {**state, "narrative": f"Use the {dim.replace('_',' ')} filter on the card to select values.", "ui_actions": []}

            elif parsed.get("action") == "clarify":
                return {**state, "narrative": parsed.get("question", "Could you clarify what you'd like to change?"), "ui_actions": []}

            elif parsed.get("action") == "sql":
                new_sql = parsed.get("sql", ""); new_type = parsed.get("chart_type", "table")
                new_title = parsed.get("title", f"{card_title} (modified)")
                print(f"[board_node] SQL modification -> new card: {new_title!r}")
                return {**state,
                    "sql": new_sql, "chart_type": new_type, "chart_title": new_title,
                    "chart_category": "Cost", "sql_error": "", "sql_retries": 0,
                    "replace_card_id": None,
                    "narrative": f"I've added '{new_title}' alongside. Remove the old one if you prefer this version.",
                    "intent": "visualise"}
        except Exception as e:
            print(f"[board_node] modify error: {e}")
        return {**state, "narrative": "I couldn't apply that change. Try rephrasing.", "ui_actions": []}

    system = f"""You are ATLAS. Answer questions about the current BI board state.
{board}
Be concise. 1-2 sentences."""
    messages = [{"role": "system", "content": system}, {"role": "user", "content": state["user_message"]}]
    try:
        narrative = await _groq(messages, max_tokens=200)
    except Exception as e:
        narrative = f"Error reading board: {e}"
    return {**state, "narrative": narrative, "ui_actions": []}

# ── Node 7: respond_node ───────────────────────────────────────────────────────
async def respond_node(state: AgentState) -> AgentState:
    if state.get("sql_error") and not state.get("chart_json"):
        error = state.get("sql_error", ""); msg = state.get("user_message", "")
        try:
            system = f"""You are ATLAS. A SQL query failed. Give the user a helpful response.
USER ASKED: {msg}
ERROR: {error[:300]}
1. Explain in plain English what went wrong (1 sentence)
2. Suggest 3 rephrased questions to try
Keep it concise and friendly. Plain text only."""
            friendly = await _groq([{"role": "system", "content": system},
                                     {"role": "user", "content": "Explain the error."}], max_tokens=300)
        except Exception:
            friendly = "I had trouble building that query. Try rephrasing — be specific about which metric (cost, count, downtime) and which dimension (brand, month, component) you want."
        return {**state, "narrative": friendly, "ui_actions": []}

    chart_json = state.get("chart_json")
    if not chart_json:
        return {**state, "ui_actions": state.get("ui_actions", [])}

    narrative = state.get("narrative", "")
    if not narrative or len(narrative) < 10:
        preview = state.get("df_rows", [])[:3]
        try:
            system = f"""You are ATLAS. Write a 1-2 sentence insight for a fleet manager.
Use specific numbers from the preview. Plain English only.
Data preview: {json.dumps(preview)}
Chart: {state.get("chart_title","")} ({state.get("chart_type","")})"""
            narrative = await _groq([{"role": "system", "content": system},
                                      {"role": "user", "content": state["user_message"]}], max_tokens=200)
            if "{" in narrative:
                narrative = narrative.split("{")[0].strip()
        except Exception:
            narrative = f"Here is the {state.get('chart_title','data')} chart."

    filter_suggestions = []
    try:
        preview = state.get("df_rows", [])[:5]; cols = state.get("df_columns", [])
        if preview and cols:
            sugg_system = f"""You are ATLAS. Suggest 2-3 useful filter chips for this data.
Data columns: {cols}
Data preview: {json.dumps(preview[:3])}
Return ONLY a JSON array: [{{"dim": "brand", "value": "Scania", "label": "Scania only"}}]
- dim must be an actual column name from the data
- value must be visible in the preview
- Max 3 suggestions, return [] if none useful"""
            sugg_raw = await _groq([{"role": "system", "content": sugg_system},
                                     {"role": "user", "content": "Suggest filters"}], max_tokens=200)
            filter_suggestions = _parse_json(sugg_raw) if sugg_raw.strip().startswith("[") else []
            if not isinstance(filter_suggestions, list):
                filter_suggestions = []
    except Exception:
        filter_suggestions = []

    replace_id  = state.get("replace_card_id")
    action_type = "replace_chart" if replace_id else "add_chart"
    long_sql    = state.get("long_sql", "") or state.get("sql", "")
    wide_sql    = state.get("wide_sql", "")
    is_wide     = state.get("is_wide", False)
    pivot_col   = state.get("pivot_col", "")

    ui_actions = [{
        "action": action_type, "card_id": replace_id,
        "metric_id": state.get("chart_title","").lower().replace(" ","_"),
        "title": state.get("chart_title","Query Result"),
        "chart_type": state.get("chart_type","bar"),
        "category": state.get("chart_category","General"),
        "filter_suggestions": filter_suggestions,
        "long_sql": long_sql, "wide_sql": wide_sql, "is_wide": is_wide, "pivot_col": pivot_col,
        "chart_data": {
            "chart": state.get("chart_json"),
            "chart_type": state.get("chart_type","bar"),
            "title": state.get("chart_title",""),
            "category": state.get("chart_category","General"),
            "sql": state.get("sql",""),
            "row_count": len(state.get("df_rows",[])),
            "summary": state.get("df_rows",[])[:5],
            "available_charts": state.get("available_charts",["bar","table"]),
        },
        "filters": {}, "sql": state.get("sql",""),
    }]
    return {**state, "narrative": narrative, "ui_actions": ui_actions}

# ── Node 8: memory_node ────────────────────────────────────────────────────────
async def memory_node(state: AgentState) -> AgentState:
    from db.supabase import load_memory, save_memory
    try:
        user_id = "default"; existing = load_memory(user_id)
        msg = state.get("user_message",""); sql = state.get("sql","")
        chart = state.get("chart_type",""); df_rows = state.get("df_rows",[])
        system = f"""You are ATLAS memory extractor. Extract learnable preferences from this interaction.
EXISTING MEMORY: {json.dumps(existing)}
THIS TURN: User asked: {msg} | SQL: {sql[:200] if sql else 'none'} | Chart: {chart}
Return ONLY a JSON object:
{{
  "preferred_chart": "{existing.get('preferred_chart','')}",
  "focus_brands": {json.dumps(existing.get('focus_brands',[]))},
  "focus_years": [],
  "focus_metrics": {json.dumps(existing.get('focus_metrics',[]))},
  "preferred_filters": {json.dumps(existing.get('preferred_filters',{}))},
  "expertise_level": "{existing.get('expertise_level','intermediate')}",
  "last_topics": {json.dumps((existing.get('last_topics') or [])[-4:] + ([msg[:60]] if msg else []))}
}}"""
        raw = await _groq([{"role": "system", "content": system},
                            {"role": "user", "content": "Extract preferences."}], max_tokens=300)
        new_memory = _parse_json(raw)
        if isinstance(new_memory, dict) and new_memory:
            save_memory(user_id, new_memory)
            print(f"[memory_node] saved: focus_brands={new_memory.get('focus_brands')} preferred_chart={new_memory.get('preferred_chart')}")
    except Exception as e:
        print(f"[memory_node] failed (non-fatal): {e}")
    return state

# ── Routing ────────────────────────────────────────────────────────────────────
def route_intent(state: AgentState) -> str:
    intent = state.get("intent", "visualise")
    if intent == "meta":     return "meta_node"
    if intent == "explain":  return "narrator_node"
    if intent in {"board","modify"}: return "board_node"
    return "sql_node"

def route_after_board(state: AgentState) -> str:
    if state.get("intent") == "visualise" and state.get("sql"):
        return "chart_node"
    return "memory_node"

def route_after_sql(state: AgentState) -> str:
    if state.get("sql_error") and state.get("sql_retries", 0) < 3:
        return "correction_node"
    elif state.get("sql_error"):
        return "respond_node"
    return "chart_node"

def route_after_chart(state: AgentState) -> str:
    if state.get("sql_error") and state.get("sql_retries", 0) < 3:
        return "correction_node"
    return "respond_node"

# ── Build graph ────────────────────────────────────────────────────────────────
def build_graph():
    g = StateGraph(AgentState)
    g.add_node("intent_node",     intent_node)
    g.add_node("sql_node",        sql_node)
    g.add_node("correction_node", correction_node)
    g.add_node("chart_node",      chart_node)
    g.add_node("narrator_node",   narrator_node)
    g.add_node("meta_node",       meta_node)
    g.add_node("board_node",      board_node)
    g.add_node("respond_node",    respond_node)
    g.add_node("memory_node",     memory_node)
    g.set_entry_point("intent_node")
    g.add_conditional_edges("intent_node", route_intent, {
        "sql_node":      "sql_node",
        "narrator_node": "narrator_node",
        "meta_node":     "meta_node",
        "board_node":    "board_node",
    })
    g.add_conditional_edges("sql_node", route_after_sql, {
        "chart_node":      "chart_node",
        "correction_node": "correction_node",
        "respond_node":    "respond_node",
    })
    g.add_edge("correction_node", "sql_node")
    g.add_conditional_edges("chart_node", route_after_chart, {
        "correction_node": "correction_node",
        "respond_node":    "respond_node",
    })
    g.add_edge("narrator_node",  "memory_node")
    g.add_edge("meta_node",      "memory_node")
    g.add_conditional_edges("board_node", route_after_board, {
        "chart_node":  "chart_node",
        "memory_node": "memory_node",
    })
    g.add_edge("respond_node", "memory_node")
    g.add_edge("memory_node",  END)
    return g.compile()

_graph = None
def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph
