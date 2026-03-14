"""
ATLAS BI — LangGraph Agent Nodes
Graph: intent_node → sql_node → [correction_node] → chart_node → respond_node → memory_node
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

SCHEMA_GUIDE = """
DATABASE VIEW: v_maintenance_full
All queries run against this single view. Use DuckDB SQL syntax.

COLUMNS (exact names — do NOT invent column names):
  plate_number       TEXT    — unique vehicle identifier
  brand              TEXT    — Scania, Volvo, Mercedes-Benz, MAN, Hino
  fleet_segment      TEXT    — Heavy, Medium, Light
  year_manufactured  INTEGER
  truck_age_years    INTEGER
  service_date       DATE
  year               INTEGER — extracted from service_date
  month              INTEGER — 1-12
  year_month         TEXT    — e.g. "2024-03"
  month_name         TEXT    — e.g. "March"
  year_quarter       TEXT    — e.g. "2024-Q1"
  maintenance_type   TEXT    — Scheduled, Unscheduled
  component_category TEXT    — Engine, Transmission, Brakes, Electrical, Tyres, Body, Suspension, Cooling
  component_name     TEXT
  failure_type       TEXT
  criticality_level  TEXT    — Critical, High, Medium, Low
  workshop_name      TEXT
  workshop_type      TEXT    — Authorised, Independent
  region             TEXT
  total_cost_myr     FLOAT
  parts_cost_myr     FLOAT
  labour_cost_myr    FLOAT
  downtime_days      FLOAT
  is_repeat_failure  BOOLEAN

DATE RANGE: 2020-2024. "This month"=Dec 2024. "This year"=2024. "Last year"=2023.

SQL RULES:
- Use strftime(service_date,'%Y') for year, strftime(service_date,'%m') for month
- Use ROUND(value,2) for monetary values
- Use NULLIF(denominator,0) to avoid division by zero
- GROUP BY all non-aggregated SELECT columns
- Time series: ORDER BY time column ASC
- Category: ORDER BY measure DESC
- No LIMIT unless user asks for top-N
"""

TIME_COLS   = {"year_month","month_name","year_quarter","service_date","year","month"}
CAT_COLS    = {"brand","plate_number","vehicle_id","workshop_name","component_name",
               "component_category","fleet_segment","maintenance_type","workshop_type",
               "region","failure_type","criticality_level"}
NUMERIC_TYPES = {"int64","float64","int32","float32"}


# ── State definition ───────────────────────────────────────────────────────────

class AgentState(TypedDict):
    # Inputs
    user_message:    str
    history:         list[dict]
    board_context:   str
    user_memory:     str
    # Routing
    intent:          str        # "visualise" | "explain" | "board" | "modify"
    selected_card_id: Optional[str]
    # SQL
    sql:             str
    sql_error:       str
    sql_retries:     int
    # Data
    df_rows:         list[dict]
    df_columns:      list[str]
    # Chart
    chart_json:      str
    chart_type:      str
    available_charts: list[str]
    chart_title:     str
    chart_category:  str
    # Output
    narrative:       str
    ui_actions:      list[dict]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _infer_meta(columns: list[str]) -> dict:
    """Infer x_col, y_col, group_col from column names."""
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
    # numeric = anything not already categorised and not year/month integers
    for c in columns:
        cl = c.lower()
        if c not in time_found + cat_found + group_found and cl not in {"year","month","id"}:
            num_found.append(c)

    x_col   = time_found[0] if time_found else (cat_found[0] if cat_found else columns[0])
    y_col   = num_found[0]  if num_found  else (columns[-1] if len(columns) > 1 else columns[0])
    group_col = group_found[0] if group_found and len(cat_found) > 0 else None

    return {"x_col": x_col, "y_col": y_col, "group_col": group_col}


def _smart_available_charts(columns: list[str], df: pd.DataFrame, chart_type: str) -> list[str]:
    """Return semantically appropriate chart types based on data shape."""
    cols_lower = [c.lower() for c in columns]
    has_time   = any(c in TIME_COLS for c in cols_lower)
    has_group  = any(c in {"component_category","fleet_segment","maintenance_type",
                           "workshop_type","failure_type"} for c in cols_lower)
    has_stats  = any(c in {"q1","q3","median","mean","std"} for c in cols_lower)
    n_numeric  = sum(1 for c in columns if str(df[c].dtype) in NUMERIC_TYPES)
    n_rows     = len(df)
    n_cats     = sum(1 for c in cols_lower if c in CAT_COLS)

    available = ["table"]  # always

    if has_time:
        available += ["line", "bar"]
        if has_group:
            available += ["stacked_bar", "heatmap"]
        # No pie, scatter, treemap for time series

    elif has_stats:
        available += ["boxplot", "histogram"]
        # No pie, stacked_bar, line

    elif has_group and n_cats >= 1:
        available += ["stacked_bar", "bar", "heatmap", "table"]
        # No pie, scatter, line

    elif n_numeric >= 2 and n_cats >= 1 and not has_time:
        available += ["scatter", "bar", "table"]
        # No pie, stacked_bar, line

    elif n_cats >= 1 and n_numeric >= 1:
        available += ["bar", "pareto", "waterfall"]
        if n_rows <= 8:
            available += ["pie", "treemap"]
        if n_rows > 8:
            available += ["treemap"]

    # Always keep current chart_type first, deduplicate
    if chart_type not in available:
        available.append(chart_type)
    return [chart_type] + [a for a in list(dict.fromkeys(available)) if a != chart_type]


def _auto_chart_type(columns: list[str], hint: str = None) -> str:
    """Pick best chart type from column names and optional hint."""
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


# ── Groq call helper (used by nodes) ──────────────────────────────────────────

import os, httpx, asyncio

GROQ_API_KEY       = os.getenv("GROQ_API_KEY","")
GROQ_MODEL         = "llama-3.3-70b-versatile"
GROQ_MODEL_FALLBACK = "llama-3.1-8b-instant"
GROQ_URL           = "https://api.groq.com/openai/v1/chat/completions"

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
    """Classify user intent and route accordingly."""
    msg = state["user_message"].lower()
    board = state.get("board_context", "")

    # Fast rule-based classification (no LLM needed)
    explain_words  = {"why","explain","reason","cause","because","how come","what makes"}
    board_words    = {"how many charts","what charts","list my charts","what's on","what is on"}
    modify_words   = {"change","switch","modify","convert","make it","turn into"}

    intent = "visualise"  # default

    if any(w in msg for w in explain_words):
        intent = "explain"
    elif any(w in msg for w in board_words):
        intent = "board"
    elif any(w in msg for w in modify_words) and "SELECTED CARD:" in board and "none" not in board.split("SELECTED CARD:")[-1][:20]:
        intent = "modify"

    # Extract selected card id if modifying
    selected_card_id = None
    if intent == "modify" and "SELECTED CARD:" in board:
        m = re.search(r"id=([a-f0-9\-]+)", board.split("SELECTED CARD:")[-1])
        if m:
            selected_card_id = m.group(1)

    print(f"[intent_node] intent={intent!r} selected_card={selected_card_id!r}")
    return {**state, "intent": intent, "selected_card_id": selected_card_id}


# ── Node 2: sql_node ───────────────────────────────────────────────────────────

async def sql_node(state: AgentState) -> AgentState:
    """LLM writes SQL + picks chart type + title."""
    system = f"""You are a DuckDB SQL expert for a fleet maintenance BI platform.

{SCHEMA_GUIDE}

{state.get("board_context", "")}

Return ONLY valid JSON — no markdown, no code blocks:
{{
  "sql": "SELECT ...",
  "chart_type": "bar",
  "title": "Short descriptive title",
  "category": "Cost",
  "narrative_hint": "One sentence about what this shows"
}}

CHART TYPE GUIDE:
- line: when query groups by year_month, year_quarter, month_name (time series)
- bar: category comparisons (brand, component, workshop)
- stacked_bar: when query has 2 categorical dims (brand + component_category)
- table: when user asks for a table, or query has many columns (>4)
- scatter: when query returns 2 numeric columns for correlation
- pie: proportions, max 8 categories
- heatmap: when query has brand × month
- pareto: 80/20 failure/cost analysis
- waterfall: cumulative cost buildup
- boxplot: distribution with quartiles (needs q1, median, q3 columns)
- histogram: frequency distribution
- treemap: hierarchical proportions"""

    # Include error context if retrying
    retry_context = ""
    if state.get("sql_error"):
        retry_context = f"\n\nPREVIOUS SQL FAILED with error: {state['sql_error']}\nPrevious SQL was: {state.get('sql','')}\nPlease fix the SQL — only use column names from the SCHEMA GUIDE above."

    messages = [
        {"role": "system", "content": system + retry_context},
        *[{"role": m["role"], "content": m["content"]} for m in state.get("history", [])[-6:]],
        {"role": "user", "content": state["user_message"]},
    ]

    try:
        raw  = await _groq(messages, max_tokens=800)
        parsed = _parse_json(raw)
        sql  = parsed.get("sql", "").strip()
        if not sql.upper().startswith("SELECT"):
            raise ValueError(f"LLM returned non-SELECT SQL: {sql[:80]}")
        print(f"[sql_node] SQL={sql[:80]}...")
        return {
            **state,
            "sql":           sql,
            "sql_error":     "",
            "chart_type":    parsed.get("chart_type", "bar"),
            "chart_title":   parsed.get("title", "Query Result"),
            "chart_category":parsed.get("category", "Cost"),
            "narrative":     parsed.get("narrative_hint", ""),
        }
    except Exception as e:
        print(f"[sql_node] ERROR: {e}")
        return {**state, "sql_error": str(e), "sql": ""}


# ── Node 3: correction_node ────────────────────────────────────────────────────

async def correction_node(state: AgentState) -> AgentState:
    """Feed SQL error back to LLM for self-correction."""
    retries = state.get("sql_retries", 0) + 1
    print(f"[correction_node] retry {retries}/3 — error: {state['sql_error']}")
    # Just increment retry count and loop back to sql_node with error context
    return {**state, "sql_retries": retries}


# ── Node 4: chart_node ─────────────────────────────────────────────────────────

async def chart_node(state: AgentState) -> AgentState:
    """Execute SQL, build Plotly chart, determine available chart types."""
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
    # Final validation: override if chart_type is semantically wrong for data
    cols_lower = [c.lower() for c in cols]
    if chart_type in {"scatter"} and not (sum(1 for c in cols if str(df[c].dtype) in NUMERIC_TYPES) >= 2):
        chart_type = "bar"
    if chart_type in {"line"} and not any(c in TIME_COLS for c in cols_lower):
        chart_type = "bar"
    if chart_type in {"stacked_bar"} and not meta.get("group_col"):
        chart_type = "bar"

    try:
        chart_json = _build_chart(df, meta, chart_type)
    except Exception as e:
        print(f"[chart_node] chart build failed ({chart_type}): {e}, falling back to table")
        try:
            chart_json = _build_chart(df, meta, "table")
            chart_type = "table"
        except Exception as e2:
            return {**state, "sql_error": f"Chart build failed: {e2}", "df_rows": [], "df_columns": []}

    available = _smart_available_charts(cols, df, chart_type)
    print(f"[chart_node] chart_type={chart_type} rows={len(df)} available={available}")

    return {
        **state,
        "sql_error":       "",
        "df_rows":         df.head(5).to_dict(orient="records"),
        "df_columns":      cols,
        "chart_json":      chart_json,
        "chart_type":      chart_type,
        "available_charts": available,
    }


# ── Node 5: narrator_node ──────────────────────────────────────────────────────

async def narrator_node(state: AgentState) -> AgentState:
    """Answer explain/why questions using board data — no chart added."""
    system = f"""You are ATLAS, a fleet maintenance analyst. Answer the user's question using the board data below.
Be specific — use actual numbers from data_preview. 2-3 sentences max.
Do NOT suggest adding a chart. Do NOT print a markdown table.

{state.get("board_context", "")}
"""
    messages = [
        {"role": "system", "content": system},
        *[{"role": m["role"], "content": m["content"]} for m in state.get("history", [])[-6:]],
        {"role": "user",   "content": state["user_message"]},
    ]
    try:
        narrative = await _groq(messages, max_tokens=300)
        # Strip any JSON bleed
        if "{" in narrative:
            narrative = narrative.split("{")[0].strip()
    except Exception as e:
        narrative = f"I couldn't retrieve that data right now: {e}"

    return {**state, "narrative": narrative, "ui_actions": []}


# ── Node 6: board_node ─────────────────────────────────────────────────────────

async def board_node(state: AgentState) -> AgentState:
    """Answer board-level questions or emit modify_chart actions."""
    msg   = state["user_message"].lower()
    board = state.get("board_context", "")
    intent = state.get("intent", "board")

    if intent == "modify":
        card_id = state.get("selected_card_id")
        if not card_id:
            return {**state, "narrative": "Please select a card first by clicking on it, then ask me to change it.", "ui_actions": []}

        # Ask LLM what to change
        system = f"""Given this selected card on the ATLAS BI board, determine what modification the user wants.
{board}
Return ONLY JSON: {{"chart_type": "bar", "filters": {{}}}}
Only include keys the user actually wants to change."""
        messages = [{"role": "system", "content": system}, {"role": "user", "content": state["user_message"]}]
        try:
            raw    = await _groq(messages, max_tokens=200)
            parsed = _parse_json(raw)
            action = {"action": "modify_chart", "card_id": card_id, **parsed}
            narrative = f"Done — I've updated the selected chart."
        except Exception as e:
            action    = {"action": "modify_chart", "card_id": card_id}
            narrative = "I've applied the change to the selected card."

        return {**state, "narrative": narrative, "ui_actions": [action]}

    # Board query — answer from context
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
    """Assemble final narrative + ui_actions for the frontend."""
    # If sql_error still set after retries, explain to user
    if state.get("sql_error") and not state.get("chart_json"):
        return {
            **state,
            "narrative":  f"I couldn't build that query — {state['sql_error']}. Could you rephrase what you're looking for?",
            "ui_actions": [],
        }

    chart_json = state.get("chart_json")
    if not chart_json:
        return {**state, "ui_actions": state.get("ui_actions", [])}

    # Generate narrative if not already set
    narrative = state.get("narrative", "")
    if not narrative or len(narrative) < 10:
        preview = state.get("df_rows", [])[:3]
        system  = f"""You are ATLAS. Write a 1-2 sentence insight about this data for a fleet manager.
Use specific numbers from the preview. Plain English only — no JSON, no markdown.
Data preview: {json.dumps(preview)}
Chart: {state.get("chart_title","")} ({state.get("chart_type","")})"""
        messages = [{"role": "system", "content": system}, {"role": "user", "content": state["user_message"]}]
        try:
            narrative = await _groq(messages, max_tokens=200)
            if "{" in narrative:
                narrative = narrative.split("{")[0].strip()
        except Exception:
            narrative = f"Here is the {state.get('chart_title','data')} chart."

    ui_actions = [{
        "action":           "add_chart",
        "metric_id":        state.get("chart_title","").lower().replace(" ","_"),
        "title":            state.get("chart_title","Query Result"),
        "chart_type":       state.get("chart_type","bar"),
        "category":         state.get("chart_category","General"),
        "chart_data": {
            "chart":            state.get("chart_json"),
            "chart_type":       state.get("chart_type","bar"),
            "title":            state.get("chart_title",""),
            "category":         state.get("chart_category","General"),
            "sql":              state.get("sql",""),
            "row_count":        len(state.get("df_rows",[])),
            "summary":          state.get("df_rows",[])[:5],
            "available_charts": state.get("available_charts",["bar","table"]),
        },
        "filters": {},
        "sql":    state.get("sql",""),
    }]

    return {**state, "narrative": narrative, "ui_actions": ui_actions}


# ── Node 8: memory_node ────────────────────────────────────────────────────────

async def memory_node(state: AgentState) -> AgentState:
    """Extract learnable preferences (stub — will write to Supabase in Step 4)."""
    # Placeholder for Step 4 — memory extraction
    return state


# ── Routing functions ──────────────────────────────────────────────────────────

def route_intent(state: AgentState) -> str:
    intent = state.get("intent", "visualise")
    if intent == "explain":
        return "narrator_node"
    elif intent in {"board", "modify"}:
        return "board_node"
    else:
        return "sql_node"


def route_after_sql(state: AgentState) -> str:
    """After sql_node: go to chart if SQL ok, correction if error, respond if too many retries."""
    if state.get("sql_error") and state.get("sql_retries", 0) < 3:
        return "correction_node"
    elif state.get("sql_error"):
        return "respond_node"   # give up after 3 retries
    else:
        return "chart_node"


def route_after_chart(state: AgentState) -> str:
    """After chart_node: retry sql if chart failed, else respond."""
    if state.get("sql_error") and state.get("sql_retries", 0) < 3:
        return "correction_node"
    return "respond_node"


# ── Build the graph ────────────────────────────────────────────────────────────

def build_graph():
    g = StateGraph(AgentState)

    g.add_node("intent_node",     intent_node)
    g.add_node("sql_node",        sql_node)
    g.add_node("correction_node", correction_node)
    g.add_node("chart_node",      chart_node)
    g.add_node("narrator_node",   narrator_node)
    g.add_node("board_node",      board_node)
    g.add_node("respond_node",    respond_node)
    g.add_node("memory_node",     memory_node)

    g.set_entry_point("intent_node")

    g.add_conditional_edges("intent_node", route_intent, {
        "sql_node":      "sql_node",
        "narrator_node": "narrator_node",
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

    g.add_edge("narrator_node", "memory_node")
    g.add_edge("board_node",    "memory_node")
    g.add_edge("respond_node",  "memory_node")
    g.add_edge("memory_node",   END)

    return g.compile()


# Singleton graph instance
_graph = None

def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph
