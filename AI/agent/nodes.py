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


# ── Schema guide — dynamic, generated from actual data ────────────────────────

def _get_schema_guide() -> str:
    """Get the current schema guide, generated dynamically from loaded data."""
    try:
        from db.duckdb_session import get_schema_guide
        return get_schema_guide()
    except Exception as e:
        print(f"[nodes] schema guide fallback: {e}")
        return "SCHEMA: Data loaded in DuckDB. Query tables using SQL."


# Alias for backward compat
SCHEMA_GUIDE = property(_get_schema_guide)  # not used directly — always call _get_schema_guide()

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
    replace_card_id: Optional[str]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_rows(df) -> list[dict]:
    """Convert DataFrame rows to JSON-safe dicts — handles NaN, Inf, None."""
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

    # Prefer year_month for time series (shows both year and month)
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
    y_col   = num_found[0]  if num_found  else (columns[-1] if len(columns) > 1 else columns[0])
    group_col = group_found[0] if group_found and len(cat_found) > 0 else None

    return {"x_col": x_col, "y_col": y_col, "group_col": group_col}


def _smart_available_charts(columns: list[str], df: pd.DataFrame, chart_type: str) -> list[str]:
    """Return valid chart types based on actual data shape.
    Uses dtype analysis — not hardcoded column names — so it works on any dataset.
    Table is always available. Current chart_type is always preserved."""

    # Analyse actual dtypes
    n_rows    = len(df)
    num_cols  = [c for c in columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols  = [c for c in columns if not pd.api.types.is_numeric_dtype(df[c])
                 and not pd.api.types.is_datetime64_any_dtype(df[c])]
    time_cols = [c for c in columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    # Also treat string columns that look like time (year_month, year_quarter etc)
    for c in columns:
        cl = c.lower()
        if cl in TIME_COLS and c not in time_cols:
            time_cols.append(c)

    n_num  = len(num_cols)
    n_cat  = len(cat_cols)
    n_time = len(time_cols)

    # Check for stat columns (boxplot indicator)
    stat_names = {"q1","q3","median","mean","std","min","max",
                  "cost_q1","cost_median","cost_q3","cost_min","cost_max","cost_mean"}
    has_stats = any(c.lower() in stat_names for c in columns)

    # Check cardinality of categorical cols for pie eligibility
    max_cat_unique = max((df[c].nunique() for c in cat_cols), default=0)

    available = ["table"]  # always

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

    # Always keep current chart type visible
    if chart_type and chart_type not in available:
        available.append(chart_type)

    return list(dict.fromkeys(available))


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
    msg   = state["user_message"].lower()
    board = state.get("board_context", "")

    # Check if a card is selected
    has_selected = "SELECTED CARD:" in board and "none" not in board.split("SELECTED CARD:")[-1][:20]
    selected_card_id = None
    if has_selected:
        m = re.search(r"id=([a-f0-9\-]+)", board.split("SELECTED CARD:")[-1])
        if m:
            selected_card_id = m.group(1)

    explain_words = {"why","explain","reason","cause","because","how come","what makes"}
    board_words   = {"how many charts","what charts","list my charts","what's on","what is on"}
    modify_words  = {"change","switch","modify","convert","make it","turn into","update","add column",
                     "add columns","another column","more columns","include","can we have","can you add",
                     "pivot","breakdown","split by","group by year","per year","by year"}

    intent = "visualise"  # default

    if any(w in msg for w in explain_words):
        intent = "explain"
    elif any(w in msg for w in board_words):
        intent = "board"
    elif has_selected and any(w in msg for w in modify_words):
        # Card is selected + user wants to change/extend it → modify
        intent = "modify"
    elif has_selected and intent == "visualise":
        # Card selected + data question → extend the selected card's query
        intent = "modify"

    print(f"[intent_node] intent={intent!r} selected_card={selected_card_id!r} has_selected={has_selected}")
    return {**state, "intent": intent, "selected_card_id": selected_card_id}


# ── Node 2: sql_node ───────────────────────────────────────────────────────────

async def sql_node(state: AgentState) -> AgentState:
    """LLM writes SQL + picks chart type + title."""
    # Build personalised memory context
    memory_hint = ""
    raw_memory = state.get("user_memory", "")
    if raw_memory:
        try:
            m = json.loads(raw_memory) if isinstance(raw_memory, str) else raw_memory
            hints = []
            if m.get("preferred_chart"):
                hints.append(f"User prefers {m['preferred_chart']} charts — use unless data clearly needs another")
            if m.get("focus_brands"):
                hints.append(f"User focuses on: {', '.join(m['focus_brands'])}")
            if m.get("focus_years"):
                hints.append(f"User usually looks at years: {', '.join(str(y) for y in m['focus_years'])}")
            if hints:
                memory_hint = "\n\nUSER PREFERENCES (apply subtly):\n" + "\n".join(f"- {h}" for h in hints)
        except Exception:
            pass

    system = f"""You are a DuckDB SQL expert for a fleet maintenance BI platform.

{_get_schema_guide()}

{state.get("board_context", "")}{memory_hint}

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
    """Feed SQL error back to LLM for self-correction with rich context."""
    retries = state.get("sql_retries", 0) + 1
    error   = state.get("sql_error", "")
    bad_sql = state.get("sql", "")
    print(f"[correction_node] retry {retries}/3 — error: {error}")

    # Annotate error with plain English explanation for the LLM
    error_hint = error
    if "not found in FROM clause" in error or "Referenced column" in error:
        error_hint = f"{error}\n→ You used a column that does not exist in v_maintenance_full. Use ONLY columns listed in the SCHEMA GUIDE."
    elif "strftime" in error.lower() or "service_date" in error.lower():
        error_hint = f"{error}\n→ Do NOT use strftime() or service_date. Use the pre-extracted columns: year (INTEGER), month (INTEGER), year_month (TEXT like '2024-01')."
    elif "syntax error" in error.lower():
        error_hint = f"{error}\n→ Fix the SQL syntax. Check for missing commas, unclosed parentheses, or invalid keywords."
    elif "ambiguous" in error.lower():
        error_hint = f"{error}\n→ Column name is ambiguous. Prefix with table name: v_maintenance_full.column_name"

    return {**state, "sql_retries": retries, "sql_error": error_hint}


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
        "df_rows":         _safe_rows(df.head(5)),
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
    """Handle board queries and selected-card modifications."""
    board  = state.get("board_context", "")
    intent = state.get("intent", "board")

    if intent == "modify":
        card_id = state.get("selected_card_id")
        if not card_id:
            return {**state, "narrative": "Please select a card first by clicking on it, then ask me to change it.", "ui_actions": []}

        # Extract selected card's SQL and current chart type from board context
        card_sql = ""
        card_chart_type = "table"
        card_title = "Query Result"
        for line in board.split("\n"):
            if f"id={card_id}" in line:
                ct_match = re.search(r"chart_type=(\w+)", line)
                if ct_match:
                    card_chart_type = ct_match.group(1)
                title_match = re.search(r"title='([^']+)'", line)
                if title_match:
                    card_title = title_match.group(1)
            if "data_preview" in line and card_sql == "":
                pass  # data preview only, not full SQL

        # Ask LLM: what kind of modification does the user want?
        system = f"""You are ATLAS. A card is selected on the BI board.
SELECTED CARD: id={card_id}, title='{card_title}', chart_type={card_chart_type}

{board}

{_get_schema_guide()}

The user wants to modify this card. Classify their request into exactly one of these:

1. CHART TYPE CHANGE — user wants a different visualisation (bar, line, pie, table, heatmap etc)
   → return {{"action": "chart_type", "chart_type": "..."}}

2. FILTER UI — user wants to add/show a filter dropdown on the card (e.g. "add a month filter",
   "add a brand filter", "I want to filter by year"). Do NOT hardcode a value — just expose the filter.
   → return {{"action": "filter_ui", "dim": "month"}}
   Valid dims: brand, year, month, quarter, fleet_segment, maintenance_type, criticality_level, workshop_type, region, component_category

3. SQL DATA CHANGE — user wants different data (more columns, different grouping, pivot, aggregation)
   → Write a NEW SQL query and return:
   {{"action": "sql", "sql": "SELECT ...", "chart_type": "table", "title": "..."}}

4. UNCLEAR — if you are not sure what the user wants, ask for clarification
   → return {{"action": "clarify", "question": "..."}}

Return ONLY valid JSON, no markdown. Think carefully before choosing."""

        messages = [{"role": "system", "content": system}, {"role": "user", "content": state["user_message"]}]
        try:
            raw    = await _groq(messages, max_tokens=600)
            parsed = _parse_json(raw)

            if parsed.get("action") == "chart_type":
                ui_action = {"action": "modify_chart", "card_id": card_id, "chart_type": parsed["chart_type"]}
                narrative = f"Done — switched to {parsed['chart_type']} chart."
                return {**state, "narrative": narrative, "ui_actions": [ui_action]}

            elif parsed.get("action") == "filter_ui":
                # User wants a filter dropdown added — emit show_filter action
                dim = parsed.get("dim", "")
                ui_action = {"action": "show_filter", "card_id": card_id, "dim": dim}
                narrative = f"I've opened the {dim.replace('_', ' ')} filter on this card — select the values you want."
                return {**state, "narrative": narrative, "ui_actions": [ui_action]}

            elif parsed.get("action") == "clarify":
                # LLM isn't sure — ask the user
                question = parsed.get("question", "Could you clarify what you'd like to change?")
                return {**state, "narrative": question, "ui_actions": []}

            elif parsed.get("action") == "sql":
                # Re-run with new SQL → adds NEW card alongside (user can compare and remove old)
                new_sql   = parsed.get("sql", "")
                new_type  = parsed.get("chart_type", "table")
                new_title = parsed.get("title", f"{card_title} (modified)")
                print(f"[board_node] SQL modification → new card: {new_title!r}")
                return {**state,
                    "sql": new_sql, "chart_type": new_type,
                    "chart_title": new_title, "chart_category": "Cost",
                    "sql_error": "", "sql_retries": 0,
                    "replace_card_id": None,
                    "narrative": f"I've added '{new_title}' alongside. Remove the old one if you prefer this version.",
                    "intent": "visualise"
                }
        except Exception as e:
            print(f"[board_node] modify error: {e}")

        return {**state, "narrative": "I couldn't apply that change. Try rephrasing.", "ui_actions": []}

    # Plain board query
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
    # If sql_error still set after retries, give rich explanation + suggestions
    if state.get("sql_error") and not state.get("chart_json"):
        error   = state.get("sql_error", "")
        bad_sql = state.get("sql", "")
        msg     = state.get("user_message", "")

        # Ask LLM to generate friendly explanation + 3 rephrasing suggestions
        try:
            system = f"""You are ATLAS. A SQL query failed. Give the user a helpful response.

USER ASKED: {msg}
SQL ATTEMPTED: {bad_sql[:300] if bad_sql else "none generated"}
ERROR: {error[:300]}

Write a response that:
1. Explains in plain English what went wrong (1 sentence, no jargon)
2. Suggests 3 specific rephrased questions the user could try instead
Keep it concise and friendly. Format as plain text, no JSON."""

            messages = [{"role": "system", "content": system},
                        {"role": "user", "content": "Explain the error and suggest alternatives."}]
            friendly = await _groq(messages, max_tokens=300)
        except Exception:
            friendly = f"I had trouble building that query. Try rephrasing — for example, be specific about which metric (cost, count, downtime) and which dimension (brand, month, component) you want."

        return {
            **state,
            "narrative":  friendly,
            "ui_actions": [],
        }

    chart_json = state.get("chart_json")
    if not chart_json:
        return {**state, "ui_actions": state.get("ui_actions", [])}

    # Generate narrative if not already set
    narrative = state.get("narrative", "")
    if not narrative or len(narrative) < 10:
        preview = state.get("df_rows", [])[:3]
        try:
            preview_str = json.dumps(preview)
        except Exception:
            preview_str = str(preview[:2])
        system  = f"""You are ATLAS. Write a 1-2 sentence insight about this data for a fleet manager.
Use specific numbers from the preview. Plain English only — no JSON, no markdown.
Data preview: {preview_str}
Chart: {state.get("chart_title","")} ({state.get("chart_type","")})"""
        messages = [{"role": "system", "content": system}, {"role": "user", "content": state["user_message"]}]
        try:
            narrative = await _groq(messages, max_tokens=200)
            if "{" in narrative:
                narrative = narrative.split("{")[0].strip()
        except Exception:
            narrative = f"Here is the {state.get('chart_title','data')} chart."

    # Generate AI filter suggestions based on data
    filter_suggestions = []
    try:
        preview = state.get("df_rows", [])[:5]
        cols    = state.get("df_columns", [])
        if preview and cols:
            sugg_system = f"""You are ATLAS. Given this data preview, suggest 2-3 useful filter chips.
Each chip should be a specific filter value the user might want to apply.
Only suggest filters that make sense for the data shown.

Data columns: {cols}
Data preview: {json.dumps(preview[:3])}

Return ONLY a JSON array of objects:
[{{"dim": "brand", "value": "Scania", "label": "Scania only"}}, ...]

Rules:
- dim must be one of: brand, year, quarter, fleet_segment, maintenance_type, criticality_level, workshop_type, region, component_category
- value must be an actual value visible in the data preview
- label is short and human-friendly (max 4 words)
- Return [] if no useful suggestions
- Max 3 suggestions"""
            sugg_messages = [{"role": "system", "content": sugg_system},
                             {"role": "user", "content": "Suggest filters"}]
            sugg_raw = await _groq(sugg_messages, max_tokens=200)
            filter_suggestions = _parse_json(sugg_raw) if sugg_raw.strip().startswith("[") else []
            if not isinstance(filter_suggestions, list):
                filter_suggestions = []
    except Exception as e:
        print(f"[respond_node] filter suggestions failed: {e}")
        filter_suggestions = []

    replace_id = state.get("replace_card_id")
    action_type = "replace_chart" if replace_id else "add_chart"

    ui_actions = [{
        "action":            action_type,
        "card_id":           replace_id,  # only used for replace_chart
        "metric_id":         state.get("chart_title","").lower().replace(" ","_"),
        "title":             state.get("chart_title","Query Result"),
        "chart_type":        state.get("chart_type","bar"),
        "category":          state.get("chart_category","General"),
        "filter_suggestions": filter_suggestions,
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
    """Extract user preferences from this turn and persist to Supabase."""
    from db.supabase import load_memory, save_memory

    try:
        user_id = "default"
        existing = load_memory(user_id)

        # Build context for extraction
        msg       = state.get("user_message", "")
        sql       = state.get("sql", "")
        chart     = state.get("chart_type", "")
        narrative = state.get("narrative", "")
        df_rows   = state.get("df_rows", [])

        system = f"""You are ATLAS memory extractor. Given a user interaction, extract any learnable preferences.

EXISTING MEMORY: {json.dumps(existing)}

THIS TURN:
- User asked: {msg}
- SQL used: {sql[:200] if sql else 'none'}
- Chart type: {chart}
- Data sample: {json.dumps(df_rows[:2]) if df_rows else 'none'}

Extract preferences and return ONLY a JSON object merging with existing memory.
Rules:
- Only update a field if you have strong evidence from this turn
- Never delete existing preferences unless contradicted
- Keep all values as simple strings or lists

Return this exact structure (fill what you can infer, keep existing values for the rest):
{{
  "preferred_chart": "{existing.get('preferred_chart', '')}",
  "focus_brands": {json.dumps(existing.get('focus_brands', []))},
  "focus_years": {json.dumps(existing.get('focus_years', []))},
  "focus_metrics": {json.dumps(existing.get('focus_metrics', []))},
  "preferred_filters": {json.dumps(existing.get('preferred_filters', {}))},
  "expertise_level": "{existing.get('expertise_level', 'intermediate')}",
  "last_topics": {json.dumps((existing.get('last_topics') or [])[-4:] + ([msg[:60]] if msg else []))}
}}"""

        messages = [{"role": "system", "content": system},
                    {"role": "user", "content": "Extract preferences from this interaction."}]

        raw = await _groq(messages, max_tokens=300)
        new_memory = _parse_json(raw)

        if isinstance(new_memory, dict) and new_memory:
            save_memory(user_id, new_memory)
            print(f"[memory_node] saved: focus_brands={new_memory.get('focus_brands')} preferred_chart={new_memory.get('preferred_chart')}")

    except Exception as e:
        print(f"[memory_node] failed (non-fatal): {e}")

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


def route_after_board(state: AgentState) -> str:
    """After board_node: if re-routed to visualise (SQL mod), go to chart_node."""
    if state.get("intent") == "visualise" and state.get("sql"):
        return "chart_node"
    return "memory_node"


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
    g.add_conditional_edges("board_node", route_after_board, {
        "chart_node":  "chart_node",
        "memory_node": "memory_node",
    })
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
