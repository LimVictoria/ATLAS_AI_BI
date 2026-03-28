"""
ATLAS BI — /chat endpoint — LangGraph agent
"""
import os
import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

from db.supabase import save_message, load_messages, clear_messages, save_board, load_board
from agent.nodes import get_graph, AgentState

router = APIRouter(prefix="/chat", tags=["chat"])


def _load_user_memory(user_id: str) -> dict:
    """Load user memory from Supabase, return empty dict on any failure."""
    try:
        from db.supabase import load_memory
        result = load_memory(user_id)
        return result if isinstance(result, dict) else {}
    except Exception as e:
        print(f"[memory] load skipped (table may not exist yet): {e}")
        return {}


# ── Board context builder ──────────────────────────────────────────────────────

def build_board_context(board_context) -> str:
    if not board_context or not board_context.charts_on_canvas:
        return "\nBOARD_CONTEXT: Canvas is empty — no charts yet.\n"

    lines = [f"\nBOARD_CONTEXT: {len(board_context.charts_on_canvas)} chart(s) on canvas:"]
    for c in board_context.charts_on_canvas:
        sel = " ← SELECTED" if c.id in (board_context.selected_ids or []) else ""
        # Build active filters description for AI context
        active_filters = c.filters or {}
        filter_desc = ""
        if active_filters:
            parts = [f"{k}={v}" for k, v in active_filters.items() if v]
            filter_desc = f" | ACTIVE_FILTERS: {', '.join(parts)}"

        lines.append(
            f"  - id={c.id} | title='{c.title}' | chart_type={c.chart_type}"
            f"{filter_desc}{sel}"
        )

        # Fetch live data preview — use filtered SQL so AI sees same data as user
        try:
            from agent.nodes import run_query, _clean_df
            from api.filters import build_where_from_filters
            import re as _re2

            # Use card.sql (which has filters applied) for the preview
            card_sql = c.sql or ""
            if not card_sql and c.base_sql:
                # Re-apply filters to base_sql for preview
                card_sql = c.base_sql
                if active_filters:
                    where = build_where_from_filters(active_filters)
                    if where:
                        new_cond = _re2.sub(r'(?i)^\s*WHERE\s+', '', where).strip()
                        if _re2.search(r'WHERE', card_sql, _re2.IGNORECASE):
                            gm = _re2.search(r'(GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT)', card_sql, _re2.IGNORECASE)
                            card_sql = (card_sql[:gm.start()].rstrip() + f' AND {new_cond} ' + card_sql[gm.start():]) if gm else card_sql.rstrip() + f' AND {new_cond}'
                        else:
                            gm = _re2.search(r'(GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT)', card_sql, _re2.IGNORECASE)
                            card_sql = (card_sql[:gm.start()].rstrip() + f' WHERE {new_cond} ' + card_sql[gm.start():]) if gm else card_sql.rstrip() + f' WHERE {new_cond}'

            if card_sql:
                df   = run_query(card_sql)
                df   = _clean_df(df)
                rows = df.head(3).to_dict(orient="records")
                preview = " | ".join(
                    ", ".join(f"{k}={v}" for k, v in row.items())
                    for row in rows
                )
                lines.append(f"    data_preview (filtered): {preview}")
        except Exception as preview_err:
            print(f"[board_context] preview failed for card {c.id}: {preview_err}")
            pass

    selected = [c for c in board_context.charts_on_canvas
                if c.id in (board_context.selected_ids or [])]
    if selected:
        s = selected[0]
        # Include base_sql so sql_node knows what dimensions the selected card was showing
        card_base_sql = getattr(s, "base_sql", "") or getattr(s, "sql", "") or ""
        active_filters = s.filters or {}
        filter_desc = ""
        if active_filters:
            parts = [f"{k}={v}" for k, v in active_filters.items() if v]
            filter_desc = f" | ACTIVE_FILTERS: {', '.join(parts)}"
        lines.append(f"\nSELECTED CARD: id={s.id}, title='{s.title}', chart_type={s.chart_type}{filter_desc}")
        if card_base_sql:
            lines.append(f"SELECTED CARD BASE_SQL: {card_base_sql}")
    else:
        lines.append("\nSELECTED CARD: none")
    return "\n".join(lines) + "\n"


# ── Models ─────────────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str
    content: str

class BoardCard(BaseModel):
    id: str
    title: str
    metric_id: str
    chart_type: str
    filters: Optional[dict] = {}
    selected: Optional[bool] = False
    sql: Optional[str] = ""
    base_sql: Optional[str] = ""

class BoardContext(BaseModel):
    charts_on_canvas: Optional[list[BoardCard]] = []
    selected_ids: Optional[list[str]] = []

class ChatRequest(BaseModel):
    session_id: str
    message: str
    history: Optional[list[ChatMessage]] = []
    board_context: Optional[BoardContext] = None
    user_id: Optional[str] = "default"

class BoardStateRequest(BaseModel):
    board_state: list
    user_id: Optional[str] = "default"


# ── Chat endpoint ──────────────────────────────────────────────────────────────

@router.post("/")
async def chat(req: ChatRequest):
    try:
        board_prompt = build_board_context(req.board_context)
    except Exception as e:
        import traceback
        print(f"[chat] build_board_context failed: {traceback.format_exc()}")
        board_prompt = "\nBOARD_CONTEXT: Error reading board context.\n"

    history = [
        {"role": m.role, "content": m.content if isinstance(m.content, str) else json.dumps(m.content)}
        for m in (req.history or [])
    ]

    initial_state: AgentState = {
        "user_message":    req.message,
        "history":         history,
        "board_context":   board_prompt,
        "user_memory":     json.dumps(_load_user_memory(getattr(req, "user_id", None) or "default")),
        "intent":          "",
        "selected_card_id": None,
        "sql":             "",
        "sql_error":       "",
        "sql_retries":     0,
        "df_rows":         [],
        "df_columns":      [],
        "chart_json":      "",
        "chart_type":      "",
        "available_charts": [],
        "chart_title":     "",
        "chart_category":  "General",
        "narrative":       "",
        "ui_actions":      [],
        "replace_card_id": None,
        "long_sql":        "",
        "wide_sql":        "",
        "is_wide":         False,
        "pivot_col":       "",
    }

    try:
        graph  = get_graph()
        result = await graph.ainvoke(initial_state)
    except Exception as e:
        err = str(e)
        import traceback
        tb = traceback.format_exc()
        print(f"[chat] FULL ERROR:\n{tb}")
        if "rate limit" in err.lower() or "429" in err:
            raise HTTPException(status_code=429, detail="Groq rate limit reached. Please wait a few seconds.")
        # Return error as narrative instead of 500 so frontend shows it
        return {
            "narrative": f"I ran into an error: {err[:200]}. Please try again.",
            "ui_actions": [],
            "fallback_sql": "",
            "error": err[:500],
        }

    narrative  = result.get("narrative", "Done.")
    ui_actions = result.get("ui_actions", [])

    # Persist — non-fatal if Supabase is down
    try:
        save_message("default", "user",      req.message,  [])
        save_message("default", "assistant", narrative,     ui_actions)
    except Exception as save_err:
        print(f"[chat] save_message failed (non-fatal): {save_err}")

    try:
        import json as _json, math

        def _deep_sanitize(obj, depth=0):
            """Recursively sanitize any value to be JSON-safe. Handles NaN, Inf,
            non-serializable types, circular refs (depth limit), and bytes."""
            if depth > 20:
                return str(obj)
            if obj is None:
                return None
            if isinstance(obj, bool):
                return obj
            if isinstance(obj, float):
                if math.isnan(obj) or math.isinf(obj):
                    return 0
                return obj
            if isinstance(obj, (int, str)):
                return obj
            if isinstance(obj, dict):
                return {str(k): _deep_sanitize(v, depth+1) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_deep_sanitize(v, depth+1) for v in obj]
            if isinstance(obj, bytes):
                return obj.decode("utf-8", errors="replace")
            # Unknown type — convert to string
            try:
                _json.dumps(obj)
                return obj
            except Exception:
                return str(obj)

        payload = _deep_sanitize({
            "narrative": str(narrative) if narrative else "Done.",
            "ui_actions": ui_actions if isinstance(ui_actions, list) else [],
            "fallback_sql": result.get("sql", "") or ""
        })

        # Per-action validation — only skip truly unsalvageable actions, never wipe all
        safe_actions = []
        for action in payload.get("ui_actions", []):
            try:
                _json.dumps(action)
                safe_actions.append(action)
            except Exception as e:
                print(f"[chat] skipping unsalvageable action (type={action.get('action','?')}): {e}")
        payload["ui_actions"] = safe_actions

        return payload
    except Exception as serial_err:
        print(f"[chat] serialization fatal: {serial_err}")
        return {"narrative": str(narrative) if narrative else "Done.", "ui_actions": [], "fallback_sql": ""}


# ── History / Board endpoints ──────────────────────────────────────────────────

@router.get("/history/{session_id}")
def get_history(session_id: str):
    msgs = load_messages("default", limit=40)
    return {"session_id": session_id, "messages": msgs}

@router.delete("/history/{session_id}")
def clear_history(session_id: str):
    ok = clear_messages("default")
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to clear history")
    return {"message": "History cleared"}

@router.get("/board")
def get_board_default():
    return {"board_state": load_board("default")}

@router.get("/board/{user_id}")
def get_board(user_id: str):
    return {"board_state": load_board(user_id)}

@router.post("/board")
def post_board(req: BoardStateRequest):
    user_id = req.user_id or "default"
    try:
        save_board(user_id, req.board_state)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to save board")
    return {"message": "Board saved"}

@router.post("/board/save")
def post_board_save(req: BoardStateRequest):
    user_id = req.user_id or "default"
    # Strip chart_data before saving — too large for Supabase, regenerated from SQL on load
    stripped = []
    for card in (req.board_state or []):
        if isinstance(card, dict):
            c = {k: v for k, v in card.items() if k != "chart_data"}
            stripped.append(c)
        else:
            stripped.append(card)
    try:
        save_board(user_id, stripped)  # save_board returns None — don't check return value
    except Exception as e:
        print(f"[board/save] failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save board")
    return {"message": "Board saved"}


@router.get("/data/schema")
def get_schema():
    """Return the current dynamic schema guide."""
    try:
        from db.duckdb_session import get_schema_guide, get_table_names
        return {"schema": get_schema_guide(), "tables": get_table_names()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/reload")
def reload_data():
    """Force reload all data files and regenerate schema guide."""
    try:
        from db.duckdb_session import reload_data as _reload
        from api.filters import invalidate_cache
        result = _reload()
        invalidate_cache()  # force filter dimensions to rebuild from new data
        return {"status": "reloaded", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/warnings")
def get_dq_warnings():
    """Return data quality warnings from the last data load."""
    try:
        from db.duckdb_session import get_dq_warnings
        return {"warnings": get_dq_warnings()}
    except Exception as e:
        return {"warnings": []}


class ToggleFormatRequest(BaseModel):
    long_sql: str
    wide_sql: str
    is_wide: bool          # True = switching TO wide, False = switching TO long
    chart_type: str
    title: str = "Chart"
    category: str = "General"
    filters: Optional[dict] = {}
    pivot_col: Optional[str] = ""

@router.post("/toggle_format")
def toggle_format(req: ToggleFormatRequest):
    """Switch a card between wide (pivot) and long format."""
    from agent.nodes import run_query, _clean_df, _build_chart, _infer_meta, _smart_available_charts
    import re as _re

    # Use the appropriate SQL based on target format
    sql = req.wide_sql if req.is_wide else req.long_sql
    if not sql:
        raise HTTPException(status_code=400, detail="SQL not available for this format")

    try:
        df = run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL error: {e}")
    if df.empty:
        raise HTTPException(status_code=400, detail="Query returned no rows")

    df   = _clean_df(df)
    cols = df.columns.tolist()
    meta = _infer_meta(cols)
    meta["category"] = req.category

    # Wide format defaults to table, long format uses requested chart type
    chart_type = "table" if req.is_wide else req.chart_type

    try:
        chart_json = _build_chart(df, meta, chart_type)
    except Exception:
        chart_json = _build_chart(df, meta, "table")
        chart_type = "table"

    available = _smart_available_charts(cols, df, chart_type)

    import re as _re2
    chart_json = _re2.sub(r'NaN', '0', chart_json)
    chart_json = _re2.sub(r'Infinity', '0', chart_json)

    return {
        "chart":            chart_json,
        "chart_type":       chart_type,
        "available_charts": available,
        "row_count":        len(df),
        "sql":              sql,
        "is_wide":          req.is_wide,
    }



# ── Star schema join map ───────────────────────────────────────────────────────
_STAR_JOIN_MAP = {
    "dim_truck":     {"alias": "dt", "fk": "f.truck_id = dt.truck_id"},
    "dim_component": {"alias": "dc", "fk": "f.component_id = dc.component_id"},
    "dim_workshop":  {"alias": "dw", "fk": "f.workshop_id = dw.workshop_id"},
}

_COL_SOURCE_TABLE: dict = {}

def _get_col_source_table() -> dict:
    global _COL_SOURCE_TABLE
    if _COL_SOURCE_TABLE:
        return _COL_SOURCE_TABLE
    try:
        from db.duckdb_session import _tables_meta
        priority = ["fact_maintenance_event","dim_truck","dim_component","dim_workshop"]
        result = {}
        for tname in priority:
            if tname in _tables_meta:
                for col in _tables_meta[tname]["df"].columns:
                    result[col.lower()] = tname
        _COL_SOURCE_TABLE = result
    except Exception:
        pass
    return _COL_SOURCE_TABLE


def derive_source_sql(view_sql: str) -> str:
    """Derive star schema source SQL from a v_maintenance_full query."""
    import re as _re
    if "v_maintenance_full" not in view_sql.lower():
        return view_sql
    col_map = _get_col_source_table()
    pat_select = _re.compile(r"SELECT\s+(.+?)\s+FROM", _re.IGNORECASE | _re.DOTALL)
    pat_where  = _re.compile(r"WHERE\s+(.+?)(?=GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|$)", _re.IGNORECASE | _re.DOTALL)
    pat_group  = _re.compile(r"GROUP\s+BY\s+(.+?)(?=ORDER\s+BY|HAVING|LIMIT|$)", _re.IGNORECASE | _re.DOTALL)
    pat_order  = _re.compile(r"ORDER\s+BY\s+(.+?)(?=LIMIT|$)", _re.IGNORECASE | _re.DOTALL)
    pat_words  = _re.compile(r"[a-zA-Z_][a-zA-Z_0-9]*")
    pat_qualified = _re.compile(r"[a-zA-Z_][a-zA-Z_0-9]*\.[a-zA-Z_][a-zA-Z_0-9]*")
    select_match = pat_select.search(view_sql)
    if not select_match:
        return view_sql
    where_match = pat_where.search(view_sql)
    group_match = pat_group.search(view_sql)
    order_match = pat_order.search(view_sql)
    all_words = set(w.lower() for w in pat_words.findall(view_sql))
    needed_dims = set()
    for col in all_words:
        src = col_map.get(col)
        if src and src != "fact_maintenance_event" and src in _STAR_JOIN_MAP:
            needed_dims.add(src)
    alias_map = {"fact_maintenance_event": "f"}
    for dim in needed_dims:
        alias_map[dim] = _STAR_JOIN_MAP[dim]["alias"]
    SQL_KEYWORDS = {
        "from","where","group","by","order","having","limit","and","or","not",
        "in","like","between","is","null","true","false","as","on","join",
        "select","sum","avg","count","max","min","round","nullif","case",
        "when","then","else","end","asc","desc","distinct","inner","left",
        "right","outer","cross","using","over","partition","window"
    }
    def qualify_token(word: str) -> str:
        src = col_map.get(word.lower())
        if src and src in alias_map:
            return f"{alias_map[src]}.{word}"
        return word
    def qualify_expr(expr: str) -> str:
        qualified_spans = set()
        for m in pat_qualified.finditer(expr):
            for i in range(m.start(), m.end()):
                qualified_spans.add(i)
        result = []
        i = 0
        while i < len(expr):
            if i in qualified_spans:
                result.append(expr[i]); i += 1; continue
            m = _re.match(r"[a-zA-Z_][a-zA-Z_0-9]*", expr[i:])
            if m:
                word = m.group(0)
                if word.lower() not in SQL_KEYWORDS:
                    result.append(qualify_token(word))
                else:
                    result.append(word)
                i += len(word)
            else:
                result.append(expr[i]); i += 1
        return "".join(result)
    joins = ["FROM fact_maintenance_event f"]
    for dim in sorted(needed_dims):
        j = _STAR_JOIN_MAP[dim]
        joins.append(f"JOIN {dim} {j['alias']} ON {j['fk']}")
    select_q = qualify_expr(select_match.group(1))
    where_q  = f"WHERE {qualify_expr(where_match.group(1).strip())}" if where_match else ""
    group_q  = f"GROUP BY {qualify_expr(group_match.group(1).strip())}" if group_match else ""
    order_q  = f"ORDER BY {qualify_expr(order_match.group(1).strip())}" if order_match else ""
    parts = ["-- Star schema equivalent (for data engineers)", f"SELECT {select_q}"] \
          + joins \
          + [p for p in [where_q, group_q, order_q] if p]
    return "\n".join(parts)


class DeriveSqlRequest(BaseModel):
    sql: str

@router.post("/derive_source_sql")
def derive_source_sql_endpoint(req: DeriveSqlRequest):
    """Derive star schema source SQL from a v_maintenance_full view query."""
    try:
        source_sql = derive_source_sql(req.sql)
        return {"source_sql": source_sql, "view_sql": req.sql}
    except Exception as e:
        return {"source_sql": req.sql, "view_sql": req.sql, "error": str(e)}


class RerenderRequest(BaseModel):
    sql: str
    chart_type: str
    title: str = "Chart"
    category: str = "General"
    filters: Optional[dict] = {}

def _qualify_conditions(conditions: str, sql: str, dims: dict) -> str:
    """
    Qualify ambiguous column names in WHERE conditions for JOIN queries.
    Detects table aliases from the SQL FROM/JOIN clauses and prefixes columns.
    e.g. "year = 2024" → "f.year = 2024" when f is alias for fact_maintenance_event
    """
    import re as _re2

    # Only qualify if SQL has JOINs
    if not _re2.search(r'\bJOIN\b', sql, _re2.IGNORECASE):
        return conditions  # single table query — no ambiguity

    # Extract table aliases from FROM and JOIN clauses
    # Matches: FROM table_name alias, JOIN table_name alias, FROM table_name AS alias
    alias_map = {}  # alias → table_name
    table_map = {}  # table_name → alias
    pattern = _re2.findall(
        r'(?:FROM|JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?',
        sql, _re2.IGNORECASE
    )
    for table_name, alias in pattern:
        alias = alias or table_name  # if no alias, use table name itself
        if alias.upper() not in {"ON", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "SELECT"}:
            alias_map[alias.lower()] = table_name.lower()
            table_map[table_name.lower()] = alias

    if not alias_map:
        return conditions

    # Build column → table mapping from loaded schema
    try:
        from db.duckdb_session import _tables_meta
        col_to_table: dict = {}
        for tname, meta in _tables_meta.items():
            for col in meta["df"].columns:
                if tname.lower() in table_map:  # only tables referenced in this SQL
                    col_to_table[col.lower()] = tname.lower()
    except Exception:
        return conditions

    # Qualify each condition clause
    qualified_parts = []
    for clause in _re2.split(r'\s+AND\s+', conditions, flags=_re2.IGNORECASE):
        clause = clause.strip()
        # Extract column name (before =, IN, LIKE, BETWEEN, IS)
        col_match = _re2.match(r'^(\w+)\s*(?:=|IN|LIKE|BETWEEN|IS)', clause, _re2.IGNORECASE)
        if col_match:
            col = col_match.group(1).lower()
            # Check if already qualified (has a dot)
            if '.' not in clause and col in col_to_table:
                t = col_to_table[col]
                alias = table_map.get(t, t)
                clause = clause.replace(col_match.group(1), f"{alias}.{col_match.group(1)}", 1)
        qualified_parts.append(clause)

    return " AND ".join(qualified_parts)


@router.post("/rerender")
def rerender_chart(req: RerenderRequest):
    """Re-render a card with a different chart type and/or filters using stored SQL."""
    from agent.nodes import run_query, _clean_df, _build_chart, _infer_meta, _smart_available_charts
    from api.filters import build_where_from_filters, _get_dims

    print(f"[rerender] sql={req.sql[:80]!r} filters={req.filters} chart_type={req.chart_type}")

    # Build WHERE clause and log it for debugging
    if req.filters:
        _debug_where = build_where_from_filters(req.filters)
        print(f"[rerender] WHERE clause: {_debug_where[:120]}")

    # Inject UI filters into SQL — APPEND to existing WHERE (never strip intent filters)
    import re as _re
    sql = req.sql
    if req.filters:
        where = build_where_from_filters(req.filters)
        if where:
            # Strip leading WHERE keyword — we'll insert conditions only
            new_conditions = _re.sub(r'(?i)^\s*WHERE\s+', '', where).strip()
            # Qualify ambiguous column names for JOIN queries
            new_conditions = _qualify_conditions(new_conditions, sql, _get_dims())
            print(f"[rerender] qualified conditions: {new_conditions[:120]}")
            sql_upper = sql.upper()
            if _re.search(r'\bWHERE\b', sql_upper):
                group_match = _re.search(r'\b(GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT)\b', sql, _re.IGNORECASE)
                if group_match:
                    insert_at = group_match.start()
                    sql = sql[:insert_at].rstrip() + f' AND {new_conditions} ' + sql[insert_at:]
                else:
                    sql = sql.rstrip() + f' AND {new_conditions}'
            else:
                group_match = _re.search(r'\b(GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT)\b', sql, _re.IGNORECASE)
                if group_match:
                    insert_at = group_match.start()
                    sql = sql[:insert_at].rstrip() + f' WHERE {new_conditions} ' + sql[insert_at:]
                else:
                    sql = sql.rstrip() + f' WHERE {new_conditions}'

    try:
        df = run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL error: {e}")
    if df.empty:
        raise HTTPException(status_code=400, detail="Query returned no rows")
    df   = _clean_df(df)
    cols = df.columns.tolist()
    meta = _infer_meta(cols)
    meta["category"] = req.category

    # Enrich meta for chart types that need specific columns
    chart_type = req.chart_type
    cols_lower  = [c.lower() for c in cols]
    numeric_cols = [c for c in cols if str(df[c].dtype) in {"int64","float64","int32","float32"}]
    cat_cols_found = [c for c in cols if c.lower() in
                      {"brand","fleet_segment","component_category","maintenance_type",
                       "workshop_type","region","failure_type","criticality_level",
                       "plate_number","workshop_name","component_name","month_name"}]

    # For heatmap: need x_col (time/cat), y_col (cat), z_col (numeric)
    if chart_type == "heatmap":
        if len(cat_cols_found) >= 2:
            meta["x_col"] = cat_cols_found[1]  # e.g. month_name
            meta["y_col"] = cat_cols_found[0]  # e.g. brand
        if numeric_cols:
            meta["z_col"] = numeric_cols[0]

    # For stacked_bar: use _infer_meta which correctly separates time/entity/dimension cols
    # cat_cols_found can miss brand when year/month are integer cols that look categorical
    elif chart_type == "stacked_bar":
        # _infer_meta already ran above and set x_col/group_col correctly
        # Only override if it didn't set group_col
        if not meta.get("group_col"):
            if len(cat_cols_found) >= 2:
                meta["x_col"] = cat_cols_found[0]
                meta["group_col"] = cat_cols_found[1]
            elif len(cat_cols_found) == 1:
                meta["group_col"] = cat_cols_found[0]
        if numeric_cols and not meta.get("y_col"):
            meta["y_col"] = numeric_cols[0]

    # For scatter: need x_col and y_col both numeric, label_col for text
    elif chart_type == "scatter":
        if len(numeric_cols) >= 2:
            meta["x_col"] = numeric_cols[0]
            meta["y_col"] = numeric_cols[1]
        if cat_cols_found:
            meta["label_col"] = cat_cols_found[0]
        if len(numeric_cols) >= 3:
            meta["size_col"] = numeric_cols[2]

    # For boxplot: need x_col (cat), and q1/median/q3 cols
    elif chart_type == "boxplot":
        stat_cols = {c.lower(): c for c in cols if c.lower() in
                     {"q1","cost_q1","median","cost_median","q3","cost_q3",
                      "min","cost_min","max","cost_max","mean","cost_mean"}}
        if stat_cols:
            meta["q1_col"]     = stat_cols.get("q1") or stat_cols.get("cost_q1") or numeric_cols[0] if numeric_cols else None
            meta["median_col"] = stat_cols.get("median") or stat_cols.get("cost_median")
            meta["q3_col"]     = stat_cols.get("q3") or stat_cols.get("cost_q3")

    # For treemap: need parent_col, label_col, value_col
    elif chart_type == "treemap":
        if len(cat_cols_found) >= 2:
            meta["parent_col"] = cat_cols_found[0]
            meta["x_col"]      = cat_cols_found[1]
        elif cat_cols_found:
            meta["x_col"] = cat_cols_found[0]
        if numeric_cols:
            meta["y_col"] = numeric_cols[0]

    try:
        chart_json = _build_chart(df, meta, chart_type)
    except Exception as e:
        print(f"[rerender] _build_chart failed for {chart_type}: {e}, falling back to table")
        chart_json = _build_chart(df, meta, "table")
        chart_type = "table"
    available = _smart_available_charts(cols, df, chart_type)
    import re as _re
    # Sanitize chart_json — replace any NaN/Inf Plotly may have embedded
    chart_json = _re.sub(r'\bNaN\b', '0', chart_json)
    chart_json = _re.sub(r'\bInfinity\b', '0', chart_json)
    chart_json = _re.sub(r'-Infinity', '0', chart_json)

    return {
        "chart":            chart_json,
        "chart_type":       req.chart_type,
        "available_charts": available,
        "row_count":        len(df),
        "sql":              sql,
    }
