"""
ATLAS BI — /chat endpoint — LangGraph agent
"""
import os
import json
from fastapi import APIRouter, HTTPException
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
        lines.append(
            f"  - id={c.id} | title='{c.title}' | chart_type={c.chart_type}"
            f" | filters={json.dumps(c.filters or {})}{sel}"
        )
        # Fetch live data preview using card's stored SQL
        try:
            from agent.nodes import run_query, _clean_df
            card_sql = c.sql or ""
            if card_sql:
                df   = run_query(card_sql)
                df   = _clean_df(df)
                rows = df.head(3).to_dict(orient="records")
                preview = " | ".join(
                    ", ".join(f"{k}={v}" for k, v in row.items())
                    for row in rows
                )
                lines.append(f"    data_preview: {preview}")
        except Exception:
            pass

    selected = [c for c in board_context.charts_on_canvas
                if c.id in (board_context.selected_ids or [])]
    if selected:
        s = selected[0]
        lines.append(f"\nSELECTED CARD: id={s.id}, title='{s.title}', chart_type={s.chart_type}")
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

class BoardContext(BaseModel):
    charts_on_canvas: Optional[list[BoardCard]] = []
    selected_ids: Optional[list[str]] = []

class ChatRequest(BaseModel):
    session_id: str
    message: str
    history: Optional[list[ChatMessage]] = []
    board_context: Optional[BoardContext] = None

class BoardStateRequest(BaseModel):
    board_state: list
    user_id: Optional[str] = "default"


# ── Chat endpoint ──────────────────────────────────────────────────────────────

@router.post("/")
async def chat(req: ChatRequest):
    board_prompt = build_board_context(req.board_context)

    history = [
        {"role": m.role, "content": m.content if isinstance(m.content, str) else json.dumps(m.content)}
        for m in (req.history or [])
    ]

    initial_state: AgentState = {
        "user_message":    req.message,
        "history":         history,
        "board_context":   board_prompt,
        "user_memory":     _load_user_memory(req.user_id or "default"),
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

    # Persist
    save_message("default", "user",      req.message,  [])
    save_message("default", "assistant", narrative,     ui_actions)

    return {"narrative": narrative, "ui_actions": ui_actions, "fallback_sql": result.get("sql")}


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
    ok = save_board(user_id, req.board_state)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save board")
    return {"message": "Board saved"}

@router.post("/board/save")
def post_board_save(req: BoardStateRequest):
    user_id = req.user_id or "default"
    ok = save_board(user_id, req.board_state)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save board")
    return {"message": "Board saved"}


class RerenderRequest(BaseModel):
    sql: str
    chart_type: str
    title: str = "Chart"
    category: str = "General"
    filters: Optional[dict] = {}

@router.post("/rerender")
def rerender_chart(req: RerenderRequest):
    """Re-render a card with a different chart type and/or filters using stored SQL."""
    from agent.nodes import run_query, _clean_df, _build_chart, _infer_meta, _smart_available_charts
    from api.filters import build_where_from_filters

    # Inject filters into SQL — strip existing WHERE then insert fresh one
    sql = req.sql
    if req.filters:
        where = build_where_from_filters(req.filters)
        if where:
            import re
            # Remove any existing WHERE clause
            sql = re.sub(
                r'(?i)\s+WHERE\s+.+?(?=\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*$)',
                ' ', sql, flags=re.DOTALL
            ).strip()
            # Find insertion point before GROUP BY / ORDER BY / end
            sql_upper = sql.upper()
            insert_at = len(sql)
            for kw in ['GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT']:
                idx = sql_upper.rfind(kw)
                if idx != -1 and idx < insert_at:
                    insert_at = idx
            sql = sql[:insert_at].rstrip() + ' ' + where + ' ' + sql[insert_at:]

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
    try:
        chart_json = _build_chart(df, meta, req.chart_type)
    except Exception:
        chart_json = _build_chart(df, meta, "table")
        req.chart_type = "table"
    available = _smart_available_charts(cols, df, req.chart_type)
    return {
        "chart":            chart_json,
        "chart_type":       req.chart_type,
        "available_charts": available,
        "row_count":        len(df),
        "sql":              sql,   # return filtered SQL so flip panel shows it
    }
