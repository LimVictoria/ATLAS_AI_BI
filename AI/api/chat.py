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
        # Fetch live data preview
        try:
            from metrics import METRICS, get_metric_sql
            from agent.nodes import run_query, _clean_df
            sql = get_metric_sql(c.metric_id, c.filters or {})
            if sql:
                df  = run_query(sql)
                df  = _clean_df(df)
                rows = df.head(3).to_dict(orient="records")
                preview = " | ".join(
                    ", ".join(f"{k}={v}" for k,v in row.items())
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
        "user_memory":     "",
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
        if "rate limit" in err.lower() or "429" in err:
            raise HTTPException(status_code=429, detail="Groq rate limit reached. Please wait a few seconds.")
        import traceback
        print(f"[chat] Graph error: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=err)

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
def get_board():
    return {"board_state": load_board("default")}

@router.post("/board")
def post_board(req: BoardStateRequest):
    ok = save_board("default", req.board_state)
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
    """Re-render an existing card with a different chart type using its stored SQL."""
    from agent.nodes import run_query, _clean_df, _build_chart, _infer_meta, _smart_available_charts
    try:
        df = run_query(req.sql)
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
        "sql":              req.sql,
    }
