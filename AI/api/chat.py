"""
ATLAS BI — /chat endpoint (Path B: dynamic SQL agent)
"""
import os
import json
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from db.supabase import save_message, load_messages, clear_messages, save_board, load_board
from agent.nodes import sql_tool, build_agent_system_prompt
from api.query import _build_chart

router = APIRouter(prefix="/chat", tags=["chat"])

GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL       = "llama-3.3-70b-versatile"
GROQ_MODEL_FALLBACK = "llama-3.1-8b-instant"
GROQ_URL         = "https://api.groq.com/openai/v1/chat/completions"
MAX_SQL_RETRIES  = 3


# ── Pydantic models ────────────────────────────────────────────────────────────

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
    user_id: str = "default"
    board_state: list = []


# ── Groq call with retry + fallback ───────────────────────────────────────────

async def call_groq(messages: list[dict], model: str = None, max_tokens: int = 1500) -> str:
    import asyncio
    chosen_model = model or GROQ_MODEL
    async with httpx.AsyncClient(timeout=45) as client:
        for attempt in range(3):
            try:
                response = await client.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                    json={"model": chosen_model, "messages": messages, "temperature": 0.1, "max_tokens": max_tokens},
                )
                if response.status_code == 429:
                    retry_after = float(response.headers.get("retry-after", 2 * (attempt + 1)))
                    wait = min(retry_after, 8)
                    print(f"[Groq] 429 on {chosen_model}, waiting {wait}s")
                    await asyncio.sleep(wait)
                    if chosen_model != GROQ_MODEL_FALLBACK:
                        chosen_model = GROQ_MODEL_FALLBACK
                    continue
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"]
            except httpx.TimeoutException:
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue
                raise
        raise Exception("Groq rate limit exceeded after 3 attempts. Please wait a moment and try again.")


# ── Board context builder ──────────────────────────────────────────────────────

def build_board_context(board_context: BoardContext | None) -> str:
    if not board_context or not board_context.charts_on_canvas:
        return "\nBOARD_CONTEXT: Canvas is empty — no charts yet.\n"

    lines = [f"\nBOARD_CONTEXT: {len(board_context.charts_on_canvas)} chart(s) on canvas:"]
    for c in board_context.charts_on_canvas:
        selected_marker = " ← SELECTED" if c.id in (board_context.selected_ids or []) else ""
        lines.append(
            f"  - id={c.id} | title='{c.title}' | metric={c.metric_id} "
            f"| chart_type={c.chart_type} | filters={json.dumps(c.filters)}{selected_marker}"
        )
        # Fetch live data preview so LLM can answer "why" questions
        try:
            from api.query import QueryRequest, run_metric as _run_metric
            req = QueryRequest(metric_id=c.metric_id, chart_type="table", filters=c.filters or {})
            result = _run_metric(req)
            summary = result.get("summary", [])
            if summary:
                row_strs = [", ".join(f"{k}={v}" for k, v in row.items()) for row in summary[:3]]
                lines.append(f"    data_preview: {' | '.join(row_strs)}")
        except Exception:
            pass

    selected = [c for c in board_context.charts_on_canvas if c.id in (board_context.selected_ids or [])]
    if selected:
        s = selected[0]
        lines.append(f"\nSELECTED CARD: id={s.id}, title='{s.title}', metric={s.metric_id}, chart_type={s.chart_type}")
    else:
        lines.append("\nSELECTED CARD: none")

    return "\n".join(lines) + "\n"


# ── Parse and clean LLM JSON response ─────────────────────────────────────────

def parse_agent_response(raw: str) -> dict:
    clean = raw.strip()
    if clean.startswith("```"):
        lines = clean.split("\n")
        clean = "\n".join(lines[1:])
        if clean.endswith("```"):
            clean = clean[:-3]
    return json.loads(clean.strip())


# ── Main agent loop with SQL self-correction ───────────────────────────────────

async def run_agent(req: ChatRequest) -> dict:
    board_str  = build_board_context(req.board_context)
    system_msg = build_agent_system_prompt(board_str)

    # Build message history for LLM
    messages = [{"role": "system", "content": system_msg}]
    for msg in (req.history or [])[-10:]:
        messages.append({
            "role": msg.role,
            "content": msg.content if isinstance(msg.content, str) else json.dumps(msg.content)
        })
    messages.append({"role": "user", "content": req.message})

    # ── First LLM call: plan SQL + chart ──────────────────────────────────────
    raw = await call_groq(messages)
    try:
        parsed = parse_agent_response(raw)
    except json.JSONDecodeError:
        return {
            "narrative": raw.strip(),
            "ui_actions": [],
            "fallback_sql": None,
        }

    narrative  = parsed.get("narrative", "Here is the data you requested.")
    sql        = parsed.get("sql")
    chart_type = parsed.get("chart_type", "bar")
    title      = parsed.get("title", "Query Result")
    category   = parsed.get("category", "Cost")
    ui_action  = parsed.get("ui_action", "add_chart")
    card_id    = parsed.get("card_id")

    # Sanitise narrative
    if "{" in narrative and "}" in narrative:
        narrative = narrative.split("{")[0].strip() or "Here is the data you requested."

    # ── Conversational answer — no chart needed ────────────────────────────────
    if ui_action == "none" or not sql:
        return {"narrative": narrative, "ui_actions": [], "fallback_sql": sql}

    # ── Execute SQL with self-correction loop ──────────────────────────────────
    result = None
    last_error = None

    for attempt in range(MAX_SQL_RETRIES):
        result = sql_tool(sql, chart_type_hint=chart_type, title=title, category=category)

        if not result.get("error"):
            break  # Success

        last_error = result["error"]
        print(f"[Agent] SQL attempt {attempt+1} failed: {last_error}")

        if attempt < MAX_SQL_RETRIES - 1:
            # Ask LLM to fix the SQL
            correction_prompt = f"""Your SQL query failed with this error:
ERROR: {last_error}

Your original SQL:
{sql}

Fix the SQL. Remember: only use columns from the SCHEMA GUIDE. Return ONLY the corrected SQL, nothing else."""
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content": correction_prompt})
            correction = await call_groq(messages, max_tokens=500)
            # Extract just the SQL
            sql = correction.strip().strip("```sql").strip("```").strip()
            print(f"[Agent] Corrected SQL attempt {attempt+2}: {sql[:100]}...")

    if result and result.get("error"):
        return {
            "narrative": f"I couldn't build that query after {MAX_SQL_RETRIES} attempts. {last_error}. Try rephrasing your question.",
            "ui_actions": [],
            "fallback_sql": sql,
        }

    # ── Build ui_action ────────────────────────────────────────────────────────
    if ui_action == "modify_chart" and card_id:
        action = {
            "action":     "modify_chart",
            "card_id":    card_id,
            "chart_type": result["chart_type"],
            "chart_data": result,
            "title":      title,
        }
    else:
        action = {
            "action":           "add_chart",
            "metric_id":        f"dynamic_{title.lower().replace(' ','_')}",
            "title":            title,
            "chart_type":       result["chart_type"],
            "chart_data":       result,
            "filters":          parsed.get("filters_used", {}),
            "sql":              sql,
        }

    return {
        "narrative":    narrative,
        "ui_actions":   [action],
        "fallback_sql": None,
    }


# ── Chat endpoint ──────────────────────────────────────────────────────────────

@router.post("/")
async def chat(req: ChatRequest):
    try:
        response = await run_agent(req)
    except Exception as e:
        err = str(e)
        if "rate limit" in err.lower() or "429" in err:
            raise HTTPException(status_code=429, detail="Groq rate limit reached. Please wait a few seconds and try again.")
        raise HTTPException(status_code=500, detail=err)

    # Persist messages
    save_message(req.session_id, "user", req.message)
    save_message(req.session_id, "assistant", response.get("narrative", ""), response.get("ui_actions", []))

    return response


# ── History endpoints ──────────────────────────────────────────────────────────

@router.get("/history/{session_id}")
def get_history(session_id: str):
    msgs = load_messages(session_id, limit=40)
    return {"session_id": session_id, "messages": msgs}


@router.delete("/history/{session_id}")
def delete_history(session_id: str):
    clear_messages(session_id)
    return {"message": "History cleared"}


# ── Board persistence endpoints ────────────────────────────────────────────────

@router.post("/board/save")
def save_board_state(req: BoardStateRequest):
    save_board(req.board_state, req.user_id)
    return {"message": "Board saved"}


@router.get("/board/{user_id}")
def load_board_state(user_id: str = "default"):
    state = load_board(user_id)
    return {"user_id": user_id, "board_state": state}
