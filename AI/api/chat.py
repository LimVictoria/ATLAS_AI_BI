"""
ATLAS BI — /chat endpoint
"""
import os
import json
import httpx
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from metrics import METRICS, TIME_SHORTCUTS, get_metrics_index
from db.supabase import get_supabase
from api.query import QueryRequest, run_metric

router = APIRouter(prefix="/chat", tags=["chat"])

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = "llama-3.3-70b-versatile"
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"

METRICS_INDEX = get_metrics_index()

SYSTEM_PROMPT = f"""You are ATLAS, a friendly and concise AI analyst for a fleet maintenance BI platform.

DATASET CONTEXT: Malaysian truck fleet, 2020–2024. Brands: Scania, Volvo, Mercedes-Benz, MAN, Hino.
"This month" = Dec 2024. "Last year" = 2023. "This year" = 2024.

AVAILABLE METRICS:
{json.dumps(METRICS_INDEX, indent=2)}

TIME SHORTCUTS: {json.dumps(list(TIME_SHORTCUTS.keys()))}

CHART TYPES: bar, line, pie, table, pareto

PARETO RULE: Always use "pareto" chart_type when user says "pareto", "80/20", "Pareto analysis", or "which X causes most Y". Pareto shows bars + cumulative % line + 80% reference.

UI ACTIONS:
- add_chart: show a metric on the canvas
- add_filter: add a filter control (brand, year, month, quarter, fleet_segment, component_category, workshop_state, maintenance_type, criticality_level, date_range)
- reset_filters: clear all filters

RESPONSE — return ONLY valid JSON, no markdown, no code blocks:
{{
  "narrative": "Your friendly conversational reply. 1-3 sentences max. Never include JSON or code in here.",
  "ui_actions": [],
  "fallback_sql": null
}}

RULES:
1. ALWAYS add a chart when the user asks ANY data question — "which components fail most", "show cost by brand", "compare brands" all get a chart immediately.
2. Only ask clarifying questions for genuinely ambiguous requests like "show me something interesting".
3. If the user specifies a chart type use it. If not, pick the best one automatically (bar for comparisons, line for trends, pie for proportions).
4. The narrative must be plain English only — friendly, concise, no JSON, no curly braces, no code.
5. For time-aware queries add time_shortcut to the filters object.
6. Multiple charts are fine — if user says "compare X and Y" add two add_chart actions.
7. If no metric matches, set fallback_sql to a DuckDB SQL query against v_maintenance_full.
"""


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    session_id: str
    message: str
    history: Optional[list[ChatMessage]] = []


async def call_groq(messages: list[dict]) -> str:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROQ_MODEL, "messages": messages, "temperature": 0.1, "max_tokens": 1200},
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


def execute_chart_action(action: dict) -> dict | None:
    if action.get("action") != "add_chart":
        return None
    try:
        req = QueryRequest(
            metric_id=action["metric_id"],
            chart_type=action.get("chart_type"),
            filters=action.get("filters", {}),
        )
        data = run_metric(req)
        return {**action, "chart_data": data}
    except Exception as e:
        print(f"[execute_chart_action] Error: {e}")
        return None


def save_message(session_id: str, role: str, content: dict):
    try:
        get_supabase().table("bi_chat_history").insert({
            "session_id": session_id, "role": role,
            "content": content, "created_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        print(f"[Supabase] Save failed: {e}")


def load_history(session_id: str) -> list[dict]:
    try:
        resp = get_supabase().table("bi_chat_history") \
            .select("role,content").eq("session_id", session_id) \
            .order("created_at").limit(20).execute()
        return resp.data or []
    except Exception:
        return []


@router.post("/")
async def chat(req: ChatRequest):
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in (req.history or []):
        messages.append({"role": msg.role,
                         "content": msg.content if isinstance(msg.content, str)
                                    else json.dumps(msg.content)})
    messages.append({"role": "user", "content": req.message})

    try:
        raw = await call_groq(messages)
        clean = raw.strip()
        # Strip markdown fences
        if clean.startswith("```"):
            lines = clean.split("\n")
            clean = "\n".join(lines[1:])
            if clean.endswith("```"):
                clean = clean[:-3]
        parsed = json.loads(clean.strip())
    except json.JSONDecodeError:
        # If LLM returns plain text, wrap it
        parsed = {"narrative": raw.strip(), "ui_actions": [], "fallback_sql": None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Sanitise narrative — strip any JSON leakage
    narrative = parsed.get("narrative", "")
    if "{" in narrative and "}" in narrative:
        # Extract only the text before any JSON
        narrative = narrative.split("{")[0].strip()
        if not narrative:
            narrative = "Here is the data you requested."

    enriched_actions = []
    for action in parsed.get("ui_actions", []):
        if action.get("action") == "add_chart":
            result = execute_chart_action(action)
            enriched_actions.append(result or action)
        else:
            enriched_actions.append(action)

    response = {
        "narrative":   narrative,
        "ui_actions":  enriched_actions,
        "fallback_sql": parsed.get("fallback_sql"),
    }

    save_message(req.session_id, "user",      {"text": req.message})
    save_message(req.session_id, "assistant", response)
    return response


@router.get("/history/{session_id}")
def get_history(session_id: str):
    return {"session_id": session_id, "messages": load_history(session_id)}


@router.delete("/history/{session_id}")
def clear_history(session_id: str):
    try:
        get_supabase().table("bi_chat_history").delete().eq("session_id", session_id).execute()
        return {"message": "History cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
