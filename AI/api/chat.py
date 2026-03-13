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

AVAILABLE METRICS (metric_id → description → available_charts):
{json.dumps(METRICS_INDEX, indent=2)}

TIME SHORTCUTS: {json.dumps(list(TIME_SHORTCUTS.keys()))}

SUPPORTED CHART TYPES:
- bar: comparisons between categories
- line: trends over time
- pie: proportions/ratios
- table: detailed data grid
- pareto: 80/20 analysis — bars + cumulative % line
- waterfall: cumulative buildup / contribution breakdown
- heatmap: 2D intensity grid (use metric: cost_heatmap_brand_month)
- boxplot: distribution, spread, outliers (use metric: cost_distribution_by_brand)
- scatter: correlation between two measures (use metric: cost_vs_downtime_scatter)
- treemap: hierarchical proportions (use metric: fleet_cost_treemap or failure_count_by_component)
- histogram: frequency distribution (use metric: downtime_histogram)
- stacked_bar: breakdown of a total into sub-components (use metric: cost_by_brand_and_component or downtime_by_brand_and_component)

CHART SELECTION RULES:
- ONLY use a chart_type if it appears in the metric's available_charts list
- If user requests a chart type not available for that metric, pick the closest available alternative and mention it
- For heatmap → always use metric cost_heatmap_brand_month
- For scatter/correlation → always use metric cost_vs_downtime_scatter
- For boxplot/distribution → always use metric cost_distribution_by_brand
- For treemap → use fleet_cost_treemap or failure_count_by_component
- For waterfall → use cost_waterfall_by_category or total_cost_by_brand
- For histogram → use downtime_histogram

UI ACTIONS:
- add_chart: show a metric visualisation on the canvas
- modify_chart: change chart_type or filters of an existing card (requires card_id)
- add_filter: apply a dimension filter to selected cards
- reset_filters: clear all filters

BOARD AWARENESS RULES:
- You will receive the current board state in each message under BOARD_CONTEXT
- Use it to answer questions like "how many charts do I have?", "what is chart X showing?", "list my charts"
- When user says "this chart", "the selected chart", or "change it" — use the selected card from BOARD_CONTEXT
- When asked to explain a chart: describe what the metric measures, what the data shows, and include the SQL used
- When asked for the code: provide a clean Python + Plotly snippet the user could run independently

MODIFY CHART RULES:
- If user says "change this to X chart" and a card is selected → emit modify_chart action with that card's id and new chart_type
- Confirm in narrative: "I've changed [chart title] to a [chart_type] chart."
- If chart_type is not in available_charts for that metric → use closest valid type and inform user

RESPONSE — return ONLY valid JSON, no markdown, no code blocks:
{{
  "narrative": "Your friendly reply. 1-3 sentences. Plain English only — no JSON, no curly braces.",
  "ui_actions": [],
  "fallback_sql": null
}}

INTENT → METRIC MAPPING (always follow these):
- "which components fail / fail most / failure by component" → metric: failure_count_by_component
- "failure by brand / which brand fails" → metric: failure_count_by_brand
- "failure trend / quarterly failures" → metric: failure_trend_by_quarter
- "cost by brand / total cost brand" → metric: total_cost_by_brand
- "cost by workshop" → metric: total_cost_by_workshop
- "cost trend / monthly cost" → metric: cost_trend_by_month
- "cost by component / component cost" → metric: total_cost_by_component_category
- "downtime by brand" → metric: downtime_by_brand
- "downtime by component" → metric: downtime_by_component_category
- "scheduled vs unscheduled / maintenance ratio" → metric: scheduled_vs_unscheduled
- "YoY / year over year cost" → metric: yoy_cost_comparison
- "last 12 months / rolling 12" → metric: last_12_months_trend
- "heatmap / brand month grid" → metric: cost_heatmap_brand_month
- "components within cost / cost breakdown by component / stacked brand component / what makes up cost / show the breakdown / what is inside this cost / show components" → metric: cost_by_brand_and_component, chart_type: stacked_bar
- "downtime breakdown by component / downtime stacked" → metric: downtime_by_brand_and_component, chart_type: stacked_bar

CRITICAL STACKED BAR RULE:
- When user asks to see "what is inside" or "breakdown of components" for a cost chart → ALWAYS emit add_chart with metric=cost_by_brand_and_component and chart_type=stacked_bar. NEVER use treemap for this. NEVER use modify_chart for this (it is a different metric, so it must be a NEW card).
- Do NOT mention treemap in your narrative unless the user explicitly asked for a treemap.
- The narrative for stacked bar should say: "I've added a stacked bar chart showing the cost breakdown by component for each brand."
- "scatter / cost vs downtime / correlation" → metric: cost_vs_downtime_scatter
- "boxplot / distribution / cost spread" → metric: cost_distribution_by_brand
- "waterfall / cost buildup / cumulative cost" → metric: cost_waterfall_by_category
- "treemap / hierarchical / fleet cost tree" → metric: fleet_cost_treemap
- "histogram / downtime distribution" → metric: downtime_histogram

CRITICAL MODIFY vs ADD RULES:
- If SELECTED CARD exists in BOARD_CONTEXT AND user says "change", "switch", "modify", "convert", "make it", "turn into" → ALWAYS emit modify_chart (NEVER add_chart)
- modify_chart requires: card_id = the selected card's id, plus chart_type and/or filters to change
- If user asks to ADD information to an existing chart (e.g. "include X in this chart") → explain you cannot merge metrics, but offer to add a separate companion chart alongside
- NEVER emit add_chart when a card is selected and user is asking to modify it

RULES:
1. ALWAYS add a chart immediately for any data question (unless modifying a selected card).
2. The narrative must be plain English only — no metric_id, no JSON, no code.
3. For time-aware queries add time_shortcut to the filters.
4. Multiple charts fine — "compare X and Y" → two add_chart actions.
5. If no metric matches, set fallback_sql to a DuckDB SQL query against v_maintenance_full.
6. When emitting modify_chart, confirm in narrative: "I've changed [title] to a [type] chart."
7. When asked to merge metrics into one chart → explain it's not possible, offer companion chart instead.
"""


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


async def call_groq(messages: list[dict]) -> str:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROQ_MODEL, "messages": messages, "temperature": 0.1, "max_tokens": 1500},
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


def build_board_context_prompt(board_context: BoardContext | None) -> str:
    if not board_context or not board_context.charts_on_canvas:
        return "\nBOARD_CONTEXT: Canvas is empty — no charts yet.\n"

    lines = [f"\nBOARD_CONTEXT: {len(board_context.charts_on_canvas)} chart(s) on canvas:"]
    for c in board_context.charts_on_canvas:
        metric = METRICS.get(c.metric_id, {})
        selected_marker = " ← SELECTED" if c.id in (board_context.selected_ids or []) else ""
        filters_str = json.dumps(c.filters) if c.filters else "none"
        lines.append(
            f"  - id={c.id} | title='{c.title}' | metric={c.metric_id} "
            f"| chart_type={c.chart_type} | filters={filters_str}{selected_marker}"
        )
        if metric:
            lines.append(f"    description: {metric.get('description','')}")
            lines.append(f"    available_charts: {metric.get('available_charts', [])}")

    selected = [c for c in board_context.charts_on_canvas if c.id in (board_context.selected_ids or [])]
    if selected:
        s = selected[0]
        lines.append(f"\nSELECTED CARD: id={s.id}, title='{s.title}', metric={s.metric_id}, chart_type={s.chart_type}")
    else:
        lines.append("\nSELECTED CARD: none")

    return "\n".join(lines) + "\n"


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


def execute_modify_action(action: dict, board_context: BoardContext | None) -> dict:
    """Validate modify_chart — ensure card_id exists and chart_type is valid."""
    card_id = action.get("card_id")
    new_chart_type = action.get("chart_type")
    new_filters = action.get("filters")

    if not card_id and board_context and board_context.selected_ids:
        card_id = board_context.selected_ids[0]
        action["card_id"] = card_id

    # Validate chart type against metric
    if card_id and board_context:
        card = next((c for c in board_context.charts_on_canvas if c.id == card_id), None)
        if card and new_chart_type:
            metric = METRICS.get(card.metric_id, {})
            available = metric.get("available_charts", [])
            if new_chart_type not in available and available:
                action["chart_type"] = available[0]
                action["fallback_note"] = f"{new_chart_type} not available, using {available[0]}"

    return action


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
    board_prompt = build_board_context_prompt(req.board_context)

    # Inject board context into system prompt dynamically
    dynamic_system = SYSTEM_PROMPT + board_prompt

    messages = [{"role": "system", "content": dynamic_system}]
    for msg in (req.history or []):
        messages.append({
            "role": msg.role,
            "content": msg.content if isinstance(msg.content, str) else json.dumps(msg.content)
        })
    messages.append({"role": "user", "content": req.message})

    try:
        raw = await call_groq(messages)
        clean = raw.strip()
        if clean.startswith("```"):
            lines = clean.split("\n")
            clean = "\n".join(lines[1:])
            if clean.endswith("```"):
                clean = clean[:-3]
        parsed = json.loads(clean.strip())
    except json.JSONDecodeError:
        parsed = {"narrative": raw.strip(), "ui_actions": [], "fallback_sql": None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Sanitise narrative
    narrative = parsed.get("narrative", "")
    if "{" in narrative and "}" in narrative:
        narrative = narrative.split("{")[0].strip()
        if not narrative:
            narrative = "Here is the data you requested."

    enriched_actions = []
    for action in parsed.get("ui_actions", []):
        if action.get("action") == "add_chart":
            result = execute_chart_action(action)
            enriched_actions.append(result or action)
        elif action.get("action") == "modify_chart":
            enriched_actions.append(execute_modify_action(action, req.board_context))
        else:
            enriched_actions.append(action)

    response = {
        "narrative":    narrative,
        "ui_actions":   enriched_actions,
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
