"""
ATLAS BI — FastAPI Backend
"""
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from api.query import router as query_router
from api.filters import router as filters_router
from api.chat import router as chat_router
from api.metrics import router as metrics_router

load_dotenv()

app = FastAPI(title="ATLAS BI API", version="2.0.0")

# ── CORS ──────────────────────────────────────────────────────────────────────
# Allow all origins robustly — works even when the app throws an unhandled error
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Fallback middleware — log ALL errors and ensure CORS headers
@app.middleware("http")
async def add_cors_fallback(request: Request, call_next):
    import traceback as tb
    try:
        response = await call_next(request)
        origin = request.headers.get("origin", "*")
        response.headers["Access-Control-Allow-Origin"] = origin or "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response
    except Exception as e:
        err = tb.format_exc()
        print(f"[middleware] UNHANDLED ERROR on {request.method} {request.url.path}:\n{err}")
        origin = request.headers.get("origin", "*")
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)},
            headers={
                "Access-Control-Allow-Origin": origin or "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
                "Access-Control-Allow-Headers": "*",
            }
        )

app.include_router(query_router)
app.include_router(filters_router)
app.include_router(chat_router)
app.include_router(metrics_router)


@app.on_event("startup")
async def startup_test():
    """Run a test invocation on startup to catch errors early."""
    import traceback
    print("[startup] Testing graph initialization...")
    try:
        from agent.nodes import get_graph, AgentState
        graph = get_graph()
        print("[startup] Graph built OK")
        # Test a minimal invocation
        test_state: AgentState = {
            "user_message": "test",
            "history": [],
            "board_context": "",
            "user_memory": "{}",
            "intent": "",
            "selected_card_id": None,
            "sql": "",
            "sql_error": "",
            "sql_retries": 0,
            "df_rows": [],
            "df_columns": [],
            "chart_json": "",
            "chart_type": "",
            "available_charts": [],
            "chart_title": "",
            "chart_category": "General",
            "narrative": "",
            "ui_actions": [],
        }
        result = await graph.ainvoke(test_state)
        print(f"[startup] Test invocation OK — narrative: {result.get('narrative','')[:80]}")
    except Exception as e:
        print(f"[startup] STARTUP TEST FAILED:\n{traceback.format_exc()}")


@app.get("/")
def root():
    return {"status": "ok", "service": "ATLAS BI API", "version": "2.0.0"}


@app.get("/health")
def health():
    return {"status": "healthy"}
