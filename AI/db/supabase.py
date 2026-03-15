"""
ATLAS BI — Supabase client + persistence helpers
"""
import os
import json
from datetime import datetime
from supabase import create_client, Client

_client: Client | None = None
DEFAULT_USER = "default"


def get_supabase() -> Client:
    global _client
    if _client is None:
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY", ""))
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set")
        _client = create_client(url, key)
    return _client


# ── Board persistence ──────────────────────────────────────────────────────────

def save_board(board_state: list, user_id: str = DEFAULT_USER) -> bool:
    try:
        get_supabase().table("bi_board").upsert({
            "user_id":     user_id,
            "board_state": board_state,
            "updated_at":  datetime.utcnow().isoformat(),
        }, on_conflict="user_id").execute()
        return True
    except Exception as e:
        print(f"[Supabase] save_board failed: {e}")
        return False


def load_board(user_id: str = DEFAULT_USER) -> list:
    try:
        resp = get_supabase().table("bi_board") \
            .select("board_state") \
            .eq("user_id", user_id) \
            .limit(1).execute()
        if resp.data:
            return resp.data[0]["board_state"] or []
    except Exception as e:
        print(f"[Supabase] load_board failed: {e}")
    return []


# ── Chat history persistence ───────────────────────────────────────────────────

def save_message(user_id: str, role: str, content: str, ui_actions: list = None) -> None:
    try:
        get_supabase().table("bi_chat_history").insert({
            "user_id":    user_id,
            "role":       role,
            "content":    content,
            "ui_actions": ui_actions or [],
            "created_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        print(f"[Supabase] save_message failed: {e}")


def load_messages(user_id: str = DEFAULT_USER, limit: int = 40) -> list[dict]:
    try:
        resp = get_supabase().table("bi_chat_history") \
            .select("role,content,ui_actions,created_at") \
            .eq("user_id", user_id) \
            .order("created_at") \
            .limit(limit).execute()
        return resp.data or []
    except Exception as e:
        print(f"[Supabase] load_messages failed: {e}")
        return []


def clear_messages(user_id: str = DEFAULT_USER) -> bool:
    try:
        get_supabase().table("bi_chat_history") \
            .delete().eq("user_id", user_id).execute()
        return True
    except Exception as e:
        print(f"[Supabase] clear_messages failed: {e}")
        return False


# ── User memory persistence ────────────────────────────────────────────────────

def save_memory(user_id: str, memory: dict) -> None:
    """Save extracted user preferences to bi_user_memory table.
    Silently skips if table does not exist yet."""
    try:
        get_supabase().table("bi_user_memory").upsert({
            "user_id":    user_id,
            "memory":     memory,
            "updated_at": datetime.utcnow().isoformat(),
        }, on_conflict="user_id").execute()
    except Exception as e:
        # Table may not exist yet — non-fatal, memory just won't persist
        print(f"[Supabase] save_memory skipped: {e}")


def load_memory(user_id: str = DEFAULT_USER) -> dict:
    """Load user preferences from bi_user_memory table.
    Returns empty dict if table does not exist yet."""
    try:
        resp = get_supabase().table("bi_user_memory") \
            .select("memory") \
            .eq("user_id", user_id) \
            .limit(1).execute()
        if resp.data:
            return resp.data[0]["memory"] or {}
    except Exception as e:
        print(f"[Supabase] load_memory skipped: {e}")
    return {}
