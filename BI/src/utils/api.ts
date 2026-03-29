import axios from "axios"

const BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

// Standard client — 60s timeout for AI chat and chart operations
const API = axios.create({
  baseURL: BASE_URL,
  timeout: 60000,
})

// Long-timeout client for board/message load on cold start
// Render free tier can take 50-80s to wake — needs extra headroom
const API_SLOW = axios.create({
  baseURL: BASE_URL,
  timeout: 120000,
})

// Path B: run arbitrary SQL
export const runSQL = (sql: string, chart_type?: string, title?: string, category?: string) =>
  API.post("/query/", { sql, chart_type, title, category }).then(r => r.data)

export const runMetric = (metric_id: string, chart_type?: string, filters?: Record<string, any>) =>
  API.post("/query/", { sql: `SELECT '${metric_id}' AS metric_removed`, chart_type }).then(r => r.data)

export const sendChat = (
  session_id: string,
  message: string,
  history: any[],
  board_context?: {
    charts_on_canvas: Array<{
      id: string; title: string; metric_id: string
      chart_type: string; filters: Record<string, any>; selected: boolean
      sql?: string; base_sql?: string
    }>
    selected_ids: string[]
  }
) =>
  API.post("/chat/", { session_id, message, history, board_context }).then(r => r.data)

export const getFilters = () =>
  API.get("/filters/").then(r => r.data)

export const getChatHistory = (session_id: string) =>
  API_SLOW.get(`/chat/history/${session_id}`).then(r => r.data)

export const clearChatHistory = (session_id: string) =>
  API.delete(`/chat/history/${session_id}`).then(r => r.data)

export const listMetrics = () =>
  API.get("/query/metrics").then(r => r.data)

// ── Wake-up ping — call before board/message load to warm Render ──────────────
export const pingBackend = () =>
  API_SLOW.get("/").catch(() => {})  // fire and forget, ignore errors

// ── Board persistence ──────────────────────────────────────────────────────────

export const saveBoard = (board_state: any[], user_id: string = "default") =>
  API.post("/chat/board/save", { user_id, board_state }).then(r => r.data)

export const loadBoard = (user_id: string = "default") =>
  API_SLOW.get(`/chat/board/${user_id}`).then(r => r.data)

export const rerenderChart = (
  sql: string,
  chart_type: string,
  title: string,
  category: string,
  filters?: Record<string, any>
) =>
  API.post("/chat/rerender", { sql, chart_type, title, category, filters }).then(r => r.data)

export const deriveSql = (sql: string) =>
  API.post("/chat/derive_source_sql", { sql }).then(r => r.data)

export const getDQWarnings = () =>
  API.get("/chat/data/warnings").then(r => r.data)

export const reloadData = () =>
  API.post("/chat/data/reload").then(r => r.data)

export const toggleFormat = (params: {
  long_sql: string
  wide_sql: string
  is_wide: boolean
  chart_type: string
  title: string
  category: string
  filters?: Record<string, any>
  pivot_col?: string
}) =>
  API.post("/chat/toggle_format", params).then(r => r.data)
