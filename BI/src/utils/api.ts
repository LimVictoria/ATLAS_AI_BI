import axios from "axios"

const API = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
  timeout: 60000,
})

export const runMetric = (metric_id: string, chart_type?: string, filters?: Record<string, any>) =>
  API.post("/query/", { metric_id, chart_type, filters }).then(r => r.data)

export const sendChat = (
  session_id: string,
  message: string,
  history: any[],
  board_context?: {
    charts_on_canvas: Array<{
      id: string; title: string; metric_id: string
      chart_type: string; filters: Record<string, any>; selected: boolean
    }>
    selected_ids: string[]
  }
) =>
  API.post("/chat/", { session_id, message, history, board_context }).then(r => r.data)

export const getFilters = () =>
  API.get("/filters/").then(r => r.data)

export const getChatHistory = (session_id: string) =>
  API.get(`/chat/history/${session_id}`).then(r => r.data)

export const clearChatHistory = (session_id: string) =>
  API.delete(`/chat/history/${session_id}`).then(r => r.data)

export const listMetrics = () =>
  API.get("/query/metrics").then(r => r.data)
