import { create } from "zustand"
import { saveBoard, loadBoard } from "@/utils/api"

export type ChartType = "bar" | "line" | "pie" | "table" | "pareto" | "waterfall" | "heatmap" | "boxplot" | "scatter" | "treemap" | "histogram" | "stacked_bar"

export interface ChartCard {
  id: string
  metric_id: string
  title: string
  category: string
  chart_type: ChartType
  chart_data: any
  filters: Record<string, any>
  available_charts: ChartType[]
  sql?: string
  base_sql?: string
  long_sql?: string
  wide_sql?: string
  is_wide?: boolean
  pivot_col?: string
  filter_suggestions?: Array<{dim: string; value: string; label: string}>
  showFilters?: boolean
  selected: boolean
  loading: boolean
  x: number
  y: number
  w: number
  h: number
}

export interface ChatMessage {
  id: string
  role: "user" | "assistant" | "system"
  text: string
  timestamp: Date
  loading?: boolean
}

interface DashboardStore {
  sessionId: string | null
  setSessionId: (id: string) => void
  userId: string
  boardLoaded: boolean
  messagesLoaded: boolean
  charts: ChartCard[]
  addChart:       (card: ChartCard) => void
  removeChart:    (id: string) => void
  updateChart:    (id: string, patch: Partial<ChartCard>) => void
  toggleSelect:   (id: string, multi: boolean) => void
  clearSelection: () => void
  selectedCharts: () => ChartCard[]
  loadBoardFromServer: () => Promise<void>
  messages: ChatMessage[]
  addMessage:    (msg: ChatMessage) => void
  updateMessage: (id: string, patch: Partial<ChatMessage>) => void
  clearMessages: () => void
  loadMessagesFromServer: () => Promise<void>
}

let _cardCounter = 0
const nextPos = (existingCount = 0) => {
  const col = existingCount % 2
  const row = Math.floor(existingCount / 2)
  return { x: col * 6, y: row * 6, w: 6, h: 6 }
}

// Keys that change too frequently or are too large to persist on every update
const SKIP_PERSIST_KEYS = new Set(["loading", "chart_data", "showFilters", "selected"])

const serialiseChart = (c: ChartCard) => {
  // long_sql is always the canonical long-format SQL — use it as the restore source
  // base_sql and sql may be wide pivot format if user had pivoted the card
  const canonicalSql = c.long_sql || c.base_sql || c.sql || ""
  return {
    id: c.id,
    metric_id: c.metric_id,
    title: c.title,
    category: c.category,
    chart_type: c.chart_type,
    chart_data: null,              // always strip — too large for Supabase, regenerated from SQL on load
    filters: c.filters,
    available_charts: c.available_charts,
    sql: canonicalSql,             // always save long format as primary sql
    base_sql: canonicalSql,        // always save long format as base
    long_sql: canonicalSql,
    wide_sql: c.wide_sql || "",
    is_wide: false,                // always restore in long format — user can re-pivot
    pivot_col: c.pivot_col || "",
    filter_suggestions: c.filter_suggestions || [],
    selected: false,
    loading: false,
    x: c.x, y: c.y, w: c.w, h: c.h,
  }
}

// Debounce board saves — avoid hammering Supabase on rapid updates
let _saveTimer: ReturnType<typeof setTimeout> | null = null
const persistBoard = (charts: ChartCard[], userId: string) => {
  if (_saveTimer) clearTimeout(_saveTimer)
  _saveTimer = setTimeout(() => {
    const serialised = charts.map(serialiseChart)
    saveBoard(serialised, userId).catch(e => console.warn("[Board] save failed:", e))
  }, 1500)
}

// Retry helper — waits ms before resolving
const delay = (ms: number) => new Promise(r => setTimeout(r, ms))

export const useDashboardStore = create<DashboardStore>((set, get) => ({
  sessionId: null,
  setSessionId: (id) => set({ sessionId: id }),
  userId: "default",
  boardLoaded: false,
  messagesLoaded: false,
  charts: [],

  addChart: (card) => {
    const count = get().charts.length
    const pos = nextPos(count)
    const newCharts = [...get().charts, { ...card, ...pos }]
    set({ charts: newCharts })
    persistBoard(newCharts, get().userId)
  },

  removeChart: (id) => {
    const newCharts = get().charts.filter(c => c.id !== id)
    set({ charts: newCharts })
    persistBoard(newCharts, get().userId)
  },

  updateChart: (id, patch) => {
    const newCharts = get().charts.map(c => c.id === id ? { ...c, ...patch } : c)
    set({ charts: newCharts })
    const keys = Object.keys(patch)
    const shouldPersist = keys.some(k => !SKIP_PERSIST_KEYS.has(k))
    if (shouldPersist) persistBoard(newCharts, get().userId)
  },

  toggleSelect: (id, multi) => set((s) => ({
    charts: s.charts.map(c => {
      if (c.id === id) return { ...c, selected: !c.selected }
      if (!multi) return { ...c, selected: false }
      return c
    })
  })),

  clearSelection: () => set((s) => ({
    charts: s.charts.map(c => ({ ...c, selected: false }))
  })),

  selectedCharts: () => get().charts.filter(c => c.selected),

  loadBoardFromServer: async () => {
    if (get().boardLoaded) return

    // Ping backend to wake Render from sleep before loading
    const { pingBackend, rerenderChart } = await import("@/utils/api")
    pingBackend()

    // Retry up to 3 times — Render free tier cold start takes 50-80s
    let resp: any = null
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        resp = await loadBoard(get().userId)
        break
      } catch (e) {
        console.warn(`[Board] load attempt ${attempt}/3 failed:`, e)
        if (attempt < 3) await delay(5000 * attempt)  // 5s then 10s
      }
    }

    if (!resp) {
      console.warn("[Board] all load attempts failed — starting with empty board")
      set({ boardLoaded: true })
      return
    }

    try {
      const saved: ChartCard[] = (resp.board_state || []).map((c: any) => ({
        ...c,
        chart_data: null,      // stripped on save — regenerated below from SQL
        loading: !!c.sql,
        showFilters: true,
      }))

      // Only keep cards that have SQL — they can be re-rendered
      const valid = saved.filter(c => !!c.sql)
      set({ charts: valid, boardLoaded: true })
      _cardCounter = valid.length

      // Re-render each card from its stored SQL
      // Always use long_sql for rerender — base_sql/sql may be wide pivot format
      // which breaks chart rendering. long_sql is always the canonical long-format SQL.
      for (const card of valid) {
        try {
          const sqlToRender = card.long_sql || card.base_sql || card.sql || ""
          const result = await rerenderChart(
            sqlToRender,
            card.chart_type,
            card.title,
            card.category,
            card.filters || {}
          )
          get().updateChart(card.id, {
            chart_data: result.chart,
            chart_type: (result.chart_type || card.chart_type) as ChartType,
            available_charts: result.available_charts || card.available_charts,
            loading: false,
          })
        } catch {
          get().updateChart(card.id, { loading: false })
        }
      }
    } catch (e) {
      console.warn("[Board] parse/rerender error:", e)
      set({ boardLoaded: true })
    }
  },

  messages: [],
  addMessage: (msg) => set((s) => ({ messages: [...s.messages, msg] })),
  updateMessage: (id, patch) => set((s) => ({
    messages: s.messages.map(m => m.id === id ? { ...m, ...patch } : m)
  })),
  clearMessages: () => set({ messages: [] }),

  loadMessagesFromServer: async () => {
    if (get().messagesLoaded) return
    const { getChatHistory } = await import("@/utils/api")

    // Retry up to 3 times for cold start
    let resp: any = null
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        resp = await getChatHistory(get().userId)
        break
      } catch (e) {
        console.warn(`[Messages] load attempt ${attempt}/3 failed:`, e)
        if (attempt < 3) await delay(5000 * attempt)
      }
    }

    if (!resp) {
      set({ messagesLoaded: true })
      return
    }

    try {
      const msgs = (resp.messages || []).map((m: any, i: number) => ({
        id: `hist-${i}`,
        role: m.role as "user" | "assistant" | "system",
        text: m.content || "",
        // Store as ISO string to avoid server/client Date hydration mismatch
        timestamp: new Date(m.created_at || 0),
        loading: false,
      }))
      set({ messages: msgs.length > 0 ? msgs : get().messages, messagesLoaded: true })
    } catch (e) {
      console.warn("[Messages] parse error:", e)
      set({ messagesLoaded: true })
    }
  },
}))
