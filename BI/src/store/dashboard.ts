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
  selected: boolean
  loading: boolean
  x: number
  y: number
  w: number
  h: number
}

export interface ChatMessage {
  id: string
  role: "user" | "assistant"
  text: string
  timestamp: Date
  loading?: boolean
}

interface DashboardStore {
  sessionId: string | null
  setSessionId: (id: string) => void
  userId: string
  boardLoaded: boolean
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

// Serialise chart for Supabase (strip non-serialisable bits)
const serialiseChart = (c: ChartCard) => ({
  id: c.id,
  metric_id: c.metric_id,
  title: c.title,
  category: c.category,
  chart_type: c.chart_type,
  chart_data: c.chart_data,
  filters: c.filters,
  available_charts: c.available_charts,
  selected: false,  // never persist selection state
  loading: false,
  x: c.x, y: c.y, w: c.w, h: c.h,
})

const persistBoard = (charts: ChartCard[], userId: string) => {
  const serialised = charts.map(serialiseChart)
  saveBoard(serialised, userId).catch(e => console.warn("[Board] save failed:", e))
}

export const useDashboardStore = create<DashboardStore>((set, get) => ({
  sessionId: null,
  setSessionId: (id) => set({ sessionId: id }),
  userId: "default",
  boardLoaded: false,

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
    // Only persist meaningful updates (not loading state flickers)
    if (!("loading" in patch)) {
      persistBoard(newCharts, get().userId)
    }
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
    try {
      const resp = await loadBoard(get().userId)
      const saved: ChartCard[] = resp.board_state || []
      if (saved.length > 0) {
        set({ charts: saved, boardLoaded: true })
        _cardCounter = saved.length
      } else {
        set({ boardLoaded: true })
      }
    } catch (e) {
      console.warn("[Board] load failed:", e)
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
    try {
      const { getChatHistory } = await import("@/utils/api")
      const resp = await getChatHistory(get().userId)
      const msgs = (resp.messages || []).map((m: any, i: number) => ({
        id: `hist-${i}`,
        role: m.role as "user" | "assistant",
        text: m.content || "",
        timestamp: new Date(m.created_at || Date.now()),
        loading: false,
      }))
      if (msgs.length > 0) {
        set({ messages: msgs })
      }
    } catch (e) {
      console.warn("[Messages] load failed:", e)
    }
  },
}))
