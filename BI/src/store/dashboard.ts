import { create } from "zustand"

export type ChartType = "bar" | "line" | "pie" | "table" | "pareto"

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
  charts: ChartCard[]
  addChart:       (card: ChartCard) => void
  removeChart:    (id: string) => void
  updateChart:    (id: string, patch: Partial<ChartCard>) => void
  toggleSelect:   (id: string, multi: boolean) => void
  clearSelection: () => void
  selectedCharts: () => ChartCard[]
  messages: ChatMessage[]
  addMessage:    (msg: ChatMessage) => void
  updateMessage: (id: string, patch: Partial<ChatMessage>) => void
  clearMessages: () => void
}

let _cardCounter = 0
const nextPos = () => {
  const col = _cardCounter % 2
  const row = Math.floor(_cardCounter / 2)
  _cardCounter++
  return { x: col * 6, y: row * 6, w: 6, h: 6 }
}

export const useDashboardStore = create<DashboardStore>((set, get) => ({
  sessionId: null,
  setSessionId: (id) => set({ sessionId: id }),

  charts: [],
  addChart: (card) => {
    const pos = nextPos()
    set((s) => ({ charts: [...s.charts, { ...card, ...pos }] }))
  },
  removeChart: (id) => set((s) => ({ charts: s.charts.filter(c => c.id !== id) })),
  updateChart: (id, patch) => set((s) => ({
    charts: s.charts.map(c => c.id === id ? { ...c, ...patch } : c)
  })),
  toggleSelect: (id, multi) => set((s) => ({
    charts: s.charts.map(c => {
      if (c.id === id) return { ...c, selected: !c.selected }
      if (!multi) return { ...c, selected: false }
      return c
    })
  })),
  clearSelection: () => set((s) => ({ charts: s.charts.map(c => ({ ...c, selected: false })) })),
  selectedCharts: () => get().charts.filter(c => c.selected),

  messages: [],
  addMessage: (msg) => set((s) => ({ messages: [...s.messages, msg] })),
  updateMessage: (id, patch) => set((s) => ({
    messages: s.messages.map(m => m.id === id ? { ...m, ...patch } : m)
  })),
  clearMessages: () => set({ messages: [] }),
}))
