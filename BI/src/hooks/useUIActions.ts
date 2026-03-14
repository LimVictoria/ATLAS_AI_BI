import { useDashboardStore } from "@/store/dashboard"
import { v4 as uuid } from "uuid"

export async function processUIActions(actions: any[]) {
  const { addChart, updateChart, charts } = useDashboardStore.getState()

  for (const action of actions) {

    // ── Add new chart to canvas ───────────────────────────────────────────
    if (action.action === "add_chart") {
      const raw = action.chart_data
      if (!raw) {
        console.error("[useUIActions] add_chart action missing chart_data:", action)
        continue
      }
      // Support both old format (raw.chart = plotly json) and new Path B format (raw = full result)
      const chartData = raw.chart !== undefined ? raw.chart : raw
      addChart({
        id: uuid(),
        metric_id: action.metric_id || raw.metric_id || "dynamic",
        title: action.title || raw.title || action.metric_id || "Query Result",
        category: raw.category || action.category || "General",
        chart_type: raw.chart_type || action.chart_type || "bar",
        chart_data: raw.chart !== undefined ? raw.chart : raw,
        filters: action.filters || {},
        available_charts: raw.available_charts || ["bar", "line", "table"],
        sql: raw.sql || action.sql || "",
        base_sql: raw.sql || action.sql || "",
        filter_suggestions: action.filter_suggestions || [],
        selected: false,
        loading: false,
        x: 0, y: 0, w: 6, h: 6,
      })
    }

    // ── Modify existing card (chart type or filters) ───────────────────────
    if (action.action === "modify_chart") {
      const { charts: currentCharts } = useDashboardStore.getState()
      const cardId = action.card_id
      if (!cardId) continue

      const card = currentCharts.find(c => c.id === cardId)
      if (!card) continue

      if (action.chart_type && action.chart_type !== card.chart_type) {
        updateChart(cardId, { loading: true })
        try {
          // Path B: card has SQL stored — re-render via /chat/rerender
          // Path A: card has metric_id — use /query/
          if (card.sql) {
            const { rerenderChart } = await import("@/utils/api")
            const result = await rerenderChart(card.sql, action.chart_type, card.title, card.category)
            updateChart(cardId, {
              chart_type: action.chart_type,
              chart_data: result.chart,
              available_charts: result.available_charts || card.available_charts,
              loading: false,
            })
          } else {
            // No SQL stored — card is from old session, cannot re-render
            console.warn("[useUIActions] card has no sql, cannot switch chart type:", card.metric_id)
            updateChart(cardId, { loading: false })
          }
        } catch {
          updateChart(cardId, { loading: false })
        }
      }

      if (action.filters) {
        const newFilters = { ...card.filters, ...action.filters }
        updateChart(cardId, { loading: true, filters: newFilters })
        try {
          if (card.sql) {
            const { rerenderChart } = await import("@/utils/api")
            const result = await rerenderChart(card.sql, card.chart_type, card.title, card.category, newFilters)
            updateChart(cardId, { chart_data: result.chart, filters: newFilters, loading: false })
          } else {
            console.warn("[useUIActions] card has no sql, cannot apply filter:", card.metric_id)
            updateChart(cardId, { loading: false })
          }
        } catch {
          updateChart(cardId, { loading: false })
        }
      }
    }

    // ── Apply filter to selected cards (or all if none selected) ──────────
    if (action.action === "add_filter") {
      const { charts: currentCharts } = useDashboardStore.getState()
      const targets = currentCharts.filter(c => c.selected)
      const applyTo = targets.length > 0 ? targets : currentCharts

      for (const card of applyTo) {
        const newFilters = { ...card.filters, [action.dimension]: action.value }
        const { runMetric } = await import("@/utils/api")
        updateChart(card.id, { loading: true, filters: newFilters })
        try {
          const result = await runMetric(card.metric_id, card.chart_type, newFilters)
          updateChart(card.id, { chart_data: result.chart, loading: false })
        } catch {
          updateChart(card.id, { loading: false })
        }
      }
    }

    // ── Reset all filters ─────────────────────────────────────────────────
    if (action.action === "reset_filters") {
      const { charts: currentCharts } = useDashboardStore.getState()
      for (const card of currentCharts) {
        updateChart(card.id, { filters: {} })
      }
    }
  }
}
