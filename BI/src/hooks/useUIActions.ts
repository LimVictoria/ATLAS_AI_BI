import { useDashboardStore } from "@/store/dashboard"
import { v4 as uuid } from "uuid"

export async function processUIActions(actions: any[]) {
  const { addChart, updateChart, charts } = useDashboardStore.getState()

  for (const action of actions) {
    if (action.action === "add_chart") {
      const raw = action.chart_data
      if (!raw) continue
      addChart({
        id: uuid(),
        metric_id: action.metric_id,
        title: action.title || raw.title || action.metric_id,
        category: raw.category || "General",
        chart_type: raw.chart_type || action.chart_type || "bar",
        chart_data: raw.chart,
        filters: action.filters || {},
        available_charts: raw.available_charts || ["bar", "line", "table"],
        selected: false,
        loading: false,
        x: 0, y: 0, w: 6, h: 6,
      })
    }

    // Apply filter to selected cards, or all cards if none selected
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

    if (action.action === "reset_filters") {
      const { charts: currentCharts } = useDashboardStore.getState()
      for (const card of currentCharts) {
        updateChart(card.id, { filters: {} })
      }
    }
  }
}
