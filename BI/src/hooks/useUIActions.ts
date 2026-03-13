import { useDashboardStore } from "@/store/dashboard"
import { runMetric } from "@/utils/api"
import { v4 as uuid } from "uuid"

export async function processUIActions(actions: any[]) {
  const { addChart } = useDashboardStore.getState()

  for (const action of actions) {
    if (action.action === "add_chart") {
      const chartData = action.chart_data
      if (!chartData) continue

      // chart_data comes back as the full run_metric response
      const raw = chartData
      addChart({
        id: uuid(),
        metric_id: action.metric_id,
        title: action.title || raw.title || action.metric_id,
        category: raw.category || "General",
        chart_type: raw.chart_type || action.chart_type || "bar",
        chart_data: raw.chart,        // the Plotly JSON string
        filters: action.filters || {},
        available_charts: raw.available_charts || ["bar", "line", "table"],
        selected: false,
        loading: false,
        x: 0, y: 0, w: 6, h: 6,
      })
    }
  }
}
