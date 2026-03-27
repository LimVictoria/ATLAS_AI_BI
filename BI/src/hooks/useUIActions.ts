import { useDashboardStore } from "@/store/dashboard"
import { v4 as uuid } from "uuid"

export async function processUIActions(actions: any[]) {
  const { addChart, updateChart, charts } = useDashboardStore.getState()

  for (const action of actions) {

    // ── Add new chart to canvas ─────────────────────────────────────────────
    if (action.action === "add_chart") {
      const raw = action.chart_data
      if (!raw) {
        console.error("[useUIActions] add_chart missing chart_data:", action)
        continue
      }
      addChart({
        id: uuid(),
        metric_id: action.metric_id || raw.metric_id || "dynamic",
        title: action.title || raw.title || "Query Result",
        category: raw.category || action.category || "General",
        chart_type: raw.chart_type || action.chart_type || "bar",
        chart_data: raw.chart !== undefined ? raw.chart : raw,
        filters: action.filters || {},
        available_charts: raw.available_charts || ["bar", "line", "table"],
        sql: raw.sql || action.sql || "",
        base_sql: raw.sql || action.sql || "",
        long_sql: action.long_sql || raw.sql || action.sql || "",
        wide_sql: action.wide_sql || "",
        is_wide: action.is_wide || false,
        pivot_col: action.pivot_col || "",
        filter_suggestions: action.filter_suggestions || [],
        selected: false,
        loading: false,
        x: 0, y: 0, w: 6, h: 6,
      })
      continue
    }

    // ── Replace existing card (SQL modification from board_node) ────────────
    if (action.action === "replace_chart") {
      const raw = action.chart_data
      if (!raw || !action.card_id) continue
      updateChart(action.card_id, {
        title: action.title || raw.title,
        chart_type: (raw.chart_type || action.chart_type || "table") as any,
        chart_data: raw.chart !== undefined ? raw.chart : raw,
        sql: raw.sql || action.sql || "",
        base_sql: raw.sql || action.sql || "",
        available_charts: raw.available_charts || ["bar", "table"],
        filter_suggestions: action.filter_suggestions || [],
        filters: {},
        loading: false,
      })
      continue
    }

    // ── modify_chart / apply_filter — AI asked to change selected card ──────
    // board_node now re-routes these through sql_node so they arrive as add_chart.
    // This handler is a fallback in case old action format arrives.
    // Always adds a NEW card — never modifies existing card in-place.
    if (action.action === "modify_chart" || action.action === "apply_filter") {
      const { charts: currentCharts } = useDashboardStore.getState()
      const card = currentCharts.find(c => c.id === action.card_id)
      if (!card || !card.sql) {
        console.warn("[useUIActions] fallback modify: card not found or no sql", action)
        continue
      }
      try {
        const { rerenderChart } = await import("@/utils/api")
        const newFilters = action.action === "apply_filter" && action.dim
          ? {
              ...card.filters,
              [action.dim]: action.values?.length === 1
                ? action.values[0]
                : (action.values || []),
            }
          : (card.filters || {})
        const newType = action.chart_type || card.chart_type
        const result = await rerenderChart(
          card.base_sql || card.sql,
          newType,
          card.title,
          card.category,
          newFilters
        )
        const filterLabel = action.action === "apply_filter" && action.dim
          ? ` · ${action.dim}=${action.values?.join(",")}`
          : " (modified)"
        // Add NEW card alongside — user can delete the old one
        addChart({
          id: uuid(),
          metric_id: card.metric_id,
          title: card.title + filterLabel,
          category: card.category,
          chart_type: newType as any,
          chart_data: result.chart,
          filters: newFilters,
          available_charts: result.available_charts || card.available_charts,
          sql: result.sql || card.sql,
          base_sql: card.base_sql || card.sql,
          long_sql: card.long_sql || "",
          wide_sql: card.wide_sql || "",
          is_wide: false,
          pivot_col: card.pivot_col || "",
          filter_suggestions: [],
          selected: false,
          loading: false,
          x: 0, y: 0, w: 6, h: 6,
        })
      } catch (e) {
        console.error("[useUIActions] modify fallback failed:", e)
      }
      continue
    }

    // ── Toggle wide/long format ─────────────────────────────────────────────
    if (action.action === "toggle_format") {
      const card = charts.find(c => c.id === action.card_id)
      if (!card) continue
      const newIsWide = !card.is_wide
      updateChart(action.card_id, { loading: true })
      try {
        const { toggleFormat } = await import("@/utils/api")
        const result = await toggleFormat({
          long_sql: card.long_sql || card.base_sql || card.sql || "",
          wide_sql: card.wide_sql || "",
          is_wide: newIsWide,
          chart_type: card.chart_type,
          title: card.title,
          category: card.category,
          filters: card.filters || {},
          pivot_col: card.pivot_col || "",
        })
        updateChart(action.card_id, {
          chart_data: result.chart,
          chart_type: card.chart_type,  // never change chart type on format toggle
          sql: result.sql,
          is_wide: newIsWide,
          available_charts: card.available_charts,
          loading: false,
        })
      } catch {
        updateChart(action.card_id, { loading: false })
      }
      continue
    }

    // ── Show filter panel on a card ─────────────────────────────────────────
    if (action.action === "show_filter") {
      if (action.card_id) updateChart(action.card_id, { showFilters: true })
      continue
    }

    // ── Apply filter to selected/all cards (global filter broadcast) ────────
    if (action.action === "add_filter") {
      const { charts: currentCharts } = useDashboardStore.getState()
      const targets = currentCharts.filter(c => c.selected)
      const applyTo = targets.length > 0 ? targets : currentCharts
      for (const card of applyTo) {
        const newFilters = { ...card.filters, [action.dimension]: action.value }
        updateChart(card.id, { loading: true, filters: newFilters })
        try {
          if (card.sql) {
            const { rerenderChart } = await import("@/utils/api")
            const result = await rerenderChart(
              card.base_sql || card.sql,
              card.chart_type,
              card.title,
              card.category,
              newFilters
            )
            updateChart(card.id, { chart_data: result.chart, filters: newFilters, loading: false })
          } else {
            updateChart(card.id, { filters: newFilters, loading: false })
          }
        } catch {
          updateChart(card.id, { loading: false })
        }
      }
      continue
    }

    // ── Reset all filters ───────────────────────────────────────────────────
    if (action.action === "reset_filters") {
      const { charts: currentCharts } = useDashboardStore.getState()
      for (const card of currentCharts) {
        updateChart(card.id, { filters: {} })
      }
      continue
    }
  }
}
