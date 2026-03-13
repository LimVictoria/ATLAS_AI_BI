"use client"
import { BarChart2 } from "lucide-react"
import { useDashboardStore } from "@/store/dashboard"
import ChartCard from "./ChartCard"
import { Responsive, WidthProvider } from "react-grid-layout"
import { useEffect, useState } from "react"

const ResponsiveGridLayout = WidthProvider(Responsive)

export default function BIPanel() {
  const { charts, clearSelection, updateChart } = useDashboardStore()
  const selected = charts.filter(c => c.selected)
  const [mounted, setMounted] = useState(false)

  useEffect(() => { setMounted(true) }, [])

  const layouts = {
    lg: charts.map((card) => ({
      i: card.id,
      x: card.x ?? 0,
      y: card.y ?? 0,
      w: card.w ?? 6,
      h: card.h ?? 6,
      minW: 3,
      minH: 4,
    }))
  }

  const onLayoutChange = (_: any, allLayouts: any) => {
    const lg = allLayouts.lg || []
    lg.forEach((item: any) => {
      updateChart(item.i, { x: item.x, y: item.y, w: item.w, h: item.h })
    })
  }

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", height: "100vh", overflow: "hidden", background: "#F4F6F9" }}>
      {/* Topbar */}
      <div style={{ height: "52px", display: "flex", alignItems: "center", padding: "0 18px", background: "#FFFFFF", borderBottom: "1px solid #E8ECF0", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#1E293B", letterSpacing: "-0.01em" }}>
          BI Canvas
        </span>
        {selected.length > 0 && (
          <span style={{ fontSize: 11, color: "#2563EB", marginLeft: 10, fontWeight: 500 }}>
            {selected.length} selected
          </span>
        )}
        <div style={{ marginLeft: "auto", display: "flex", gap: 10, alignItems: "center" }}>
          {selected.length > 0 && (
            <button onClick={clearSelection} style={{
              fontSize: 11, color: "#64748B", background: "none", border: "1px solid #E2E8F0",
              borderRadius: 6, padding: "4px 10px", cursor: "pointer",
            }}>
              Clear selection
            </button>
          )}
          {charts.length > 0 && (
            <span style={{
              fontSize: 11, color: "#64748B", background: "#F1F5F9",
              padding: "3px 10px", borderRadius: 99, fontWeight: 500,
            }}>
              {charts.length} chart{charts.length !== 1 ? "s" : ""}
            </span>
          )}
        </div>
      </div>

      {/* Canvas */}
      <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden", padding: "12px 16px" }}>
        {charts.length === 0 ? (
          <div className="canvas-empty">
            <BarChart2 size={36} style={{ color: "#94A3B8" }} />
            <p style={{ fontSize: 14, fontWeight: 600, color: "#64748B" }}>No charts yet</p>
            <p style={{ fontSize: 12, color: "#94A3B8" }}>Ask the AI to visualise a metric</p>
          </div>
        ) : mounted ? (
          <ResponsiveGridLayout
            className="layout"
            layouts={layouts}
            breakpoints={{ lg: 1200, md: 768, sm: 400 }}
            cols={{ lg: 12, md: 8, sm: 4 }}
            rowHeight={52}
            onLayoutChange={onLayoutChange}
            draggableHandle=".chart-drag-handle"
            margin={[14, 14]}
            containerPadding={[0, 4]}
            resizeHandles={["se"]}
            useCSSTransforms={true}
            isDraggable={true}
            isResizable={true}
          >
            {charts.map(card => (
              <div key={card.id}>
                <ChartCard card={card} />
              </div>
            ))}
          </ResponsiveGridLayout>
        ) : null}
      </div>
    </div>
  )
}
