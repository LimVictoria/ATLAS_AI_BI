"use client"
import { BarChart2, Layers } from "lucide-react"
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
      x: card.x ?? 0, y: card.y ?? 0,
      w: card.w ?? 6, h: card.h ?? 6,
      minW: 3, minH: 4,
    }))
  }

  const onLayoutChange = (_: any, allLayouts: any) => {
    const lg = allLayouts.lg || []
    lg.forEach((item: any) => {
      updateChart(item.i, { x: item.x, y: item.y, w: item.w, h: item.h })
    })
  }

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", height: "100vh", overflow: "hidden", background: "#F0F4F8" }}>
      {/* Topbar */}
      <div style={{
        height: 52, display: "flex", alignItems: "center", padding: "0 20px",
        background: "linear-gradient(135deg, #0F172A 0%, #1E293B 60%, #1e3a5f 100%)",
        borderBottom: "1px solid rgba(255,255,255,0.06)", flexShrink: 0,
        boxShadow: "0 1px 12px rgba(0,0,0,0.18)",
      }}>
        <Layers size={15} style={{ color: "#38BDF8", marginRight: 8, flexShrink: 0 }} />
        <span style={{ fontSize: 13, fontWeight: 700, color: "#F1F5F9", letterSpacing: "0.01em" }}>
          BI Board
        </span>

        {selected.length > 0 && (
          <span style={{
            fontSize: 10, color: "#38BDF8", marginLeft: 12, fontWeight: 600,
            background: "rgba(56,189,248,0.12)", border: "1px solid rgba(56,189,248,0.25)",
            padding: "2px 8px", borderRadius: 99,
          }}>
            {selected.length} selected
          </span>
        )}

        <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
          {selected.length > 0 && (
            <button onClick={clearSelection} style={{
              fontSize: 11, color: "#94A3B8",
              background: "rgba(255,255,255,0.06)",
              border: "1px solid rgba(255,255,255,0.1)",
              borderRadius: 6, padding: "4px 12px", cursor: "pointer",
              transition: "all 0.15s",
            }}
              onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.1)")}
              onMouseLeave={e => (e.currentTarget.style.background = "rgba(255,255,255,0.06)")}
            >
              Clear selection
            </button>
          )}

          {/* Category dots */}
          {["#3B82F6","#7C3AED","#DC2626","#059669","#D97706","#0891B2"].map((c, i) => (
            <div key={i} style={{ width: 7, height: 7, borderRadius: "50%", background: c, opacity: 0.7 }} />
          ))}

          {charts.length > 0 && (
            <span style={{
              fontSize: 10, color: "#94A3B8",
              background: "rgba(255,255,255,0.07)",
              border: "1px solid rgba(255,255,255,0.1)",
              padding: "3px 10px", borderRadius: 99, fontWeight: 500,
            }}>
              {charts.length} chart{charts.length !== 1 ? "s" : ""}
            </span>
          )}
        </div>
      </div>

      {/* Canvas */}
      <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden", padding: "16px 18px" }}>
        {charts.length === 0 ? (
          <div style={{
            height: "100%", display: "flex", flexDirection: "column",
            alignItems: "center", justifyContent: "center", gap: 14,
          }}>
            {/* Colourful empty state illustration */}
            <div style={{ display: "flex", gap: 6, alignItems: "flex-end", marginBottom: 4 }}>
              {[
                { h: 40, c: "#3B82F6" }, { h: 60, c: "#7C3AED" },
                { h: 50, c: "#059669" }, { h: 75, c: "#D97706" },
                { h: 45, c: "#DC2626" },
              ].map((b, i) => (
                <div key={i} style={{
                  width: 16, height: b.h, borderRadius: "4px 4px 2px 2px",
                  background: b.c, opacity: 0.25,
                }} />
              ))}
            </div>
            <p style={{ fontSize: 14, fontWeight: 700, color: "#475569", margin: 0 }}>No charts yet</p>
            <p style={{ fontSize: 12, color: "#94A3B8", margin: 0 }}>Ask the AI to visualise a metric.</p>
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
