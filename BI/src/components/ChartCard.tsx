"use client"
import dynamic from "next/dynamic"
import {
  X, Copy, BarChart2, TrendingUp, PieChart, Table,
  GripHorizontal, Pencil, Check, SlidersHorizontal, ChevronDown, TrendingDown
} from "lucide-react"
import { useDashboardStore, ChartCard as ChartCardType, ChartType } from "@/store/dashboard"
import { runMetric } from "@/utils/api"
import { v4 as uuid } from "uuid"
import { useState, useRef, useEffect } from "react"

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false })

const CHART_ICONS: Record<ChartType, React.ReactNode> = {
  bar:    <BarChart2 size={12} />,
  line:   <TrendingUp size={12} />,
  pie:    <PieChart size={12} />,
  table:  <Table size={12} />,
  pareto: <TrendingDown size={12} />,
}

const FILTER_OPTIONS: Record<string, string[]> = {
  brand: ["Scania", "Volvo", "Mercedes-Benz", "MAN", "Hino"],
  year: ["2020", "2021", "2022", "2023", "2024"],
  quarter: ["1", "2", "3", "4"],
  fleet_segment: ["Heavy", "Medium", "Light"],
  maintenance_type: ["Scheduled", "Unscheduled"],
  criticality_level: ["Critical", "High", "Medium"],
  workshop_state: ["Selangor", "Johor", "Perak", "Kedah", "Penang"],
}

const CAT: Record<string, { color: string; light: string; border: string; glass: string }> = {
  Cost:     { color: "#2563EB", light: "#EFF6FF", border: "#BFDBFE", glass: "rgba(37,99,235,0.12)" },
  Downtime: { color: "#7C3AED", light: "#F5F3FF", border: "#DDD6FE", glass: "rgba(124,58,237,0.12)" },
  Failure:  { color: "#DC2626", light: "#FEF2F2", border: "#FECACA", glass: "rgba(220,38,38,0.12)" },
  Fleet:    { color: "#059669", light: "#ECFDF5", border: "#A7F3D0", glass: "rgba(5,150,105,0.12)" },
  Workshop: { color: "#D97706", light: "#FFFBEB", border: "#FDE68A", glass: "rgba(217,119,6,0.12)" },
  Time:     { color: "#0891B2", light: "#ECFEFF", border: "#A5F3FC", glass: "rgba(8,145,178,0.12)" },
  General:  { color: "#475569", light: "#F8FAFC", border: "#E2E8F0", glass: "rgba(71,85,105,0.12)" },
}

// ── Glassy button ─────────────────────────────────────────────────────────────
function GlassBtn({
  onClick, active, activeColor, activeGlass, title, children
}: {
  onClick: (e: React.MouseEvent) => void
  active?: boolean
  activeColor?: string
  activeGlass?: string
  title?: string
  children: React.ReactNode
}) {
  const [hovered, setHovered] = useState(false)

  return (
    <button
      title={title}
      onMouseDown={e => e.stopPropagation()}
      onClick={e => { e.stopPropagation(); onClick(e) }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        display: "flex", alignItems: "center", justifyContent: "center",
        width: 30, height: 30, borderRadius: 8,
        border: active
          ? `1.5px solid ${activeColor}60`
          : `1px solid ${hovered ? "#D1D5DB" : "#E5E7EB"}`,
        background: active
          ? `linear-gradient(145deg, ${activeGlass}, ${activeColor}25)`
          : hovered
            ? "linear-gradient(145deg, #F9FAFB, #F3F4F6)"
            : "linear-gradient(145deg, #FFFFFF, #F8FAFC)",
        color: active ? activeColor : hovered ? "#374151" : "#9CA3AF",
        cursor: "pointer",
        boxShadow: active
          ? `0 2px 8px ${activeColor}30, inset 0 1px 0 rgba(255,255,255,0.4)`
          : hovered
            ? "0 2px 6px rgba(0,0,0,0.08), inset 0 1px 0 rgba(255,255,255,0.8)"
            : "0 1px 3px rgba(0,0,0,0.05), inset 0 1px 0 rgba(255,255,255,0.9)",
        transition: "all 0.15s ease",
        backdropFilter: "blur(4px)",
        flexShrink: 0,
      }}
    >
      {children}
    </button>
  )
}

// ── Multi-select dropdown ─────────────────────────────────────────────────────
function MultiSelect({ dim, values, options, color, glass, onChange }: {
  dim: string; values: string[]; options: string[]
  color: string; glass: string
  onChange: (vals: string[]) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener("mousedown", handler)
    return () => document.removeEventListener("mousedown", handler)
  }, [])

  const toggle = (v: string) =>
    onChange(values.includes(v) ? values.filter(x => x !== v) : [...values, v])

  const label = values.length === 0 ? "All" : values.length === 1 ? values[0] : `${values.length} selected`
  const active = values.length > 0

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        onMouseDown={e => e.stopPropagation()}
        onClick={e => { e.stopPropagation(); setOpen(o => !o) }}
        style={{
          display: "flex", alignItems: "center", gap: 5,
          padding: "4px 10px", borderRadius: 8,
          border: `1px solid ${active ? color + "60" : "#E5E7EB"}`,
          background: active
            ? `linear-gradient(145deg, ${glass}, ${color}18)`
            : "linear-gradient(145deg, #FFFFFF, #F8FAFC)",
          color: active ? color : "#6B7280",
          fontSize: 11, fontWeight: 500, cursor: "pointer",
          boxShadow: active
            ? `0 2px 8px ${color}20, inset 0 1px 0 rgba(255,255,255,0.5)`
            : "0 1px 3px rgba(0,0,0,0.05), inset 0 1px 0 rgba(255,255,255,0.9)",
          whiteSpace: "nowrap", transition: "all 0.15s",
        }}
      >
        <span style={{ textTransform: "capitalize" }}>{dim.replace(/_/g, " ")}</span>
        <span style={{ opacity: 0.4, fontSize: 9 }}>▸</span>
        <span style={{ opacity: active ? 1 : 0.6 }}>{label}</span>
        <ChevronDown size={9} style={{ opacity: 0.4 }} />
      </button>

      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 6px)", left: 0, zIndex: 9999,
          background: "rgba(255,255,255,0.95)",
          backdropFilter: "blur(20px) saturate(180%)",
          borderRadius: 10,
          boxShadow: "0 8px 32px rgba(0,0,0,0.14), 0 2px 8px rgba(0,0,0,0.08)",
          border: "1px solid rgba(255,255,255,0.6)",
          minWidth: 170, padding: "5px 0", overflow: "hidden",
        }}>
          {["All", ...options].map(opt => {
            const isAll = opt === "All"
            const isActive = isAll ? values.length === 0 : values.includes(opt)
            return (
              <div
                key={opt}
                onMouseDown={e => e.stopPropagation()}
                onClick={e => { e.stopPropagation(); isAll ? onChange([]) : toggle(opt) }}
                style={{
                  display: "flex", alignItems: "center", gap: 9,
                  padding: "7px 13px", cursor: "pointer", fontSize: 12,
                  color: isActive ? color : "#374155",
                  background: isActive ? `${color}0E` : "transparent",
                  fontWeight: isActive ? 600 : 400,
                  transition: "background 0.1s",
                }}
                onMouseEnter={e => { if (!isActive) e.currentTarget.style.background = "rgba(0,0,0,0.03)" }}
                onMouseLeave={e => { e.currentTarget.style.background = isActive ? `${color}0E` : "transparent" }}
              >
                <div style={{
                  width: 15, height: 15, borderRadius: 4, flexShrink: 0,
                  border: `1.5px solid ${isActive ? color : "#D1D5DB"}`,
                  background: isActive ? color : "transparent",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  boxShadow: isActive ? `0 1px 4px ${color}40` : "none",
                  transition: "all 0.15s",
                }}>
                  {isActive && <Check size={9} color="#fff" strokeWidth={3} />}
                </div>
                {opt}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Card filter panel ─────────────────────────────────────────────────────────
function CardFilterPanel({ filters, color, glass, onFilterChange }: {
  filters: Record<string, any>; color: string; glass: string
  onFilterChange: (key: string, vals: string[]) => void
}) {
  return (
    <div style={{
      padding: "8px 12px 10px",
      borderBottom: "1px solid #F1F5F9",
      background: "linear-gradient(to bottom, #FAFBFC, #F5F7FA)",
      display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center",
    }}>
      <span style={{ fontSize: 10, fontWeight: 600, color: "#94A3B8", letterSpacing: "0.06em", marginRight: 2 }}>
        FILTER
      </span>
      {Object.keys(FILTER_OPTIONS).map(dim => (
        <MultiSelect
          key={dim} dim={dim}
          values={Array.isArray(filters[dim]) ? filters[dim] : filters[dim] ? [filters[dim]] : []}
          options={FILTER_OPTIONS[dim]}
          color={color} glass={glass}
          onChange={vals => onFilterChange(dim, vals)}
        />
      ))}
    </div>
  )
}

// ── Main ChartCard ────────────────────────────────────────────────────────────
interface Props { card: ChartCardType }

export default function ChartCard({ card }: Props) {
  const { toggleSelect, removeChart, updateChart, addChart } = useDashboardStore()
  const [editingTitle, setEditingTitle] = useState(false)
  const [titleDraft, setTitleDraft] = useState(card.title)
  const [showFilters, setShowFilters] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => { if (editingTitle) inputRef.current?.focus() }, [editingTitle])

  const cat = CAT[card.category] || CAT.General

  const applyCardFilter = async (key: string, vals: string[]) => {
    const newFilters = { ...card.filters }
    if (vals.length === 0) delete newFilters[key]
    else newFilters[key] = vals.length === 1 ? vals[0] : vals
    updateChart(card.id, { loading: true, filters: newFilters })
    try {
      const result = await runMetric(card.metric_id, card.chart_type, newFilters)
      updateChart(card.id, { chart_data: result.chart, loading: false })
    } catch {
      updateChart(card.id, { loading: false })
    }
  }

  const switchChartType = async (type: ChartType) => {
    if (type === card.chart_type) return
    updateChart(card.id, { loading: true })
    try {
      const result = await runMetric(card.metric_id, type, card.filters)
      updateChart(card.id, { chart_type: type, chart_data: result.chart, loading: false })
    } catch {
      updateChart(card.id, { loading: false })
    }
  }

  const saveTitle = () => {
    updateChart(card.id, { title: titleDraft.trim() || card.title })
    setEditingTitle(false)
  }

  let plotData: any = null
  if (card.chart_data && !card.loading) {
    try {
      const raw = card.chart_data
      if (typeof raw === "string") plotData = JSON.parse(raw)
      else if (raw?.chart) plotData = typeof raw.chart === "string" ? JSON.parse(raw.chart) : raw.chart
      else plotData = raw
    } catch {}
  }

  const hasActiveFilters = Object.keys(card.filters || {}).some(k => k !== "time_shortcut")

  return (
    <div
      style={{
        height: "100%", display: "flex", flexDirection: "column",
        borderRadius: 14, overflow: "hidden",
        border: card.selected ? `1.5px solid ${cat.color}80` : "1px solid #E4E8EF",
        boxShadow: card.selected
          ? `0 0 0 3px ${cat.color}18, 0 8px 24px rgba(0,0,0,0.10)`
          : "0 2px 8px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.04)",
        background: "#FFFFFF",
        transition: "box-shadow 0.2s, border-color 0.2s",
      }}
      onClick={e => {
        if ((e.target as HTMLElement).closest("button,input,select,[data-no-drag]")) return
        toggleSelect(card.id, e.shiftKey)
      }}
    >
      {/* ── Header ── */}
      <div
        className="chart-drag-handle"
        style={{
          display: "flex", alignItems: "center",
          padding: "0 8px",
          height: 44,
          borderBottom: "1px solid #F0F2F5",
          background: "linear-gradient(to bottom, #FAFBFD, #F5F7FA)",
          cursor: "grab", flexShrink: 0,
        }}
      >
        {/* Drag grip — only this part drags */}
        <GripHorizontal size={13} style={{ color: "#C8D0DA", flexShrink: 0, marginRight: 4 }} />

        {/* Checkbox */}
        <div data-no-drag onMouseDown={e => e.stopPropagation()} style={{ marginRight: 6 }}>
          <input
            type="checkbox" checked={card.selected} onChange={() => {}}
            onClick={e => { e.stopPropagation(); toggleSelect(card.id, e.shiftKey) }}
            style={{ cursor: "pointer", accentColor: cat.color, width: 13, height: 13 }}
          />
        </div>

        {/* Category badge */}
        <span style={{
          fontSize: 9.5, fontWeight: 700, padding: "2px 7px", borderRadius: 99,
          color: cat.color, background: cat.light, border: `1px solid ${cat.border}`,
          letterSpacing: "0.05em", flexShrink: 0, lineHeight: 1.7,
          boxShadow: `0 1px 3px ${cat.glass}`,
        }}>
          {card.category.toUpperCase()}
        </span>

        {/* Title */}
        <div data-no-drag onMouseDown={e => e.stopPropagation()}
          style={{ flex: 1, minWidth: 0, display: "flex", alignItems: "center", gap: 4, margin: "0 6px" }}>
          {editingTitle ? (
            <>
              <input
                ref={inputRef}
                value={titleDraft}
                onChange={e => setTitleDraft(e.target.value)}
                onKeyDown={e => { if (e.key === "Enter") saveTitle(); if (e.key === "Escape") setEditingTitle(false) }}
                onBlur={saveTitle}
                style={{
                  flex: 1, minWidth: 0, fontSize: 11.5, fontWeight: 600,
                  border: `1.5px solid ${cat.color}`, borderRadius: 6,
                  padding: "2px 8px", outline: "none", color: "#0F172A",
                  background: "#fff", fontFamily: "inherit",
                  boxShadow: `0 0 0 3px ${cat.glass}`,
                }}
              />
              <GlassBtn onClick={saveTitle} active activeColor={cat.color} activeGlass={cat.glass}>
                <Check size={11} />
              </GlassBtn>
            </>
          ) : (
            <>
              <span
                style={{
                  fontSize: 11.5, fontWeight: 600, color: "#1E293B",
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1,
                }}
                onDoubleClick={() => { setTitleDraft(card.title); setEditingTitle(true) }}
                title={`${card.title} — double-click to rename`}
              >
                {card.title}
              </span>
              <GlassBtn onClick={() => { setTitleDraft(card.title); setEditingTitle(true) }} title="Rename">
                <Pencil size={11} />
              </GlassBtn>
            </>
          )}
        </div>

        {/* Right actions */}
        <div data-no-drag onMouseDown={e => e.stopPropagation()}
          style={{ display: "flex", gap: 4, alignItems: "center", flexShrink: 0 }}>

          {/* Filter */}
          <GlassBtn
            onClick={() => setShowFilters(f => !f)}
            active={showFilters || hasActiveFilters}
            activeColor={cat.color}
            activeGlass={cat.glass}
            title="Filters"
          >
            <div style={{ position: "relative" }}>
              <SlidersHorizontal size={12} />
              {hasActiveFilters && (
                <span style={{
                  position: "absolute", top: -4, right: -4,
                  width: 6, height: 6, borderRadius: "50%",
                  background: cat.color, border: "1.5px solid #fff",
                }} />
              )}
            </div>
          </GlassBtn>

          <div style={{ width: 1, height: 18, background: "#E8ECF0", margin: "0 2px" }} />

          {/* Chart type switchers */}
          {card.available_charts?.map(t => (
            <GlassBtn
              key={t} title={t}
              onClick={() => switchChartType(t)}
              active={card.chart_type === t}
              activeColor={cat.color}
              activeGlass={cat.glass}
            >
              {CHART_ICONS[t]}
            </GlassBtn>
          ))}

          <div style={{ width: 1, height: 18, background: "#E8ECF0", margin: "0 2px" }} />

          <GlassBtn title="Duplicate" onClick={() => addChart({ ...card, id: uuid(), selected: false, title: card.title + " (copy)" })}>
            <Copy size={12} />
          </GlassBtn>

          <GlassBtn title="Remove" onClick={() => removeChart(card.id)}>
            <X size={12} />
          </GlassBtn>
        </div>
      </div>

      {/* ── Per-card filters ── */}
      {showFilters && (
        <CardFilterPanel
          filters={card.filters || {}}
          color={cat.color}
          glass={cat.glass}
          onFilterChange={applyCardFilter}
        />
      )}

      {/* ── Chart body ── */}
      <div style={{ flex: 1, minHeight: 0, display: "flex", alignItems: "center", justifyContent: "center" }}>
        {card.loading ? (
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 10 }}>
            <div style={{
              width: 30, height: 30, borderRadius: "50%",
              border: `3px solid ${cat.light}`,
              borderTopColor: cat.color,
              animation: "spin 0.7s linear infinite",
            }} />
            <span style={{ fontSize: 11, color: "#94A3B8", fontWeight: 500 }}>Loading...</span>
          </div>
        ) : plotData?.data ? (
          <Plot
            data={plotData.data}
            layout={{
              ...(plotData.layout || {}),
              paper_bgcolor: "rgba(0,0,0,0)",
              plot_bgcolor: "rgba(0,0,0,0)",
              font: { color: "#334155", family: "Inter, system-ui, -apple-system, sans-serif", size: 11 },
              margin: { t: 8, r: 16, b: 44, l: 56 },
              autosize: true,
              showlegend: card.chart_type === "pie" || card.chart_type === "pareto",
              title: undefined,
            }}
            config={{ responsive: true, displayModeBar: false }}
            style={{ width: "100%", height: "100%" }}
            useResizeHandler={true}
          />
        ) : (
          <div style={{ textAlign: "center", color: "#94A3B8" }}>
            <BarChart2 size={28} style={{ marginBottom: 8, opacity: 0.2 }} />
            <div style={{ fontSize: 12, fontWeight: 500 }}>No data</div>
            <div style={{ fontSize: 11, marginTop: 3, opacity: 0.6 }}>Try adjusting filters</div>
          </div>
        )}
      </div>
    </div>
  )
}
