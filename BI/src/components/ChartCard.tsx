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
  bar:    <BarChart2 size={11} />,
  line:   <TrendingUp size={11} />,
  pie:    <PieChart size={11} />,
  table:  <Table size={11} />,
  pareto: <TrendingDown size={11} />,
}

// Brand & dimension options — extend as needed
const FILTER_OPTIONS: Record<string, string[]> = {
  brand: ["Scania", "Volvo", "Mercedes-Benz", "MAN", "Hino"],
  fleet_segment: ["Heavy", "Medium", "Light"],
  maintenance_type: ["Scheduled", "Unscheduled"],
  criticality_level: ["Critical", "High", "Medium"],
  workshop_state: ["Selangor", "Johor", "Perak", "Kedah", "Penang"],
  year: ["2020", "2021", "2022", "2023", "2024"],
  quarter: ["1", "2", "3", "4"],
}

const CAT: Record<string, { color: string; light: string; border: string }> = {
  Cost:     { color: "#2563eb", light: "#EFF6FF", border: "#BFDBFE" },
  Downtime: { color: "#7C3AED", light: "#F5F3FF", border: "#DDD6FE" },
  Failure:  { color: "#DC2626", light: "#FEF2F2", border: "#FECACA" },
  Fleet:    { color: "#059669", light: "#ECFDF5", border: "#A7F3D0" },
  Workshop: { color: "#D97706", light: "#FFFBEB", border: "#FDE68A" },
  Time:     { color: "#0891B2", light: "#ECFEFF", border: "#A5F3FC" },
  General:  { color: "#475569", light: "#F8FAFC", border: "#E2E8F0" },
}

// ── Multi-select dropdown ────────────────────────────────────────────────────
function MultiSelect({
  dim, values, options, color, onChange
}: {
  dim: string; values: string[]; options: string[]; color: string;
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

  const toggle = (v: string) => {
    onChange(values.includes(v) ? values.filter(x => x !== v) : [...values, v])
  }

  const label = values.length === 0 ? "All" : values.length === 1 ? values[0] : `${values.length} selected`

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          display: "flex", alignItems: "center", gap: 4,
          padding: "3px 8px", borderRadius: 6,
          border: `1px solid ${values.length ? color : "#E2E8F0"}`,
          background: values.length ? `${color}0F` : "#FAFAFA",
          color: values.length ? color : "#64748B",
          fontSize: 11, fontWeight: 500, cursor: "pointer",
          whiteSpace: "nowrap",
        }}
      >
        <span style={{ textTransform: "capitalize" }}>{dim.replace("_", " ")}</span>
        <span style={{ opacity: 0.6 }}>·</span>
        <span>{label}</span>
        <ChevronDown size={9} style={{ opacity: 0.5, marginLeft: 1 }} />
      </button>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 4px)", left: 0, zIndex: 999,
          background: "#fff", borderRadius: 8, boxShadow: "0 8px 24px rgba(0,0,0,0.12)",
          border: "1px solid #F1F5F9", minWidth: 160, padding: "4px 0", overflow: "hidden",
        }}>
          {["All", ...options].map(opt => {
            const isAll = opt === "All"
            const active = isAll ? values.length === 0 : values.includes(opt)
            return (
              <div
                key={opt}
                onClick={() => isAll ? onChange([]) : toggle(opt)}
                style={{
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "7px 12px", cursor: "pointer", fontSize: 12,
                  color: active ? color : "#334155",
                  background: active ? `${color}0D` : "transparent",
                  fontWeight: active ? 600 : 400,
                }}
                onMouseEnter={e => (e.currentTarget.style.background = `${color}08`)}
                onMouseLeave={e => (e.currentTarget.style.background = active ? `${color}0D` : "transparent")}
              >
                <div style={{
                  width: 14, height: 14, borderRadius: 4,
                  border: `1.5px solid ${active ? color : "#CBD5E1"}`,
                  background: active ? color : "transparent",
                  display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                }}>
                  {active && <Check size={9} color="#fff" strokeWidth={3} />}
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

// ── Filter panel (per-card) ──────────────────────────────────────────────────
function CardFilterPanel({
  filters, color, onFilterChange
}: {
  filters: Record<string, any>; color: string;
  onFilterChange: (key: string, vals: string[]) => void
}) {
  const activeFilters = Object.entries(filters).filter(([k]) => k !== "time_shortcut")
  const allDims = Object.keys(FILTER_OPTIONS)

  return (
    <div style={{
      padding: "6px 12px 8px",
      borderBottom: "1px solid #F1F5F9",
      background: "#FAFAFA",
      display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center",
    }}>
      {allDims.map(dim => (
        <MultiSelect
          key={dim}
          dim={dim}
          values={(filters[dim] as string[]) || []}
          options={FILTER_OPTIONS[dim]}
          color={color}
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

  const duplicate = () => {
    addChart({ ...card, id: uuid(), selected: false, title: card.title + " (copy)" })
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
        borderRadius: 12, overflow: "hidden",
        border: card.selected ? `1.5px solid ${cat.color}` : "1px solid #E8ECF0",
        boxShadow: card.selected
          ? `0 0 0 3px ${cat.color}20, 0 4px 16px rgba(0,0,0,0.08)`
          : "0 1px 4px rgba(0,0,0,0.05), 0 4px 12px rgba(0,0,0,0.04)",
        background: "#FFFFFF",
        transition: "box-shadow 0.2s, border-color 0.2s",
      }}
      onClick={(e) => {
        if ((e.target as HTMLElement).closest("button,input,select,[data-no-select]")) return
        toggleSelect(card.id, e.shiftKey)
      }}
    >
      {/* ── Header ── */}
      <div
        className="chart-drag-handle"
        style={{
          display: "flex", alignItems: "center", gap: 6,
          padding: "9px 10px",
          borderBottom: "1px solid #F1F5F9",
          background: "linear-gradient(to bottom, #FAFBFC, #F7F9FB)",
          cursor: "grab", userSelect: "none", flexShrink: 0,
        }}
      >
        <GripHorizontal size={12} style={{ color: "#C8D0DA", flexShrink: 0 }} />

        <input
          type="checkbox" checked={card.selected} onChange={() => {}}
          onClick={e => { e.stopPropagation(); toggleSelect(card.id, e.shiftKey) }}
          style={{ cursor: "pointer", accentColor: cat.color, width: 13, height: 13, flexShrink: 0 }}
        />

        <span style={{
          fontSize: 10, fontWeight: 600, padding: "2px 7px", borderRadius: 99,
          color: cat.color, background: cat.light, border: `1px solid ${cat.border}`,
          letterSpacing: "0.04em", flexShrink: 0, lineHeight: 1.6,
        }}>
          {card.category}
        </span>

        {/* Editable title */}
        <div style={{ flex: 1, minWidth: 0, display: "flex", alignItems: "center", gap: 4 }}
          onClick={e => e.stopPropagation()} data-no-select>
          {editingTitle ? (
            <>
              <input
                ref={inputRef}
                value={titleDraft}
                onChange={e => setTitleDraft(e.target.value)}
                onKeyDown={e => { if (e.key === "Enter") saveTitle(); if (e.key === "Escape") setEditingTitle(false) }}
                onBlur={saveTitle}
                style={{
                  flex: 1, minWidth: 0, fontSize: 11, fontWeight: 600,
                  border: `1.5px solid ${cat.color}`, borderRadius: 5,
                  padding: "2px 7px", outline: "none", color: "#0F172A",
                  background: "#fff", fontFamily: "inherit",
                }}
              />
              <button onClick={saveTitle} style={{ background: "none", border: "none", cursor: "pointer", color: cat.color, padding: 2, lineHeight: 1 }}>
                <Check size={11} />
              </button>
            </>
          ) : (
            <>
              <span
                style={{
                  fontSize: 11, fontWeight: 600, color: "#1E293B",
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                  flex: 1, cursor: "text",
                }}
                onDoubleClick={() => { setTitleDraft(card.title); setEditingTitle(true) }}
                title="Double-click to rename"
              >
                {card.title}
              </span>
              <button
                onClick={() => { setTitleDraft(card.title); setEditingTitle(true) }}
                style={{ background: "none", border: "none", cursor: "pointer", color: "#CBD5E1", padding: 2, lineHeight: 1, flexShrink: 0 }}
                title="Rename"
              >
                <Pencil size={10} />
              </button>
            </>
          )}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 3, flexShrink: 0, alignItems: "center" }}
          onClick={e => e.stopPropagation()} data-no-select>

          {/* Filter toggle */}
          <button
            title="Card filters"
            onClick={() => setShowFilters(f => !f)}
            style={{
              display: "flex", alignItems: "center", justifyContent: "center",
              width: 26, height: 26, borderRadius: 6,
              border: `1px solid ${showFilters || hasActiveFilters ? cat.color : "#E2E8F0"}`,
              background: showFilters || hasActiveFilters ? cat.light : "#fff",
              color: showFilters || hasActiveFilters ? cat.color : "#94A3B8",
              cursor: "pointer", position: "relative",
            }}
          >
            <SlidersHorizontal size={11} />
            {hasActiveFilters && (
              <span style={{
                position: "absolute", top: -3, right: -3,
                width: 7, height: 7, borderRadius: "50%",
                background: cat.color, border: "1.5px solid #fff",
              }} />
            )}
          </button>

          <div style={{ width: 1, height: 16, background: "#F1F5F9", margin: "0 1px" }} />

          {/* Chart type buttons */}
          {card.available_charts?.map(t => (
            <button key={t} title={t} onClick={() => switchChartType(t)}
              style={{
                display: "flex", alignItems: "center", justifyContent: "center",
                width: 26, height: 26, borderRadius: 6,
                border: `1px solid ${card.chart_type === t ? cat.color : "#E2E8F0"}`,
                background: card.chart_type === t ? cat.color : "#fff",
                color: card.chart_type === t ? "#fff" : "#94A3B8",
                cursor: "pointer", transition: "all 0.12s",
              }}
            >
              {CHART_ICONS[t]}
            </button>
          ))}

          <div style={{ width: 1, height: 16, background: "#F1F5F9", margin: "0 1px" }} />

          <button title="Duplicate" onClick={duplicate}
            style={{ display: "flex", alignItems: "center", justifyContent: "center", width: 26, height: 26, borderRadius: 6, border: "1px solid #E2E8F0", background: "#fff", color: "#94A3B8", cursor: "pointer" }}>
            <Copy size={11} />
          </button>
          <button title="Remove" onClick={() => removeChart(card.id)}
            style={{ display: "flex", alignItems: "center", justifyContent: "center", width: 26, height: 26, borderRadius: 6, border: "1px solid #E2E8F0", background: "#fff", color: "#94A3B8", cursor: "pointer" }}>
            <X size={11} />
          </button>
        </div>
      </div>

      {/* ── Per-card filters ── */}
      {showFilters && (
        <CardFilterPanel
          filters={card.filters || {}}
          color={cat.color}
          onFilterChange={applyCardFilter}
        />
      )}

      {/* ── Chart body ── */}
      <div style={{ flex: 1, minHeight: 0, display: "flex", alignItems: "center", justifyContent: "center" }}>
        {card.loading ? (
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 10 }}>
            <div style={{
              width: 32, height: 32, borderRadius: "50%",
              border: `3px solid ${cat.light}`,
              borderTopColor: cat.color,
              animation: "spin 0.7s linear infinite",
            }} />
            <span style={{ fontSize: 11, color: "#94A3B8", fontWeight: 500 }}>Loading data...</span>
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
            <BarChart2 size={28} style={{ marginBottom: 8, opacity: 0.25 }} />
            <div style={{ fontSize: 12, fontWeight: 500 }}>No data available</div>
            <div style={{ fontSize: 11, marginTop: 4, opacity: 0.7 }}>Try adjusting the filters</div>
          </div>
        )}
      </div>
    </div>
  )
}
