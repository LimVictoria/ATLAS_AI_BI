"use client"
import dynamic from "next/dynamic"
import {
  X, Copy, BarChart2, GripHorizontal, Pencil, Check,
  SlidersHorizontal, ChevronDown, Code2, RotateCcw
} from "lucide-react"
import { useDashboardStore, ChartCard as ChartCardType, ChartType } from "@/store/dashboard"
// runMetric removed — ChartCard now uses rerenderChart via card.sql
import { v4 as uuid } from "uuid"
import { useState, useRef, useEffect } from "react"

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false })

// ── Metric SQL lookup (mirrors backend metrics.py) ────────────────────────────
const METRIC_SQL: Record<string, { sql: string; python: string }> = {
  total_cost_by_brand: {
    sql: `SELECT brand, ROUND(SUM(total_cost_myr), 2) AS total_cost
FROM v_maintenance_full
GROUP BY brand
ORDER BY total_cost DESC`,
    python: `import plotly.express as px
df = run_query("SELECT brand, ROUND(SUM(total_cost_myr),2) AS total_cost FROM v_maintenance_full GROUP BY brand ORDER BY total_cost DESC")
fig = px.bar(df, x="brand", y="total_cost", title="Total Maintenance Cost by Brand")
fig.show()`,
  },
  cost_by_brand_and_component: {
    sql: `SELECT brand, component_category,
       ROUND(SUM(total_cost_myr), 2) AS total_cost
FROM v_maintenance_full
GROUP BY brand, component_category
ORDER BY brand, total_cost DESC`,
    python: `import plotly.express as px
df = run_query("""SELECT brand, component_category, ROUND(SUM(total_cost_myr),2) AS total_cost
FROM v_maintenance_full GROUP BY brand, component_category ORDER BY brand, total_cost DESC""")
fig = px.bar(df, x="brand", y="total_cost", color="component_category", barmode="stack")
fig.show()`,
  },
  failure_count_by_component: {
    sql: `SELECT component_category,
       COUNT(*) AS failure_count
FROM v_maintenance_full
WHERE maintenance_type = 'Unscheduled'
GROUP BY component_category
ORDER BY failure_count DESC`,
    python: `import plotly.express as px
df = run_query("""SELECT component_category, COUNT(*) AS failure_count
FROM v_maintenance_full WHERE maintenance_type='Unscheduled'
GROUP BY component_category ORDER BY failure_count DESC""")
fig = px.bar(df, x="failure_count", y="component_category", orientation="h")
fig.show()`,
  },
}

const FALLBACK_SQL = (metricId: string, cardSql?: string) => ({
  sql: cardSql || `-- SQL not available for: ${metricId}\n-- Ask the AI to regenerate this chart to see its SQL`,
  python: cardSql
    ? `import plotly.express as px\nfrom db.duckdb_session import run_query\n\ndf = run_query("""\n${cardSql}\n""")\nprint(df.head())`
    : `# Ask the AI to regenerate this chart to see its SQL`,
})

// ── Colourful SVG micro-icons ─────────────────────────────────────────────────
const CHART_ICONS: Record<string, React.ReactNode> = {
  bar: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="7" width="2.5" height="5" rx="0.8" fill="#3B82F6"/><rect x="5" y="4" width="2.5" height="8" rx="0.8" fill="#2563EB"/><rect x="9" y="1.5" width="2.5" height="10.5" rx="0.8" fill="#1D4ED8"/></svg>,
  line: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><polyline points="1,10 4,6 7,8 10,3 12,5" stroke="#10B981" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" fill="none"/><circle cx="1" cy="10" r="1.2" fill="#10B981"/><circle cx="7" cy="8" r="1.2" fill="#10B981"/><circle cx="12" cy="5" r="1.2" fill="#10B981"/></svg>,
  pie: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><path d="M6.5 6.5 L6.5 1 A5.5 5.5 0 0 1 11.5 8.5 Z" fill="#F59E0B"/><path d="M6.5 6.5 L11.5 8.5 A5.5 5.5 0 0 1 2 10 Z" fill="#EF4444"/><path d="M6.5 6.5 L2 10 A5.5 5.5 0 0 1 6.5 1 Z" fill="#8B5CF6"/></svg>,
  table: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="1" width="11" height="3" rx="1" fill="#475569"/><rect x="1" y="5.5" width="5" height="2.2" rx="0.6" fill="#94A3B8"/><rect x="7" y="5.5" width="5" height="2.2" rx="0.6" fill="#94A3B8"/><rect x="1" y="9" width="5" height="2.2" rx="0.6" fill="#CBD5E1"/><rect x="7" y="9" width="5" height="2.2" rx="0.6" fill="#CBD5E1"/></svg>,
  pareto: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="3" width="2.5" height="9" rx="0.8" fill="#6366F1"/><rect x="5" y="5.5" width="2.5" height="6.5" rx="0.8" fill="#818CF8"/><rect x="9" y="8" width="2.5" height="4" rx="0.8" fill="#A5B4FC"/><polyline points="2.25,3 6.25,5.5 10.25,8" stroke="#EF4444" strokeWidth="1.5" strokeLinecap="round" strokeDasharray="1.5 1" fill="none"/></svg>,
  waterfall: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="5" width="2.2" height="6" rx="0.7" fill="#0891B2"/><rect x="4" y="3" width="2.2" height="2" rx="0.7" fill="#10B981"/><rect x="7" y="1.5" width="2.2" height="3.5" rx="0.7" fill="#10B981"/><rect x="10" y="1" width="2.2" height="10" rx="0.7" fill="#1E293B"/><line x1="3.2" y1="5" x2="4" y2="5" stroke="#94A3B8" strokeWidth="0.8" strokeDasharray="1 0.8"/><line x1="6.2" y1="3" x2="7" y2="3" stroke="#94A3B8" strokeWidth="0.8" strokeDasharray="1 0.8"/></svg>,
  heatmap: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="1" width="3" height="3" rx="0.6" fill="#BFDBFE"/><rect x="5" y="1" width="3" height="3" rx="0.6" fill="#60A5FA"/><rect x="9" y="1" width="3" height="3" rx="0.6" fill="#2563EB"/><rect x="1" y="5" width="3" height="3" rx="0.6" fill="#60A5FA"/><rect x="5" y="5" width="3" height="3" rx="0.6" fill="#2563EB"/><rect x="9" y="5" width="3" height="3" rx="0.6" fill="#1D4ED8"/><rect x="1" y="9" width="3" height="3" rx="0.6" fill="#3B82F6"/><rect x="5" y="9" width="3" height="3" rx="0.6" fill="#1D4ED8"/><rect x="9" y="9" width="3" height="3" rx="0.6" fill="#1E3A8A"/></svg>,
  boxplot: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="2" y="4" width="4" height="5" rx="0.8" stroke="#7C3AED" strokeWidth="1.3" fill="rgba(124,58,237,0.12)"/><line x1="4" y1="2" x2="4" y2="4" stroke="#7C3AED" strokeWidth="1.2" strokeLinecap="round"/><line x1="4" y1="9" x2="4" y2="11" stroke="#7C3AED" strokeWidth="1.2" strokeLinecap="round"/><line x1="2.5" y1="6.5" x2="5.5" y2="6.5" stroke="#7C3AED" strokeWidth="1.5" strokeLinecap="round"/><rect x="7.5" y="3" width="4" height="6" rx="0.8" stroke="#10B981" strokeWidth="1.3" fill="rgba(16,185,129,0.12)"/><line x1="9.5" y1="1.5" x2="9.5" y2="3" stroke="#10B981" strokeWidth="1.2" strokeLinecap="round"/><line x1="9.5" y1="9" x2="9.5" y2="11" stroke="#10B981" strokeWidth="1.2" strokeLinecap="round"/><line x1="8" y1="6" x2="11" y2="6" stroke="#10B981" strokeWidth="1.5" strokeLinecap="round"/></svg>,
  scatter: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><circle cx="3" cy="10" r="1.5" fill="#F59E0B"/><circle cx="5" cy="7" r="1.2" fill="#EF4444"/><circle cx="7" cy="5" r="1.8" fill="#8B5CF6"/><circle cx="9" cy="3" r="1.2" fill="#3B82F6"/><circle cx="11" cy="6" r="1.5" fill="#10B981"/></svg>,
  treemap: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="1" width="7" height="7" rx="0.8" fill="#059669"/><rect x="9" y="1" width="3" height="3.2" rx="0.8" fill="#10B981"/><rect x="9" y="5" width="3" height="3" rx="0.8" fill="#34D399"/><rect x="1" y="9" width="3.5" height="3" rx="0.8" fill="#6EE7B7"/><rect x="5.5" y="9" width="3" height="3" rx="0.8" fill="#A7F3D0"/><rect x="9.5" y="9" width="2.5" height="3" rx="0.8" fill="#D1FAE5"/></svg>,
  stacked_bar: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="8" width="2.5" height="4" rx="0.7" fill="#1D4ED8"/><rect x="1" y="5" width="2.5" height="3" fill="#7C3AED"/><rect x="1" y="3" width="2.5" height="2" rx="0.7" fill="#059669"/><rect x="5" y="6" width="2.5" height="6" rx="0.7" fill="#1D4ED8"/><rect x="5" y="3.5" width="2.5" height="2.5" fill="#7C3AED"/><rect x="5" y="2" width="2.5" height="1.5" rx="0.7" fill="#059669"/><rect x="9" y="7" width="2.5" height="5" rx="0.7" fill="#1D4ED8"/><rect x="9" y="4.5" width="2.5" height="2.5" fill="#7C3AED"/><rect x="9" y="2.5" width="2.5" height="2" rx="0.7" fill="#D97706"/></svg>,
  histogram: <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><rect x="1" y="9" width="1.8" height="3" rx="0.5" fill="#F59E0B" opacity="0.6"/><rect x="3.2" y="6" width="1.8" height="6" rx="0.5" fill="#F59E0B" opacity="0.75"/><rect x="5.4" y="3" width="1.8" height="9" rx="0.5" fill="#F59E0B"/><rect x="7.6" y="5" width="1.8" height="7" rx="0.5" fill="#F59E0B" opacity="0.75"/><rect x="9.8" y="8" width="1.8" height="4" rx="0.5" fill="#F59E0B" opacity="0.5"/></svg>,
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

function GlassBtn({ onClick, active, activeColor, activeGlass, title, children }: {
  onClick: (e: React.MouseEvent) => void
  active?: boolean; activeColor?: string; activeGlass?: string
  title?: string; children: React.ReactNode
}) {
  const [hovered, setHovered] = useState(false)
  return (
    <button title={title}
      onMouseDown={e => e.stopPropagation()}
      onClick={e => { e.stopPropagation(); onClick(e) }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        display: "flex", alignItems: "center", justifyContent: "center",
        width: 30, height: 30, borderRadius: 8,
        border: active ? `1.5px solid ${activeColor}60` : `1px solid ${hovered ? "#D1D5DB" : "#E5E7EB"}`,
        background: active ? `linear-gradient(145deg, ${activeGlass}, ${activeColor}25)` : hovered ? "linear-gradient(145deg, #F9FAFB, #F3F4F6)" : "linear-gradient(145deg, #FFFFFF, #F8FAFC)",
        color: active ? activeColor : hovered ? "#374151" : "#9CA3AF",
        cursor: "pointer",
        boxShadow: active ? `0 2px 8px ${activeColor}30, inset 0 1px 0 rgba(255,255,255,0.4)` : hovered ? "0 2px 6px rgba(0,0,0,0.08), inset 0 1px 0 rgba(255,255,255,0.8)" : "0 1px 3px rgba(0,0,0,0.05), inset 0 1px 0 rgba(255,255,255,0.9)",
        transition: "all 0.15s ease", backdropFilter: "blur(4px)", flexShrink: 0,
      }}
    >{children}</button>
  )
}

function MultiSelect({ dim, values, options, color, glass, onChange }: {
  dim: string; values: string[]; options: string[]
  color: string; glass: string; onChange: (vals: string[]) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false) }
    document.addEventListener("mousedown", h)
    return () => document.removeEventListener("mousedown", h)
  }, [])
  const toggle = (v: string) => onChange(values.includes(v) ? values.filter(x => x !== v) : [...values, v])
  const label = values.length === 0 ? "All" : values.length === 1 ? values[0] : `${values.length} selected`
  const active = values.length > 0
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button onMouseDown={e => e.stopPropagation()} onClick={e => { e.stopPropagation(); setOpen(o => !o) }}
        style={{ display: "flex", alignItems: "center", gap: 5, padding: "4px 10px", borderRadius: 8, border: `1px solid ${active ? color + "60" : "#E5E7EB"}`, background: active ? `linear-gradient(145deg, ${glass}, ${color}18)` : "linear-gradient(145deg, #FFFFFF, #F8FAFC)", color: active ? color : "#6B7280", fontSize: 11, fontWeight: 500, cursor: "pointer", boxShadow: active ? `0 2px 8px ${color}20` : "0 1px 3px rgba(0,0,0,0.05)", whiteSpace: "nowrap", transition: "all 0.15s" }}>
        <span style={{ textTransform: "capitalize" }}>{dim.replace(/_/g, " ")}</span>
        <span style={{ opacity: 0.4, fontSize: 9 }}>▸</span>
        <span style={{ opacity: active ? 1 : 0.6 }}>{label}</span>
        <ChevronDown size={9} style={{ opacity: 0.4 }} />
      </button>
      {open && (
        <div style={{ position: "absolute", top: "calc(100% + 6px)", left: 0, zIndex: 9999, background: "rgba(255,255,255,0.97)", backdropFilter: "blur(20px)", borderRadius: 10, boxShadow: "0 8px 32px rgba(0,0,0,0.14)", border: "1px solid rgba(255,255,255,0.6)", minWidth: 170, padding: "5px 0" }}>
          {["All", ...options].map(opt => {
            const isAll = opt === "All"; const isActive = isAll ? values.length === 0 : values.includes(opt)
            return (
              <div key={opt} onMouseDown={e => e.stopPropagation()} onClick={e => { e.stopPropagation(); isAll ? onChange([]) : toggle(opt) }}
                style={{ display: "flex", alignItems: "center", gap: 9, padding: "7px 13px", cursor: "pointer", fontSize: 12, color: isActive ? color : "#374155", background: isActive ? `${color}0E` : "transparent", fontWeight: isActive ? 600 : 400 }}
                onMouseEnter={e => { if (!isActive) e.currentTarget.style.background = "rgba(0,0,0,0.03)" }}
                onMouseLeave={e => { e.currentTarget.style.background = isActive ? `${color}0E` : "transparent" }}>
                <div style={{ width: 15, height: 15, borderRadius: 4, flexShrink: 0, border: `1.5px solid ${isActive ? color : "#D1D5DB"}`, background: isActive ? color : "transparent", display: "flex", alignItems: "center", justifyContent: "center" }}>
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

function CardFilterPanel({ filters, color, glass, onFilterChange }: {
  filters: Record<string, any>; color: string; glass: string
  onFilterChange: (key: string, vals: string[]) => void
}) {
  return (
    <div style={{ padding: "8px 12px 10px", borderBottom: "1px solid #F1F5F9", background: "linear-gradient(to bottom, #FAFBFC, #F5F7FA)", display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center" }}>
      <span style={{ fontSize: 10, fontWeight: 600, color: "#94A3B8", letterSpacing: "0.06em", marginRight: 2 }}>FILTER</span>
      {Object.keys(FILTER_OPTIONS).map(dim => (
        <MultiSelect key={dim} dim={dim}
          values={Array.isArray(filters[dim]) ? filters[dim] : filters[dim] ? [filters[dim]] : []}
          options={FILTER_OPTIONS[dim]} color={color} glass={glass}
          onChange={vals => onFilterChange(dim, vals)} />
      ))}
    </div>
  )
}

// ── Code back-face ────────────────────────────────────────────────────────────
function CodeFace({ metricId, sql, color, light }: { metricId: string; sql?: string; color: string; light: string }) {
  const info = METRIC_SQL[metricId] ?? FALLBACK_SQL(metricId, sql)
  const [tab, setTab] = useState<"sql" | "python">("sql")
  const [copied, setCopied] = useState(false)
  const code = tab === "sql" ? info.sql : info.python

  const copy = () => {
    navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", background: "#0F172A", overflow: "hidden" }}>
      {/* Tab bar */}
      <div style={{ display: "flex", alignItems: "center", padding: "8px 12px 0", gap: 4, borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
        {(["sql", "python"] as const).map(t => (
          <button key={t} onClick={() => setTab(t)}
            onMouseDown={e => e.stopPropagation()}
            style={{
              padding: "4px 12px", borderRadius: "6px 6px 0 0", fontSize: 11, fontWeight: 600,
              border: "none", cursor: "pointer", transition: "all 0.12s",
              background: tab === t ? "rgba(255,255,255,0.08)" : "transparent",
              color: tab === t ? color : "#475569",
              borderBottom: tab === t ? `2px solid ${color}` : "2px solid transparent",
            }}>{t.toUpperCase()}</button>
        ))}
        <button onClick={copy} onMouseDown={e => e.stopPropagation()}
          style={{ marginLeft: "auto", fontSize: 10, padding: "3px 10px", borderRadius: 6, border: `1px solid rgba(255,255,255,0.1)`, background: copied ? color : "rgba(255,255,255,0.06)", color: copied ? "#fff" : "#64748B", cursor: "pointer", transition: "all 0.15s" }}>
          {copied ? "Copied!" : "Copy"}
        </button>
      </div>
      {/* Code */}
      <div style={{ flex: 1, overflow: "auto", padding: "14px 16px" }}>
        <pre style={{ margin: 0, fontSize: 12, lineHeight: 1.8, color: "#E2E8F0", fontFamily: "'Fira Code', 'Cascadia Code', 'Courier New', monospace", whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
          {code.split("\n").map((line, i) => (
            <div key={i} style={{ display: "flex", gap: 12 }}>
              <span style={{ color: "#475569", userSelect: "none", minWidth: 24, textAlign: "right", fontSize: 10, paddingTop: 1 }}>{i + 1}</span>
              <span style={{ color: "#E2E8F0" }}>{line}</span>
            </div>
          ))}
        </pre>
      </div>
      {/* Footer note */}
      <div style={{ padding: "6px 14px 8px", borderTop: "1px solid rgba(255,255,255,0.05)", fontSize: 10, color: "#334155" }}>
        metric_id: <span style={{ color: "#38BDF8" }}>{metricId}</span>
        <span style={{ marginLeft: 12, color: "#1E293B" }}>· table: </span>
        <span style={{ color: "#86EFAC" }}>v_maintenance_full</span>
      </div>
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
  const [flipped, setFlipped] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  useEffect(() => { if (editingTitle) inputRef.current?.focus() }, [editingTitle])

  const cat = CAT[card.category] || CAT.General

  const applyCardFilter = async (key: string, vals: string[]) => {
    const newFilters = { ...card.filters }
    if (vals.length === 0) delete newFilters[key]
    else newFilters[key] = vals.length === 1 ? vals[0] : vals
    updateChart(card.id, { loading: true, filters: newFilters })
    try {
      if (card.sql) {
        const { rerenderChart } = await import("@/utils/api")
        const result = await rerenderChart(card.sql, card.chart_type, card.title, card.category, newFilters)
        updateChart(card.id, {
          chart_data: result.chart,
          sql: result.sql || card.sql,
          filters: newFilters,
          loading: false,
        })
      } else {
        // No SQL — just update filters display without re-fetching
        updateChart(card.id, { filters: newFilters, loading: false })
      }
    } catch { updateChart(card.id, { loading: false }) }
  }

  const switchChartType = async (type: string) => {
    if (type === card.chart_type) return
    if (!card.sql) {
      // Old card with no SQL — ask user to regenerate
      alert("This chart was created in a previous session. Ask the AI to regenerate it, then you can switch chart types.")
      return
    }
    updateChart(card.id, { loading: true })
    try {
      const { rerenderChart } = await import("@/utils/api")
      const result = await rerenderChart(card.sql, type, card.title, card.category, card.filters || {})
      updateChart(card.id, {
        chart_type: type as ChartType,
        chart_data: result.chart,
        sql: result.sql || card.sql,
        available_charts: result.available_charts || card.available_charts,
        loading: false,
      })
    } catch { updateChart(card.id, { loading: false }) }
  }

  const saveTitle = () => { updateChart(card.id, { title: titleDraft.trim() || card.title }); setEditingTitle(false) }

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
  const needsLegend = ["pie", "pareto", "heatmap", "scatter", "treemap", "stacked_bar"].includes(card.chart_type)

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", borderRadius: 14, overflow: "hidden", border: card.selected ? `1.5px solid ${cat.color}80` : "1px solid #E4E8EF", boxShadow: card.selected ? `0 0 0 3px ${cat.color}18, 0 8px 24px rgba(0,0,0,0.10)` : "0 2px 8px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.04)", background: flipped ? "#0F172A" : "#FFFFFF", transition: "box-shadow 0.2s, border-color 0.2s, background 0.3s" }}
      onClick={e => {
        if ((e.target as HTMLElement).closest("button,input,select,[data-no-drag]")) return
        toggleSelect(card.id, e.shiftKey)
      }}
    >
      {/* ── Header ── */}
      <div className="chart-drag-handle" style={{ display: "flex", alignItems: "center", padding: "0 8px", height: 44, borderBottom: flipped ? "1px solid rgba(255,255,255,0.06)" : "1px solid #F0F2F5", background: flipped ? "linear-gradient(to bottom, #1E293B, #0F172A)" : "linear-gradient(to bottom, #FAFBFD, #F5F7FA)", cursor: "grab", flexShrink: 0, transition: "background 0.3s" }}>

        <GripHorizontal size={13} style={{ color: flipped ? "#334155" : "#C8D0DA", flexShrink: 0, marginRight: 4 }} />

        <div data-no-drag onMouseDown={e => e.stopPropagation()} style={{ marginRight: 6 }}>
          <input type="checkbox" checked={card.selected} onChange={() => {}}
            onClick={e => { e.stopPropagation(); toggleSelect(card.id, e.shiftKey) }}
            style={{ cursor: "pointer", accentColor: cat.color, width: 13, height: 13 }} />
        </div>

        <span style={{ fontSize: 9.5, fontWeight: 700, padding: "2px 7px", borderRadius: 99, color: cat.color, background: flipped ? `${cat.color}20` : cat.light, border: `1px solid ${cat.border}`, letterSpacing: "0.05em", flexShrink: 0, lineHeight: 1.7 }}>
          {card.category.toUpperCase()}
        </span>

        {/* Title */}
        <div data-no-drag onMouseDown={e => e.stopPropagation()} style={{ flex: 1, minWidth: 0, display: "flex", alignItems: "center", gap: 4, margin: "0 6px" }}>
          {editingTitle ? (
            <>
              <input ref={inputRef} value={titleDraft}
                onChange={e => setTitleDraft(e.target.value)}
                onKeyDown={e => { if (e.key === "Enter") saveTitle(); if (e.key === "Escape") setEditingTitle(false) }}
                onBlur={saveTitle}
                style={{ flex: 1, minWidth: 0, fontSize: 11.5, fontWeight: 600, border: `1.5px solid ${cat.color}`, borderRadius: 6, padding: "2px 8px", outline: "none", color: "#0F172A", background: "#fff", fontFamily: "inherit" }} />
              <GlassBtn onClick={saveTitle} active activeColor={cat.color} activeGlass={cat.glass}><Check size={11} /></GlassBtn>
            </>
          ) : (
            <>
              <span style={{ fontSize: 11.5, fontWeight: 600, color: flipped ? "#94A3B8" : "#1E293B", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}
                onDoubleClick={() => { setTitleDraft(card.title); setEditingTitle(true) }}
                title={`${card.title} — double-click to rename`}>
                {card.title}
              </span>
              {!flipped && (
                <GlassBtn onClick={() => { setTitleDraft(card.title); setEditingTitle(true) }} title="Rename"><Pencil size={11} /></GlassBtn>
              )}
            </>
          )}
        </div>

        {/* Right actions */}
        <div data-no-drag onMouseDown={e => e.stopPropagation()} style={{ display: "flex", gap: 4, alignItems: "center", flexShrink: 0 }}>

          {/* Flip to code button */}
          <GlassBtn onClick={() => setFlipped(f => !f)} title={flipped ? "Show chart" : "Show SQL & code"}
            active={flipped} activeColor="#0891B2" activeGlass="rgba(8,145,178,0.12)">
            {flipped ? <RotateCcw size={12} /> : <Code2 size={12} />}
          </GlassBtn>

          {!flipped && (
            <>
              <GlassBtn onClick={() => setShowFilters(f => !f)} active={showFilters || hasActiveFilters} activeColor={cat.color} activeGlass={cat.glass} title="Filters">
                <div style={{ position: "relative" }}>
                  <SlidersHorizontal size={12} />
                  {hasActiveFilters && <span style={{ position: "absolute", top: -4, right: -4, width: 6, height: 6, borderRadius: "50%", background: cat.color, border: "1.5px solid #fff" }} />}
                </div>
              </GlassBtn>

              <div style={{ width: 1, height: 18, background: "#E8ECF0", margin: "0 2px" }} />

              {card.available_charts?.map(t => (
                <GlassBtn key={t} title={t.charAt(0).toUpperCase() + t.slice(1).replace("_", " ")}
                  onClick={() => switchChartType(t)}
                  active={card.chart_type === t} activeColor={cat.color} activeGlass={cat.glass}>
                  {CHART_ICONS[t] ?? <BarChart2 size={12} />}
                </GlassBtn>
              ))}

              <div style={{ width: 1, height: 18, background: "#E8ECF0", margin: "0 2px" }} />

              <GlassBtn title="Duplicate" onClick={() => addChart({ ...card, id: uuid(), selected: false, title: card.title + " (copy)" })}>
                <Copy size={12} />
              </GlassBtn>
            </>
          )}

          <GlassBtn title="Remove" onClick={() => removeChart(card.id)}>
            <X size={12} />
          </GlassBtn>
        </div>
      </div>

      {!flipped && showFilters && (
        <CardFilterPanel filters={card.filters || {}} color={cat.color} glass={cat.glass} onFilterChange={applyCardFilter} />
      )}

      {/* ── Active filter caption ── */}
      {!flipped && hasActiveFilters && (() => {
        const activeFilters = Object.entries(card.filters || {})
          .filter(([k, v]) => k !== "time_shortcut" && v && (Array.isArray(v) ? v.length > 0 : true))
        const FILTER_COLORS: Record<string, string> = {
          brand: "#2563EB", year: "#0891B2", quarter: "#7C3AED",
          fleet_segment: "#059669", maintenance_type: "#D97706",
          criticality_level: "#DC2626", workshop_state: "#0891B2",
        }
        return (
          <div style={{ padding: "5px 12px 5px", display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center", borderBottom: "1px solid #F1F5F9", background: "#FAFBFC" }}>
            <span style={{ fontSize: 9.5, fontWeight: 700, color: "#94A3B8", letterSpacing: "0.07em", marginRight: 2 }}>SHOWING</span>
            {activeFilters.map(([key, val]) => {
              const vals = Array.isArray(val) ? val : [val]
              const color = FILTER_COLORS[key] || "#475569"
              return vals.map(v => (
                <span key={`${key}-${v}`} style={{
                  fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 99,
                  color: color, background: `${color}12`,
                  border: `1px solid ${color}30`,
                  display: "flex", alignItems: "center", gap: 4,
                }}>
                  <span style={{ opacity: 0.5, fontSize: 9 }}>{key.replace(/_/g, " ")}</span>
                  <span>·</span>
                  {v}
                </span>
              ))
            })}
          </div>
        )
      })()}

      {/* ── Body ── */}
      {flipped ? (
        <CodeFace metricId={card.metric_id} sql={card.sql} color={cat.color} light={cat.light} />
      ) : (
        <div style={{ flex: 1, minHeight: 0, display: "flex", alignItems: "center", justifyContent: "center" }}>
          {card.loading ? (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 10 }}>
              <div style={{ width: 30, height: 30, borderRadius: "50%", border: `3px solid ${cat.light}`, borderTopColor: cat.color, animation: "spin 0.7s linear infinite" }} />
              <span style={{ fontSize: 11, color: "#94A3B8", fontWeight: 500 }}>Loading...</span>
            </div>
          ) : plotData?.data ? (
            <Plot
              data={plotData.data}
              layout={{ ...(plotData.layout || {}), paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: { color: "#334155", family: "Inter, system-ui, sans-serif", size: 11 }, margin: { t: 8, r: card.chart_type === "heatmap" ? 80 : 16, b: 44, l: 56 }, autosize: true, showlegend: needsLegend, title: undefined }}
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
      )}
    </div>
  )
}
