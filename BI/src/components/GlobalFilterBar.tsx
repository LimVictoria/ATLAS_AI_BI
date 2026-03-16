"use client"
import { useState, useRef, useEffect, useCallback } from "react"
import { Check, ChevronDown, X } from "lucide-react"
import { useDashboardStore } from "@/store/dashboard"
import { rerenderChart } from "@/utils/api"

// ── Types ─────────────────────────────────────────────────────────────────────
interface FilterDim {
  label: string
  column: string
  options: string[]
}

// ── Filter cache ──────────────────────────────────────────────────────────────
let _dimCache: Record<string, FilterDim> | null = null
let _dimFetch: Promise<void> | null = null

async function loadDims(): Promise<Record<string, FilterDim>> {
  if (_dimCache) return _dimCache
  if (_dimFetch) { await _dimFetch; return _dimCache! }
  _dimFetch = (async () => {
    try {
      const { getFilters } = await import("@/utils/api")
      const data = await getFilters()
      const result: Record<string, FilterDim> = {}
      // Always include quarter
      result["quarter"] = { label: "Quarter", column: "month", options: ["1","2","3","4"] }
      Object.entries(data.filters || {}).forEach(([key, cfg]: [string, any]) => {
        if (cfg.options?.length > 0) {
          result[key] = { label: cfg.label || key.replace(/_/g, " "), column: cfg.column, options: cfg.options }
        }
      })
      _dimCache = result
    } catch {
      _dimCache = { quarter: { label: "Quarter", column: "month", options: ["1","2","3","4"] } }
    }
  })()
  await _dimFetch
  return _dimCache!
}

// ── SearchableDropdown ────────────────────────────────────────────────────────
function SearchableDropdown({
  dimKey, dim, selectedCards, onApply, color,
}: {
  dimKey: string
  dim: FilterDim
  selectedCards: any[]
  onApply: (dimKey: string, vals: string[]) => void
  color: string
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState("")
  const ref = useRef<HTMLDivElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)

  // Compute current values across selected cards
  const cardValues: Record<string, string[]> = {}
  selectedCards.forEach(card => {
    const v = card.filters?.[dimKey]
    if (v) {
      const vals = Array.isArray(v) ? v : [v]
      vals.forEach(val => {
        if (!cardValues[val]) cardValues[val] = []
        cardValues[val].push(card.title)
      })
    }
  })
  // Union of all active values
  const activeVals = Object.keys(cardValues)
  const hasActive = activeVals.length > 0
  const isConflict = selectedCards.length > 1 && new Set(
    selectedCards.map(c => JSON.stringify(
      Array.isArray(c.filters?.[dimKey]) ? c.filters[dimKey] : c.filters?.[dimKey] ? [c.filters[dimKey]] : []
    ))
  ).size > 1

  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) { setOpen(false); setSearch("") }
    }
    document.addEventListener("mousedown", h)
    return () => document.removeEventListener("mousedown", h)
  }, [])

  useEffect(() => {
    if (open) setTimeout(() => searchRef.current?.focus(), 50)
    else setSearch("")
  }, [open])

  const toggle = (v: string) => {
    const next = activeVals.includes(v) ? activeVals.filter(x => x !== v) : [...activeVals, v]
    onApply(dimKey, next)
  }

  const filtered = search.trim()
    ? dim.options.filter(o => o.toLowerCase().includes(search.toLowerCase()))
    : dim.options

  const label = activeVals.length === 0 ? "All"
    : activeVals.length === 1 ? activeVals[0]
    : `${activeVals.length} selected`

  return (
    <div ref={ref} style={{ position: "relative", flexShrink: 0 }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          display: "flex", alignItems: "center", gap: 5, padding: "5px 10px",
          borderRadius: 7, fontSize: 11, fontWeight: 500, cursor: "pointer",
          border: `1px solid ${hasActive ? (isConflict ? "#F59E0B60" : `${color}60`) : "rgba(255,255,255,0.12)"}`,
          background: hasActive
            ? isConflict ? "rgba(245,158,11,0.12)" : `rgba(56,189,248,0.1)`
            : "rgba(255,255,255,0.06)",
          color: hasActive ? (isConflict ? "#F59E0B" : "#38BDF8") : "#94A3B8",
          whiteSpace: "nowrap", transition: "all 0.15s",
          boxShadow: hasActive ? `0 2px 8px rgba(56,189,248,0.15)` : "none",
        }}
      >
        <span>{dim.label}</span>
        {isConflict && <span style={{ fontSize: 9, opacity: 0.8 }}>⚡</span>}
        <span style={{ opacity: hasActive ? 1 : 0.5, maxWidth: 80, overflow: "hidden", textOverflow: "ellipsis" }}>{label}</span>
        <ChevronDown size={9} style={{ opacity: 0.5 }} />
      </button>

      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 6px)", left: 0, zIndex: 9999,
          background: "rgba(15,23,42,0.98)", backdropFilter: "blur(20px)",
          borderRadius: 10, boxShadow: "0 8px 32px rgba(0,0,0,0.4)",
          border: "1px solid rgba(255,255,255,0.1)", minWidth: 220, maxWidth: 300,
        }}>
          {/* Search */}
          <div style={{ padding: "8px 10px 6px", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
            <div style={{
              display: "flex", alignItems: "center", gap: 6,
              background: "rgba(255,255,255,0.06)", borderRadius: 6, padding: "5px 8px",
              border: "1px solid rgba(255,255,255,0.1)",
            }}>
              <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                <circle cx="4.5" cy="4.5" r="3.5" stroke="#475569" strokeWidth="1.3"/>
                <line x1="7.5" y1="7.5" x2="10" y2="10" stroke="#475569" strokeWidth="1.3" strokeLinecap="round"/>
              </svg>
              <input ref={searchRef} value={search} onChange={e => setSearch(e.target.value)}
                placeholder="Search..."
                style={{ border: "none", outline: "none", background: "transparent",
                  fontSize: 11, color: "#E2E8F0", width: "100%", fontFamily: "inherit" }}
              />
              {search && (
                <button onClick={() => setSearch("")}
                  style={{ border: "none", background: "none", cursor: "pointer", color: "#475569", fontSize: 12, padding: 0 }}>×</button>
              )}
            </div>
          </div>

          {/* Options */}
          <div style={{ maxHeight: 260, overflowY: "auto", padding: "4px 0" }}>
            {/* All */}
            {!search.trim() && (
              <div onClick={() => { onApply(dimKey, []); setOpen(false) }}
                style={{
                  display: "flex", alignItems: "center", gap: 9, padding: "7px 12px",
                  cursor: "pointer", fontSize: 12,
                  color: activeVals.length === 0 ? "#38BDF8" : "#94A3B8",
                  background: activeVals.length === 0 ? "rgba(56,189,248,0.08)" : "transparent",
                }}
                onMouseEnter={e => { if (activeVals.length > 0) e.currentTarget.style.background = "rgba(255,255,255,0.04)" }}
                onMouseLeave={e => { e.currentTarget.style.background = activeVals.length === 0 ? "rgba(56,189,248,0.08)" : "transparent" }}
              >
                <div style={{
                  width: 14, height: 14, borderRadius: 4, flexShrink: 0,
                  border: `1.5px solid ${activeVals.length === 0 ? "#38BDF8" : "#334155"}`,
                  background: activeVals.length === 0 ? "#38BDF8" : "transparent",
                  display: "flex", alignItems: "center", justifyContent: "center",
                }}>
                  {activeVals.length === 0 && <Check size={8} color="#0F172A" strokeWidth={3} />}
                </div>
                <span style={{ fontWeight: activeVals.length === 0 ? 600 : 400 }}>All</span>
              </div>
            )}

            {filtered.length === 0 && (
              <div style={{ padding: "10px 12px", fontSize: 11, color: "#475569", textAlign: "center" }}>
                No results for "{search}"
              </div>
            )}

            {filtered.map(opt => {
              const isActive = activeVals.includes(opt)
              const cardNames = cardValues[opt] || []
              const isConflictVal = cardNames.length > 0 && cardNames.length < selectedCards.length

              return (
                <div key={opt} onClick={() => toggle(opt)}
                  style={{
                    display: "flex", alignItems: "flex-start", gap: 9, padding: "6px 12px",
                    cursor: "pointer", fontSize: 12,
                    background: isActive ? "rgba(56,189,248,0.08)" : "transparent",
                  }}
                  onMouseEnter={e => { if (!isActive) e.currentTarget.style.background = "rgba(255,255,255,0.04)" }}
                  onMouseLeave={e => { e.currentTarget.style.background = isActive ? "rgba(56,189,248,0.08)" : "transparent" }}
                >
                  <div style={{
                    width: 14, height: 14, borderRadius: 4, flexShrink: 0, marginTop: 1,
                    border: `1.5px solid ${isActive ? "#38BDF8" : isConflictVal ? "#F59E0B" : "#334155"}`,
                    background: isActive ? "#38BDF8" : "transparent",
                    display: "flex", alignItems: "center", justifyContent: "center",
                  }}>
                    {isActive && <Check size={8} color="#0F172A" strokeWidth={3} />}
                    {!isActive && isConflictVal && <div style={{ width: 6, height: 6, borderRadius: 2, background: "#F59E0B" }} />}
                  </div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ color: isActive ? "#38BDF8" : isConflictVal ? "#F59E0B" : "#CBD5E1", fontWeight: isActive || isConflictVal ? 600 : 400 }}>
                      {opt}
                    </div>
                    {isConflictVal && (
                      <div style={{ fontSize: 10, color: "#64748B", marginTop: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        applied to: {cardNames.join(", ")}
                      </div>
                    )}
                  </div>
                </div>
              )
            })}
          </div>

          {/* Footer */}
          {activeVals.length > 0 && (
            <div style={{
              padding: "5px 12px 7px", borderTop: "1px solid rgba(255,255,255,0.06)",
              display: "flex", alignItems: "center", justifyContent: "space-between",
            }}>
              <span style={{ fontSize: 10, color: "#475569" }}>{activeVals.length} active</span>
              <button onClick={() => { onApply(dimKey, []); setOpen(false) }}
                style={{ fontSize: 10, color: "#38BDF8", background: "none", border: "none", cursor: "pointer", fontWeight: 600 }}>
                Clear
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── GlobalFilterBar ───────────────────────────────────────────────────────────
export default function GlobalFilterBar() {
  const { charts, updateChart } = useDashboardStore()
  const [dims, setDims] = useState<Record<string, FilterDim>>({})
  const [applyTo, setApplyTo] = useState<"all" | "selected">("all")
  const [applyOpen, setApplyOpen] = useState(false)
  const applyRef = useRef<HTMLDivElement>(null)

  const selected = charts.filter(c => c.selected)

  // Auto-switch applyTo based on selection
  useEffect(() => {
    setApplyTo(selected.length > 0 ? "selected" : "all")
  }, [selected.length])

  useEffect(() => {
    loadDims().then(setDims)
  }, [])

  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (applyRef.current && !applyRef.current.contains(e.target as Node)) setApplyOpen(false)
    }
    document.addEventListener("mousedown", h)
    return () => document.removeEventListener("mousedown", h)
  }, [])

  const targetCards = applyTo === "selected" && selected.length > 0 ? selected : charts

  const applyFilter = useCallback(async (dimKey: string, vals: string[]) => {
    for (const card of targetCards) {
      const newFilters = { ...card.filters }
      if (vals.length === 0) delete newFilters[dimKey]
      else newFilters[dimKey] = vals.length === 1 ? vals[0] : vals

      updateChart(card.id, { loading: true, filters: newFilters })
      try {
        const sourceSql = card.base_sql || card.sql || ""
        if (sourceSql) {
          const result = await rerenderChart(sourceSql, card.chart_type, card.title, card.category, newFilters)
          updateChart(card.id, {
            chart_data: result.chart,
            sql: result.sql || sourceSql,
            filters: newFilters,
            loading: false,
          })
        } else {
          updateChart(card.id, { filters: newFilters, loading: false })
        }
      } catch {
        updateChart(card.id, { loading: false })
      }
    }
  }, [targetCards, updateChart])

  const clearAll = useCallback(async () => {
    for (const card of targetCards) {
      updateChart(card.id, { loading: true, filters: {} })
      try {
        const sourceSql = card.base_sql || card.sql || ""
        if (sourceSql) {
          const result = await rerenderChart(sourceSql, card.chart_type, card.title, card.category, {})
          updateChart(card.id, { chart_data: result.chart, sql: result.sql || sourceSql, filters: {}, loading: false })
        } else {
          updateChart(card.id, { filters: {}, loading: false })
        }
      } catch {
        updateChart(card.id, { loading: false })
      }
    }
  }, [targetCards, updateChart])

  // Count total active filters across target cards
  const totalActive = targetCards.reduce((sum, c) =>
    sum + Object.keys(c.filters || {}).filter(k => c.filters[k] && c.filters[k] !== "").length, 0)

  if (charts.length === 0) return null

  return (
    <div style={{
      flexShrink: 0, padding: "8px 18px",
      background: "linear-gradient(135deg, #0F172A 0%, #1E293B 100%)",
      borderBottom: "1px solid rgba(255,255,255,0.06)",
      display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap",
      boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
    }}>

      {/* Apply to selector */}
      <div ref={applyRef} style={{ position: "relative", flexShrink: 0 }}>
        <button onClick={() => setApplyOpen(o => !o)} style={{
          display: "flex", alignItems: "center", gap: 5, padding: "5px 10px",
          borderRadius: 7, fontSize: 11, fontWeight: 600, cursor: "pointer",
          border: "1px solid rgba(56,189,248,0.3)",
          background: "rgba(56,189,248,0.1)", color: "#38BDF8",
          whiteSpace: "nowrap",
        }}>
          <span>Apply to:</span>
          <span>{applyTo === "all" ? "All cards" : `${selected.length} selected`}</span>
          <ChevronDown size={9} />
        </button>
        {applyOpen && (
          <div style={{
            position: "absolute", top: "calc(100% + 6px)", left: 0, zIndex: 9999,
            background: "rgba(15,23,42,0.98)", borderRadius: 8,
            boxShadow: "0 8px 24px rgba(0,0,0,0.4)",
            border: "1px solid rgba(255,255,255,0.1)", minWidth: 160, padding: "4px 0",
          }}>
            {[
              { key: "all", label: `All cards (${charts.length})` },
              { key: "selected", label: `Selected cards (${selected.length})`, disabled: selected.length === 0 },
            ].map(({ key, label, disabled }) => (
              <div key={key}
                onClick={() => { if (!disabled) { setApplyTo(key as any); setApplyOpen(false) } }}
                style={{
                  padding: "8px 14px", fontSize: 12, cursor: disabled ? "not-allowed" : "pointer",
                  color: disabled ? "#334155" : applyTo === key ? "#38BDF8" : "#94A3B8",
                  background: applyTo === key ? "rgba(56,189,248,0.08)" : "transparent",
                  fontWeight: applyTo === key ? 600 : 400,
                }}
                onMouseEnter={e => { if (!disabled && applyTo !== key) e.currentTarget.style.background = "rgba(255,255,255,0.04)" }}
                onMouseLeave={e => { e.currentTarget.style.background = applyTo === key ? "rgba(56,189,248,0.08)" : "transparent" }}
              >
                {label}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Divider */}
      <div style={{ width: 1, height: 20, background: "rgba(255,255,255,0.1)", flexShrink: 0 }} />

      {/* Filter dropdowns */}
      {Object.entries(dims).map(([key, dim]) => (
        <SearchableDropdown
          key={key}
          dimKey={key}
          dim={dim}
          selectedCards={targetCards}
          onApply={applyFilter}
          color="#38BDF8"
        />
      ))}

      {/* Clear all */}
      {totalActive > 0 && (
        <>
          <div style={{ width: 1, height: 20, background: "rgba(255,255,255,0.1)", flexShrink: 0 }} />
          <button onClick={clearAll} style={{
            display: "flex", alignItems: "center", gap: 5, padding: "5px 10px",
            borderRadius: 7, fontSize: 11, fontWeight: 500, cursor: "pointer",
            border: "1px solid rgba(239,68,68,0.3)",
            background: "rgba(239,68,68,0.08)", color: "#F87171",
            whiteSpace: "nowrap", flexShrink: 0,
          }}>
            <X size={10} />
            <span>Clear all ({totalActive})</span>
          </button>
        </>
      )}
    </div>
  )
}
