"use client"
import { useState, useRef, useEffect, useCallback } from "react"
import { Send, Loader, Sparkles, Clock } from "lucide-react"
import { v4 as uuid } from "uuid"
import { useDashboardStore } from "@/store/dashboard"
import { sendChat, clearChatHistory, getDQWarnings } from "@/utils/api"
import { processUIActions } from "@/hooks/useUIActions"

const SUGGESTIONS = [
  "Show total cost by brand",
  "Components within the cost — stacked",
  "Which components fail most?",
  "Show failure trend by quarter",
  "Cost vs downtime scatter",
  "Cost heatmap by brand & month",
  "Waterfall chart of cost by category",
  "Cost distribution boxplot",
  "Show last 12 months trend",
  "How many charts do I have?",
]

// Accent colours cycling through categories
const MSG_ACCENTS = ["#3B82F6","#7C3AED","#059669","#D97706","#0891B2","#DC2626"]

export default function AIPanel() {
  const { sessionId, messages, addMessage, updateMessage, clearMessages, charts, loadBoardFromServer, loadMessagesFromServer } = useDashboardStore()
  const [input, setInput] = useState("")
  const [loading, setLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  // Load board and chat history on first mount
  useEffect(() => {
    loadBoardFromServer()
    loadMessagesFromServer()
    // Load data quality warnings once per session
    getDQWarnings().then(data => {
      const warnings: string[] = data?.warnings || []
      if (warnings.length > 0) {
        addMessage({
          id: "dq-warning",
          role: "system",
          text: ["\u26a0\ufe0f Data Quality Notice", ...warnings.map((w: string) => "\u00b7 " + w)].join("\n"),
          timestamp: new Date(),
          loading: false,
        })
      }
    }).catch(() => {})
  }, [])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages])

  const buildHistory = useCallback(() =>
    messages.filter(m => !m.loading).slice(-10)
      .map(m => ({ role: m.role, content: m.text })),
    [messages]
  )

  const buildBoardContext = useCallback(() => ({
    charts_on_canvas: charts.map(c => ({
      id: c.id, title: c.title, metric_id: c.metric_id,
      chart_type: c.chart_type, filters: c.filters || {},
      selected: c.selected || false,
      sql: c.sql || "",
    })),
    selected_ids: charts.filter(c => c.selected).map(c => c.id),
  }), [charts])

  const send = useCallback(async (text: string) => {
    if (!text.trim() || loading || !sessionId) return
    setInput("")
    setLoading(true)
    addMessage({ id: uuid(), role: "user", text, timestamp: new Date() })
    const loadingId = uuid()
    addMessage({ id: loadingId, role: "assistant", text: "", timestamp: new Date(), loading: true })
    try {
      const resp = await sendChat(sessionId, text, buildHistory(), buildBoardContext())
      updateMessage(loadingId, { loading: false, text: resp.narrative || "Done." })
      if (resp.ui_actions?.length) await processUIActions(resp.ui_actions)
    } catch (err: any) {
      const is429 = err?.status === 429 || (err?.message || "").includes("429") || (err?.message || "").includes("rate limit")
      const msg = is429
        ? "⏳ Groq is rate-limited right now — wait a few seconds and try again."
        : "Something went wrong. Please try again."
      updateMessage(loadingId, { loading: false, text: msg })
    } finally {
      setLoading(false)
    }
  }, [loading, sessionId, addMessage, updateMessage, buildHistory, buildBoardContext])

  const handleClear = async () => {
    clearMessages()
    if (sessionId) await clearChatHistory(sessionId).catch(() => {})
  }

  const selectedCard = charts.find(c => c.selected)
  const [now, setNow] = useState(new Date())
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(t)
  }, [])
  const fmtTime = (d: Date) => d.toLocaleTimeString("en-MY", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false })
  const fmtDate = (d: Date) => d.toLocaleDateString("en-MY", { weekday: "short", day: "2-digit", month: "short", year: "numeric" })

  return (
    <div style={{
      width: 400, flexShrink: 0, display: "flex", flexDirection: "column",
      height: "100vh", background: "#FFFFFF",
      borderLeft: "1px solid #E8ECF0", overflow: "hidden",
    }}>
      {/* ── Header ── */}
      <div style={{
        flexShrink: 0, padding: "16px 18px 14px",
        background: "linear-gradient(135deg, #0F172A 0%, #1E293B 70%, #0c2340 100%)",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        boxShadow: "0 2px 12px rgba(0,0,0,0.2)",
      }}>
        {/* Logo row */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{
              width: 34, height: 34, borderRadius: 10,
              background: "linear-gradient(135deg, #0EA5E9, #2563EB)",
              display: "flex", alignItems: "center", justifyContent: "center",
              boxShadow: "0 2px 10px rgba(14,165,233,0.4)",
              flexShrink: 0,
            }}>
              <Sparkles size={16} color="#fff" />
            </div>
            <div>
              <div style={{
                fontFamily: "'IBM Plex Mono', monospace", fontWeight: 800,
                fontSize: 18, color: "#F1F5F9", letterSpacing: "0.12em", lineHeight: 1,
              }}>
                ATLAS
              </div>
              <div style={{ fontSize: 11, color: "#38BDF8", fontWeight: 500, letterSpacing: "0.08em", marginTop: 2 }}>
                AI Chat
              </div>
            </div>
          </div>

          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            {charts.length > 0 && (
              <div style={{ textAlign: "right" }}>
                <div style={{ fontSize: 11, color: "#94A3B8", fontWeight: 500 }}>
                  {charts.length} chart{charts.length !== 1 ? "s" : ""}
                  {charts.filter(c => c.selected).length > 0 && (
                    <span style={{ color: "#38BDF8", marginLeft: 6 }}>
                      · {charts.filter(c => c.selected).length} selected
                    </span>
                  )}
                </div>
              </div>
            )}

          </div>
        </div>

        {/* Tagline */}
        <div style={{
          marginTop: 10, fontSize: 12, color: "#475569", lineHeight: 1.5,
          fontStyle: "italic",
        }}>
          Every decision, grounded.
        </div>

        {/* Datetime */}
        <div style={{ marginTop: 10, display: "flex", alignItems: "center", gap: 8 }}>
          <Clock size={11} color="#475569" />
          <span style={{ fontSize: 11, color: "#38BDF8", fontWeight: 600, letterSpacing: "0.04em", fontFamily: "'IBM Plex Mono', monospace" }}>
            {fmtTime(now)}
          </span>
          <span style={{ fontSize: 10, color: "#475569", fontWeight: 400 }}>
            {fmtDate(now)}
          </span>
        </div>
      </div>

      {/* ── Suggestions ── */}
      {/* ── Persistent suggestions ── */}
      <div style={{ padding: "10px 14px 6px", flexShrink: 0, borderBottom: "1px solid #F1F5F9" }}>
        <div style={{ fontSize: 11, fontWeight: 600, color: "#94A3B8", letterSpacing: "0.06em", marginBottom: 6 }}>QUICK ASKS</div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
          {SUGGESTIONS.map((s, i) => (
            <button key={s} onClick={() => send(s)} style={{
              padding: "4px 9px",
              border: `1px solid ${MSG_ACCENTS[i % MSG_ACCENTS.length]}28`,
              borderRadius: 99, fontSize: 12,
              color: MSG_ACCENTS[i % MSG_ACCENTS.length],
              background: `${MSG_ACCENTS[i % MSG_ACCENTS.length]}07`,
              cursor: "pointer", transition: "all 0.12s", whiteSpace: "nowrap", fontWeight: 500,
            }}
              onMouseEnter={e => {
                e.currentTarget.style.background = `${MSG_ACCENTS[i % MSG_ACCENTS.length]}16`
                e.currentTarget.style.borderColor = `${MSG_ACCENTS[i % MSG_ACCENTS.length]}55`
              }}
              onMouseLeave={e => {
                e.currentTarget.style.background = `${MSG_ACCENTS[i % MSG_ACCENTS.length]}07`
                e.currentTarget.style.borderColor = `${MSG_ACCENTS[i % MSG_ACCENTS.length]}28`
              }}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {/* ── Messages ── */}
      <div style={{ flex: 1, overflowY: "auto", padding: "12px 14px", display: "flex", flexDirection: "column", gap: 8 }}>
        {messages.map((msg, idx) => (
          <div key={msg.id} style={{
            maxWidth: "90%",
            alignSelf: msg.role === "user" ? "flex-end" : "flex-start",
          }}>
            {msg.loading ? (
              <div style={{
                padding: "10px 14px", background: "#F8FAFC",
                borderRadius: "12px 12px 12px 3px", border: "1px solid #F1F5F9",
              }}>
                <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                  {[0, 1, 2].map(i => (
                    <div key={i} style={{
                      width: 6, height: 6, borderRadius: "50%", background: "#94A3B8",
                      animation: "typing 1.4s infinite ease-in-out",
                      animationDelay: `${i * 0.2}s`,
                    }} />
                  ))}
                </div>
              </div>
            ) : msg.role === "system" ? (
              <div style={{
                padding: "10px 14px",
                background: "linear-gradient(135deg, #FEF9C3, #FEF3C7)",
                color: "#78350F",
                borderRadius: 10,
                border: "1px solid #FDE68A",
                fontSize: 12, lineHeight: 1.7, whiteSpace: "pre-wrap",
                boxShadow: "0 1px 4px rgba(251,191,36,0.15)",
                maxWidth: "100%",
              }}>
                {msg.text}
              </div>
            ) : (
              <div style={{
                padding: "9px 13px",
                background: msg.role === "user"
                  ? `linear-gradient(135deg, #0EA5E9, #2563EB)`
                  : "#F8FAFC",
                color: msg.role === "user" ? "#FFFFFF" : "#1E293B",
                borderRadius: msg.role === "user" ? "12px 12px 3px 12px" : "12px 12px 12px 3px",
                border: msg.role === "user" ? "none" : "1px solid #F1F5F9",
                fontSize: 14, lineHeight: 1.7, whiteSpace: "pre-wrap",
                boxShadow: msg.role === "user"
                  ? "0 2px 8px rgba(14,165,233,0.25)"
                  : "0 1px 3px rgba(0,0,0,0.04)",
              }}>
                {msg.text}
              </div>
            )}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      {/* ── Selected card indicator ── */}
      {selectedCard && (
        <div style={{
          margin: "0 14px 6px",
          fontSize: 12, color: "#0284C7",
          background: "linear-gradient(135deg, #EFF6FF, #F0F9FF)",
          border: "1px solid #BFDBFE", borderRadius: 8,
          padding: "5px 12px", display: "flex", alignItems: "center", gap: 7,
          boxShadow: "0 1px 4px rgba(37,99,235,0.08)",
        }}>
          <span style={{
            width: 7, height: 7, borderRadius: "50%", background: "#0284C7",
            flexShrink: 0, display: "inline-block",
            boxShadow: "0 0 0 2px rgba(2,132,199,0.25)",
          }} />
          <span style={{ fontWeight: 600 }}>"{selectedCard.title}"</span>
          <span style={{ color: "#64748B" }}>selected — ask to modify it</span>
        </div>
      )}

      {/* ── Input ── */}
      <div style={{ padding: "6px 14px 14px", borderTop: "1px solid #F1F5F9", background: "#FFFFFF", flexShrink: 0 }}>
        <div style={{ position: "relative" }}>
          <textarea
            ref={textareaRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(input) } }}
            placeholder="Ask anything about fleet maintenance..."
            rows={2}
            disabled={loading}
            style={{
              width: "100%", padding: "9px 44px 9px 12px",
              border: "1px solid #E2E8F0", borderRadius: 10,
              fontSize: 14, outline: "none", resize: "none",
              fontFamily: "Inter, system-ui, sans-serif",
              color: "#1E293B", background: loading ? "#F8FAFC" : "#FFFFFF",
              boxSizing: "border-box", transition: "border-color 0.15s, box-shadow 0.15s",
            }}
            onFocus={e => { e.target.style.borderColor = "#0EA5E9"; e.target.style.boxShadow = "0 0 0 3px rgba(14,165,233,0.12)" }}
            onBlur={e => { e.target.style.borderColor = "#E2E8F0"; e.target.style.boxShadow = "none" }}
          />
          <button
            onClick={() => send(input)}
            disabled={loading || !input.trim()}
            style={{
              position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)",
              background: loading || !input.trim()
                ? "#E2E8F0"
                : "linear-gradient(135deg, #0EA5E9, #2563EB)",
              color: loading || !input.trim() ? "#94A3B8" : "#FFFFFF",
              border: "none", borderRadius: 8,
              width: 30, height: 30, display: "flex", alignItems: "center", justifyContent: "center",
              cursor: loading || !input.trim() ? "not-allowed" : "pointer",
              transition: "all 0.15s",
              boxShadow: loading || !input.trim() ? "none" : "0 2px 8px rgba(14,165,233,0.3)",
            }}
          >
            {loading
              ? <Loader size={13} style={{ animation: "spin 1s linear infinite" }} />
              : <Send size={13} />
            }
          </button>
        </div>
        <div style={{ fontSize: 11, color: "#CBD5E1", marginTop: 4, textAlign: "right" }}>
          Shift+Enter for new line
        </div>
      </div>
    </div>
  )
}
