"use client"
import { useState, useRef, useEffect, useCallback } from "react"
import { Send, Trash2, Loader } from "lucide-react"
import { v4 as uuid } from "uuid"
import { useDashboardStore } from "@/store/dashboard"
import { sendChat, clearChatHistory } from "@/utils/api"
import { processUIActions } from "@/hooks/useUIActions"

const SUGGESTIONS = [
  "Show total cost by brand",
  "Which components fail most?",
  "Show failure trend by quarter",
  "Cost vs downtime by workshop",
  "Show last 12 months trend",
  "Scheduled vs unscheduled ratio",
  "Add a brand filter",
  "Show YoY cost comparison",
]

export default function AIPanel() {
  const { sessionId, messages, addMessage, updateMessage, clearMessages, charts } = useDashboardStore()
  const [input, setInput] = useState("")
  const [loading, setLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages])

  const buildHistory = useCallback(() =>
    messages.filter(m => !m.loading).slice(-10)
      .map(m => ({ role: m.role, content: m.text })),
    [messages]
  )

  const send = useCallback(async (text: string) => {
    if (!text.trim() || loading || !sessionId) return
    setInput("")
    setLoading(true)

    addMessage({ id: uuid(), role: "user", text, timestamp: new Date() })
    const loadingId = uuid()
    addMessage({ id: loadingId, role: "assistant", text: "", timestamp: new Date(), loading: true })

    try {
      const resp = await sendChat(sessionId, text, buildHistory())
      updateMessage(loadingId, { loading: false, text: resp.narrative || "Done." })
      if (resp.ui_actions?.length) await processUIActions(resp.ui_actions)
    } catch {
      updateMessage(loadingId, { loading: false, text: "Something went wrong. Please try again." })
    } finally {
      setLoading(false)
    }
  }, [loading, sessionId, addMessage, updateMessage, buildHistory])

  const handleClear = async () => {
    clearMessages()
    if (sessionId) await clearChatHistory(sessionId).catch(() => {})
  }

  return (
    <div style={{ width: "380px", flexShrink: 0, display: "flex", flexDirection: "column", height: "100vh", background: "#FFFFFF", borderRight: "1px solid #E8ECF0", overflow: "hidden" }}>
      {/* Header */}
      <div style={{
        height: 52, display: "flex", alignItems: "center",
        padding: "0 16px", borderBottom: "1px solid #f1f5f9",
        background: "#ffffff", gap: 8, flexShrink: 0,
      }}>
        <span style={{ fontFamily: "'IBM Plex Mono', monospace", fontWeight: 700, fontSize: 13, color: "#0284c7", letterSpacing: "0.08em" }}>
          ATLAS
        </span>
        <span style={{ fontSize: 11, color: "#cbd5e1", fontWeight: 400 }}>· AI</span>
        <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
          {charts.length > 0 && (
            <span style={{
              fontSize: 10, color: "#64748b", background: "#f1f5f9",
              padding: "2px 8px", borderRadius: 99, fontWeight: 500,
            }}>
              {charts.length} chart{charts.length !== 1 ? "s" : ""}
            </span>
          )}
          <button onClick={handleClear} title="Clear chat"
            style={{ background: "none", border: "none", cursor: "pointer", color: "#cbd5e1", padding: 4, display: "flex", lineHeight: 1 }}>
            <Trash2 size={13} />
          </button>
        </div>
      </div>

      {/* Suggestions */}
      {messages.length === 0 && (
        <>
          <div style={{ padding: "20px 16px 8px", flexShrink: 0 }}>
            <p style={{ fontSize: 12, color: "#94a3b8", marginBottom: 10, lineHeight: 1.6 }}>
              Ask about fleet maintenance costs, failures, downtime, or workshops.
              Charts appear on the right.
            </p>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
              {SUGGESTIONS.map(s => (
                <button key={s} onClick={() => send(s)}
                  style={{
                    padding: "5px 10px", border: "1px solid #e2e8f0",
                    borderRadius: 99, fontSize: 11, color: "#64748b",
                    background: "#ffffff", cursor: "pointer",
                    transition: "all 0.12s", whiteSpace: "nowrap",
                  }}
                  onMouseEnter={e => { (e.target as HTMLElement).style.borderColor = "#0284c7"; (e.target as HTMLElement).style.color = "#0284c7" }}
                  onMouseLeave={e => { (e.target as HTMLElement).style.borderColor = "#e2e8f0"; (e.target as HTMLElement).style.color = "#64748b" }}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        </>
      )}

      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", padding: "12px 16px", display: "flex", flexDirection: "column", gap: 10 }}>
        {messages.map(msg => (
          <div key={msg.id} style={{
            maxWidth: "88%",
            alignSelf: msg.role === "user" ? "flex-end" : "flex-start",
          }}>
            {msg.loading ? (
              <div style={{
                padding: "10px 14px", background: "#f8fafc",
                borderRadius: "12px 12px 12px 3px",
                border: "1px solid #f1f5f9",
              }}>
                <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                  {[0, 1, 2].map(i => (
                    <div key={i} style={{
                      width: 6, height: 6, borderRadius: "50%", background: "#94a3b8",
                      animation: "typing 1.4s infinite ease-in-out",
                      animationDelay: `${i * 0.2}s`,
                    }} />
                  ))}
                </div>
              </div>
            ) : (
              <div style={{
                padding: "9px 13px",
                background: msg.role === "user" ? "#0284c7" : "#f8fafc",
                color: msg.role === "user" ? "#ffffff" : "#1e293b",
                borderRadius: msg.role === "user" ? "12px 12px 3px 12px" : "12px 12px 12px 3px",
                border: msg.role === "user" ? "none" : "1px solid #f1f5f9",
                fontSize: 13, lineHeight: 1.6,
              }}>
                {msg.text}
              </div>
            )}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div style={{ padding: "10px 16px 14px", borderTop: "1px solid #f1f5f9", background: "#ffffff", flexShrink: 0 }}>
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
              border: "1px solid #e2e8f0", borderRadius: 8,
              fontSize: 13, outline: "none", resize: "none",
              fontFamily: "Inter, system-ui, sans-serif",
              color: "#1e293b", background: loading ? "#f8fafc" : "#ffffff",
              boxSizing: "border-box",
              transition: "border-color 0.15s",
            }}
            onFocus={e => e.target.style.borderColor = "#0284c7"}
            onBlur={e => e.target.style.borderColor = "#e2e8f0"}
          />
          <button
            onClick={() => send(input)}
            disabled={loading || !input.trim()}
            style={{
              position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)",
              background: loading || !input.trim() ? "#e2e8f0" : "#0284c7",
              color: loading || !input.trim() ? "#94a3b8" : "#ffffff",
              border: "none", borderRadius: 6,
              width: 30, height: 30, display: "flex", alignItems: "center", justifyContent: "center",
              cursor: loading || !input.trim() ? "not-allowed" : "pointer",
              transition: "all 0.15s",
            }}
          >
            {loading ? <Loader size={13} style={{ animation: "spin 1s linear infinite" }} /> : <Send size={13} />}
          </button>
        </div>
        <div style={{ fontSize: 10, color: "#cbd5e1", marginTop: 4, textAlign: "right" }}>
          Shift+Enter for new line
        </div>
      </div>
    </div>
  )
}
