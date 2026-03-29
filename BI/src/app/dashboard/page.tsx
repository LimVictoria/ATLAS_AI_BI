"use client"
import { useEffect, useState } from "react"
import { v4 as uuid } from "uuid"
import { useDashboardStore } from "@/store/dashboard"
import AIPanel from "@/components/AIPanel"
import BIPanel from "@/components/BIPanel"

export default function DashboardPage() {
  const { sessionId, setSessionId } = useDashboardStore()
  // Use state + useEffect to generate uuid ONLY on client
  // Never on server — uuid() produces different values each call
  // causing React hydration mismatch (#418/#423/#425)
  const [mounted, setMounted] = useState(false)
  useEffect(() => {
    setMounted(true)
    if (!sessionId) setSessionId(uuid())
  }, [])

  if (!mounted) {
    // Server render + first client paint: render empty shell, no dynamic content
    return (
      <div style={{
        display: "flex", flexDirection: "row",
        height: "100vh", width: "100vw",
        overflow: "hidden", background: "#F4F6F9",
        position: "fixed", top: 0, left: 0,
      }} />
    )
  }

  return (
    <div style={{
      display: "flex", flexDirection: "row",
      height: "100vh", width: "100vw",
      overflow: "hidden", background: "#F4F6F9",
      position: "fixed", top: 0, left: 0,
    }}>
      <BIPanel />
      <AIPanel />
    </div>
  )
}
