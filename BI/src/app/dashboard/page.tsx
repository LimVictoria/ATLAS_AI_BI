"use client"
import { useEffect } from "react"
import { v4 as uuid } from "uuid"
import { useDashboardStore } from "@/store/dashboard"
import AIPanel from "@/components/AIPanel"
import BIPanel from "@/components/BIPanel"

export default function DashboardPage() {
  const { sessionId, setSessionId } = useDashboardStore()

  useEffect(() => {
    if (!sessionId) setSessionId(uuid())
  }, [sessionId, setSessionId])

  return (
    <div style={{
      display: "flex",
      flexDirection: "row",
      height: "100vh",
      width: "100vw",
      overflow: "hidden",
      background: "#F4F6F9",
      position: "fixed",
      top: 0,
      left: 0,
    }}>
      <BIPanel />
      <AIPanel />
    </div>
  )
}
