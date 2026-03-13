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
    <div className="atlas-layout">
      <AIPanel />
      <BIPanel />
    </div>
  )
}
