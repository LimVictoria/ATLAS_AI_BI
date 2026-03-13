import type { Metadata } from "next"
import "./globals.css"

export const metadata: Metadata = {
  title: "ATLAS — Advanced Transport & Logistics Analytics System",
  description: "Every decision, grounded.",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
