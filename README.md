# ATLAS — Advanced Transport & Logistics Analytics System

> Every decision, grounded.

## Structure

```
ATLAS/
├── AI/          FastAPI backend — LangGraph agent, semantic layer, DuckDB
└── BI/          Next.js frontend — split chat + visual canvas
```

## Stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 14 + shadcn/ui + Tailwind CSS |
| State | Zustand |
| Charts | Plotly.js |
| Canvas | react-grid-layout |
| Backend | FastAPI + LangGraph |
| LLM | Groq — llama-3.3-70b-versatile |
| Query engine | DuckDB |
| Database | Supabase (PostgreSQL) |
| Frontend deploy | Vercel |
| Backend deploy | Render |

## Quick Start

```bash
# Backend
cd AI
pip install -r requirements.txt
uvicorn main:app --reload

# Frontend
cd BI
npm install
npm run dev
```

## Metrics
28 pre-built metrics across Cost, Downtime, Failure, Fleet, Workshop, Time categories.
See `AI/metrics.py` for the full semantic layer.
