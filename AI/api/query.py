"""
ATLAS BI — /query endpoint
"""
import json
import plotly.graph_objects as go
import plotly.utils
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from db.duckdb_session import run_query
from metrics import METRICS, get_metric_sql

router = APIRouter(prefix="/query", tags=["query"])

# ── Design tokens ─────────────────────────────────────────────────────────────
PALETTES = {
    "Cost":     ["#1D4ED8","#2563EB","#3B82F6","#60A5FA","#93C5FD","#BFDBFE"],
    "Downtime": ["#5B21B6","#6D28D9","#7C3AED","#8B5CF6","#A78BFA","#C4B5FD"],
    "Failure":  ["#991B1B","#B91C1C","#DC2626","#EF4444","#F87171","#FCA5A5"],
    "Fleet":    ["#064E3B","#065F46","#047857","#059669","#10B981","#34D399"],
    "Workshop": ["#78350F","#92400E","#B45309","#D97706","#F59E0B","#FCD34D"],
    "Time":     ["#0C4A6E","#0E7490","#0891B2","#06B6D4","#22D3EE","#67E8F9"],
    "General":  ["#0F172A","#1E293B","#334155","#475569","#64748B","#94A3B8"],
}

BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, system-ui, -apple-system, sans-serif", color="#334155", size=11),
    margin=dict(t=12, r=20, b=50, l=62),
    xaxis=dict(
        gridcolor="#F1F5F9", gridwidth=1, zeroline=False,
        linecolor="#E2E8F0", tickfont=dict(size=10, color="#64748B"),
        title_font=dict(size=11, color="#94A3B8"),
    ),
    yaxis=dict(
        gridcolor="#F1F5F9", gridwidth=1, zeroline=False,
        linecolor="#E2E8F0", tickfont=dict(size=10, color="#64748B"),
        title_font=dict(size=11, color="#94A3B8"),
    ),
    hoverlabel=dict(
        bgcolor="#1E293B", font_color="#F8FAFC",
        font_family="Inter, sans-serif", font_size=12,
        bordercolor="#1E293B",
    ),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.0,
        xanchor="right", x=1,
        font=dict(size=10, color="#64748B"),
        bgcolor="rgba(0,0,0,0)",
    ),
    bargap=0.28,
)


def _palette(category: str, n: int) -> list:
    base = PALETTES.get(category, PALETTES["General"])
    # cycle if more data points than colours
    return [base[i % len(base)] for i in range(n)]


def _to_json(fig) -> str:
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def _build_chart(df, metric: dict, chart_type: str) -> str:
    x_col = metric.get("x_col")
    y_col = metric.get("y_col")
    cat   = metric.get("category", "General")
    n     = len(df)

    # ── Bar ──────────────────────────────────────────────────────────────────
    if chart_type == "bar":
        colors = _palette(cat, n)
        fig = go.Figure(go.Bar(
            x=df[x_col], y=df[y_col],
            marker=dict(
                color=colors,
                cornerradius=5,
                line=dict(width=0),
            ),
            hovertemplate=f"<b>%{{x}}</b><br>{y_col.replace('_',' ').title()}: <b>%{{y:,.1f}}</b><extra></extra>",
        ))
        fig.update_layout(**BASE)

    # ── Line ─────────────────────────────────────────────────────────────────
    elif chart_type == "line":
        color = PALETTES.get(cat, PALETTES["General"])[1]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df[y_col],
            mode="lines+markers",
            line=dict(color=color, width=2.5, shape="spline", smoothing=0.8),
            marker=dict(size=5, color=color, line=dict(color="#FFFFFF", width=1.5)),
            fill="tozeroy",
            fillcolor=f"rgba({r},{g},{b},0.07)",
            hovertemplate=f"<b>%{{x}}</b><br>{y_col.replace('_',' ').title()}: <b>%{{y:,.1f}}</b><extra></extra>",
        ))
        fig.update_layout(**BASE)

    # ── Pie / Donut ───────────────────────────────────────────────────────────
    elif chart_type == "pie":
        colors = _palette(cat, n)
        fig = go.Figure(go.Pie(
            labels=df[x_col], values=df[y_col],
            marker=dict(colors=colors, line=dict(color="#FFFFFF", width=2.5)),
            hole=0.48,
            textinfo="percent",
            textfont=dict(size=10, color="#FFFFFF"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f}<br><b>%{percent}</b><extra></extra>",
            sort=True,
            direction="clockwise",
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, system-ui, sans-serif", color="#334155", size=11),
            margin=dict(t=12, r=16, b=12, l=16),
            showlegend=True,
            legend=dict(
                orientation="v", yanchor="middle", y=0.5,
                xanchor="left", x=1.02,
                font=dict(size=11, color="#475569"),
                bgcolor="rgba(0,0,0,0)",
            ),
            hoverlabel=dict(bgcolor="#1E293B", font_color="#F8FAFC", font_size=12),
        )

    # ── Table ─────────────────────────────────────────────────────────────────
    elif chart_type == "table":
        col_names = df.columns.tolist()
        header_vals = [f"<b>{c.replace('_', ' ').title()}</b>" for c in col_names]
        row_fill = ["#F8FAFC" if i % 2 == 0 else "#FFFFFF" for i in range(n)]
        fig = go.Figure(go.Table(
            header=dict(
                values=header_vals,
                fill_color="#1E293B",
                font=dict(color="#F8FAFC", size=11, family="Inter, sans-serif"),
                align="left", height=36,
                line=dict(width=0),
            ),
            cells=dict(
                values=[df[c].tolist() for c in col_names],
                fill_color=[row_fill] * len(col_names),
                font=dict(color="#334155", size=11, family="Inter, sans-serif"),
                align="left", height=32,
                line=dict(color="#F1F5F9", width=1),
                format=[None if df[c].dtype == object else ",.2f" for c in col_names],
            ),
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=4, r=4, b=4, l=4),
        )

    # ── Pareto ────────────────────────────────────────────────────────────────
    elif chart_type == "pareto":
        df_sorted = df.sort_values(by=y_col, ascending=False).reset_index(drop=True)
        total = df_sorted[y_col].sum()
        df_sorted["cumulative_pct"] = df_sorted[y_col].cumsum() / total * 100
        color = PALETTES.get(cat, PALETTES["General"])[1]
        colors = _palette(cat, len(df_sorted))

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_sorted[x_col], y=df_sorted[y_col],
            name=y_col.replace("_", " ").title(),
            marker=dict(color=colors, cornerradius=5, line=dict(width=0)),
            hovertemplate=f"<b>%{{x}}</b><br>Value: <b>%{{y:,.1f}}</b><extra></extra>",
            yaxis="y",
        ))
        fig.add_trace(go.Scatter(
            x=df_sorted[x_col], y=df_sorted["cumulative_pct"],
            name="Cumulative %",
            mode="lines+markers",
            line=dict(color="#94A3B8", width=2, dash="dot"),
            marker=dict(size=5, color="#94A3B8"),
            hovertemplate="<b>%{x}</b><br>Cumulative: <b>%{y:.1f}%</b><extra></extra>",
            yaxis="y2",
        ))
        # 80% reference line
        fig.add_hline(y=80, yref="y2", line=dict(color="#EF4444", width=1, dash="dash"),
                      annotation_text="80%", annotation_position="right",
                      annotation_font=dict(color="#EF4444", size=10))

        pareto_layout = {**BASE}
        pareto_layout["yaxis"] = {**pareto_layout.get("yaxis", {}),
                                   "title": y_col.replace("_", " ").title(),
                                   "side": "left"}
        pareto_layout["yaxis2"] = dict(
            title="Cumulative %", overlaying="y", side="right",
            range=[0, 110], ticksuffix="%",
            gridcolor="rgba(0,0,0,0)", zeroline=False,
            tickfont=dict(size=10, color="#94A3B8"),
            title_font=dict(size=11, color="#94A3B8"),
        )
        pareto_layout["legend"] = dict(
            orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1,
            font=dict(size=10), bgcolor="rgba(0,0,0,0)",
        )
        fig.update_layout(**pareto_layout)

    else:
        # Fallback to bar
        return _build_chart(df, metric, "bar")

    return _to_json(fig)


class QueryRequest(BaseModel):
    metric_id: str
    chart_type: Optional[str] = None
    filters: Optional[dict] = {}


@router.post("/")
def run_metric(req: QueryRequest):
    metric = METRICS.get(req.metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{req.metric_id}' not found")

    sql = get_metric_sql(req.metric_id, req.filters or {})
    if not sql:
        raise HTTPException(status_code=400, detail="Could not build SQL")

    try:
        df = run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

    chart_type = req.chart_type or metric["default_chart"]
    # Ensure pareto is valid for this metric — fallback to bar if not
    available = metric.get("available_charts", ["bar"])
    if chart_type not in available and chart_type != "pareto":
        chart_type = metric["default_chart"]

    chart_json = _build_chart(df, metric, chart_type)
    return {
        "metric_id":        req.metric_id,
        "title":            metric["title"],
        "category":         metric["category"],
        "chart_type":       chart_type,
        "chart":            chart_json,
        "row_count":        len(df),
        "summary":          df.head(5).to_dict(orient="records"),
        "available_charts": list(set(available + ["pareto"])),
        "sql":              sql,
    }


@router.get("/metrics")
def list_metrics():
    from metrics import get_metrics_index
    return {"metrics": get_metrics_index()}
