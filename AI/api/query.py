"""
ATLAS BI — /query endpoint
"""
import json
import plotly.graph_objects as go
import plotly.utils
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from db.duckdb_session import run_query
from metrics import METRICS, get_metric_sql

router = APIRouter(prefix="/query", tags=["query"])

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
    font=dict(family="Inter, system-ui, -apple-system, sans-serif", color="#1E293B", size=12),
    margin=dict(t=12, r=20, b=60, l=70),
    xaxis=dict(
        gridcolor="#F1F5F9", gridwidth=1, zeroline=False,
        linecolor="#E2E8F0", tickfont=dict(size=11, color="#1E293B"),
        title_font=dict(size=12, color="#0F172A", family="Inter, system-ui, sans-serif"),
        title_standoff=12,
    ),
    yaxis=dict(
        gridcolor="#F1F5F9", gridwidth=1, zeroline=False,
        linecolor="#E2E8F0", tickfont=dict(size=11, color="#1E293B"),
        title_font=dict(size=12, color="#0F172A", family="Inter, system-ui, sans-serif"),
        title_standoff=12,
    ),
    hoverlabel=dict(
        bgcolor="#1E293B", font_color="#F8FAFC",
        font_family="Inter, sans-serif", font_size=12,
        bordercolor="#1E293B",
    ),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.0,
        xanchor="right", x=1,
        font=dict(size=11, color="#1E293B"),
        bgcolor="rgba(0,0,0,0)",
    ),
    bargap=0.28,
)


def _palette(category: str, n: int) -> list:
    base = PALETTES.get(category, PALETTES["General"])
    return [base[i % len(base)] for i in range(n)]


def _to_json(fig) -> str:
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Fix NaN in string columns — DuckDB sometimes returns nulls as float NaN"""
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna("—").astype(str)
    return df


def _build_chart(df: pd.DataFrame, metric: dict, chart_type: str) -> str:
    df = _clean_df(df)
    x_col = metric.get("x_col")
    y_col = metric.get("y_col")
    cat   = metric.get("category", "General")
    n     = len(df)

    # ── Bar ──────────────────────────────────────────────────────────────────
    if chart_type == "bar":
        colors = _palette(cat, n)
        fig = go.Figure(go.Bar(
            x=df[x_col], y=df[y_col],
            marker=dict(color=colors, cornerradius=5, line=dict(width=0)),
            hovertemplate=f"<b>%{{x}}</b><br>{y_col.replace('_',' ').title()}: <b>%{{y:,.1f}}</b><extra></extra>",
        ))
        fig.update_layout(**BASE)
        fig.update_layout(
            xaxis_title=x_col.replace("_", " ").title() if x_col else "",
            yaxis_title=y_col.replace("_", " ").title() if y_col else "",
        )

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
        fig.update_layout(
            xaxis_title=x_col.replace("_", " ").title() if x_col else "",
            yaxis_title=y_col.replace("_", " ").title() if y_col else "",
        )

    # ── Pie / Donut ───────────────────────────────────────────────────────────
    elif chart_type == "pie":
        colors = _palette(cat, n)
        fig = go.Figure(go.Pie(
            labels=df[x_col], values=df[y_col],
            marker=dict(colors=colors, line=dict(color="#FFFFFF", width=2.5)),
            hole=0.48, textinfo="percent",
            textfont=dict(size=10, color="#FFFFFF"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f}<br><b>%{percent}</b><extra></extra>",
            sort=True, direction="clockwise",
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, system-ui, sans-serif", color="#334155", size=11),
            margin=dict(t=12, r=16, b=12, l=16),
            showlegend=True,
            legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.02,
                        font=dict(size=11, color="#475569"), bgcolor="rgba(0,0,0,0)"),
            hoverlabel=dict(bgcolor="#1E293B", font_color="#F8FAFC", font_size=12),
        )

    # ── Table ─────────────────────────────────────────────────────────────────
    elif chart_type == "table":
        col_names = df.columns.tolist()
        header_vals = [f"<b>{c.replace('_', ' ').title()}</b>" for c in col_names]
        row_fill = ["#F8FAFC" if i % 2 == 0 else "#FFFFFF" for i in range(n)]
        # Safe format: only apply number format to genuinely numeric columns
        cell_formats = []
        for c in col_names:
            if pd.api.types.is_numeric_dtype(df[c]) and df[c].dtype != object:
                cell_formats.append(",.2f")
            else:
                cell_formats.append(None)
        fig = go.Figure(go.Table(
            header=dict(
                values=header_vals,
                fill_color="#1E293B",
                font=dict(color="#F8FAFC", size=11, family="Inter, sans-serif"),
                align="left", height=36, line=dict(width=0),
            ),
            cells=dict(
                values=[df[c].tolist() for c in col_names],
                fill_color=[row_fill] * len(col_names),
                font=dict(color="#334155", size=11, family="Inter, sans-serif"),
                align="left", height=32,
                line=dict(color="#F1F5F9", width=1),
                format=cell_formats,
            ),
        ))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", margin=dict(t=4, r=4, b=4, l=4))

    # ── Pareto ────────────────────────────────────────────────────────────────
    elif chart_type == "pareto":
        df_sorted = df.sort_values(by=y_col, ascending=False).reset_index(drop=True)
        total = df_sorted[y_col].sum()
        df_sorted["cumulative_pct"] = df_sorted[y_col].cumsum() / total * 100
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
        fig.add_hline(y=80, yref="y2", line=dict(color="#EF4444", width=1, dash="dash"),
                      annotation_text="80%", annotation_position="right",
                      annotation_font=dict(color="#EF4444", size=10))
        pareto_layout = {**BASE}
        pareto_layout["yaxis"] = {**pareto_layout.get("yaxis", {}), "title": y_col.replace("_", " ").title(), "side": "left"}
        pareto_layout["yaxis2"] = dict(title="Cumulative %", overlaying="y", side="right",
                                        range=[0, 110], ticksuffix="%", gridcolor="rgba(0,0,0,0)",
                                        zeroline=False, tickfont=dict(size=10, color="#94A3B8"),
                                        title_font=dict(size=11, color="#94A3B8"))
        pareto_layout["legend"] = dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1,
                                        font=dict(size=10), bgcolor="rgba(0,0,0,0)")
        fig.update_layout(**pareto_layout)

    # ── Waterfall ─────────────────────────────────────────────────────────────
    elif chart_type == "waterfall":
        color = PALETTES.get(cat, PALETTES["General"])[1]
        total = df[y_col].sum()
        measures = ["relative"] * len(df) + ["total"]
        x_vals = df[x_col].tolist() + ["Total"]
        y_vals = df[y_col].tolist() + [total]
        fig = go.Figure(go.Waterfall(
            orientation="v",
            measure=measures,
            x=x_vals,
            y=y_vals,
            text=[f"{v:,.0f}" for v in y_vals],
            textposition="outside",
            connector=dict(line=dict(color="#E2E8F0", width=1.5, dash="dot")),
            increasing=dict(marker=dict(color=color)),
            decreasing=dict(marker=dict(color=PALETTES["Failure"][2])),
            totals=dict(marker=dict(color="#1E293B")),
            hovertemplate="<b>%{x}</b><br>Value: <b>%{y:,.1f}</b><extra></extra>",
        ))
        fig.update_layout(**BASE)
        fig.update_layout(
            xaxis_title=x_col.replace("_", " ").title() if x_col else "",
            yaxis_title=y_col.replace("_", " ").title() if y_col else "",
        )

    # ── Heatmap ───────────────────────────────────────────────────────────────
    elif chart_type == "heatmap":
        x_col_h = metric.get("x_col", "month_name")
        y_col_h = metric.get("y_col", "brand")
        z_col_h = metric.get("z_col", "total_cost")
        month_order = ["January","February","March","April","May","June",
                       "July","August","September","October","November","December"]
        pivot = df.pivot_table(index=y_col_h, columns=x_col_h, values=z_col_h, aggfunc="sum", fill_value=0)
        # Reorder months if present
        ordered_cols = [m for m in month_order if m in pivot.columns]
        if ordered_cols:
            pivot = pivot[ordered_cols]
        color_scale = [
            [0.0, "#EFF6FF"], [0.2, "#BFDBFE"], [0.4, "#93C5FD"],
            [0.6, "#3B82F6"], [0.8, "#2563EB"], [1.0, "#1D4ED8"]
        ]
        if cat == "Downtime":
            color_scale = [[0.0,"#F5F3FF"],[0.5,"#7C3AED"],[1.0,"#3B0764"]]
        elif cat == "Failure":
            color_scale = [[0.0,"#FEF2F2"],[0.5,"#EF4444"],[1.0,"#7F1D1D"]]
        fig = go.Figure(go.Heatmap(
            z=pivot.values.tolist(),
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=color_scale,
            hovertemplate="<b>%{y}</b> · %{x}<br>Value: <b>%{z:,.0f}</b><extra></extra>",
            showscale=True,
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, system-ui, sans-serif", color="#334155", size=11),
            margin=dict(t=12, r=80, b=80, l=100),
            xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
            yaxis=dict(tickfont=dict(size=10)),
            hoverlabel=dict(bgcolor="#1E293B", font_color="#F8FAFC", font_size=12),
        )

    # ── Box Plot ──────────────────────────────────────────────────────────────
    elif chart_type == "boxplot":
        colors = _palette(cat, len(df))
        fig = go.Figure()
        for i, row in df.iterrows():
            label = str(row[x_col])
            q1  = float(row.get("cost_q1",  row.get("avg_downtime", 0)))
            med = float(row.get("cost_median", row.get("avg_downtime", 0)))
            q3  = float(row.get("cost_q3",  row.get("avg_downtime", 0)))
            mn  = float(row.get("cost_min", q1 * 0.5))
            mx  = float(row.get("cost_max", q3 * 1.5))
            fig.add_trace(go.Box(
                name=label,
                q1=[q1], median=[med], q3=[q3],
                lowerfence=[mn], upperfence=[mx],
                mean=[float(row.get("cost_mean", med))],
                marker_color=colors[i % len(colors)],
                line_color=colors[i % len(colors)],
                fillcolor=colors[i % len(colors)] + "40",
                showlegend=False,
                hovertemplate=f"<b>{label}</b><br>Median: <b>%{{median:,.0f}}</b><br>Q1: %{{q1:,.0f}}<br>Q3: %{{q3:,.0f}}<extra></extra>",
            ))
        fig.update_layout(**BASE)

    # ── Scatter ───────────────────────────────────────────────────────────────
    elif chart_type == "scatter":
        x_s     = metric.get("x_col", "avg_downtime")
        y_s     = metric.get("y_col", "avg_cost")
        label_c = metric.get("label_col", x_col)
        size_c  = metric.get("size_col")
        color   = PALETTES.get(cat, PALETTES["General"])[1]
        sizes   = None
        if size_c and size_c in df.columns:
            raw_sizes = df[size_c].astype(float)
            sizes = ((raw_sizes - raw_sizes.min()) / (raw_sizes.max() - raw_sizes.min() + 1) * 30 + 10).tolist()
        fig = go.Figure(go.Scatter(
            x=df[x_s] if x_s in df.columns else df.iloc[:, 0],
            y=df[y_s] if y_s in df.columns else df.iloc[:, 1],
            mode="markers+text",
            text=df[label_c].tolist() if label_c in df.columns else None,
            textposition="top center",
            textfont=dict(size=10, color="#475569"),
            marker=dict(
                size=sizes or 14,
                color=color,
                opacity=0.8,
                line=dict(color="#FFFFFF", width=1.5),
            ),
            hovertemplate=f"<b>%{{text}}</b><br>X: <b>%{{x:,.2f}}</b><br>Y: <b>%{{y:,.2f}}</b><extra></extra>",
        ))
        fig.update_layout(**BASE)

    # ── Treemap ───────────────────────────────────────────────────────────────
    elif chart_type == "treemap":
        parent_col = metric.get("parent_col")
        colors = _palette(cat, n)
        if parent_col and parent_col in df.columns:
            # Two-level hierarchy: parent → child
            parents_list = df[parent_col].tolist()
            labels_list  = (df[parent_col] + " / " + df[x_col]).tolist()
            values_list  = df[y_col].tolist()
            # Add parent nodes
            for p in df[parent_col].unique():
                labels_list.append(str(p))
                parents_list.append("")
                values_list.append(0)
            fig = go.Figure(go.Treemap(
                labels=labels_list,
                parents=["" if p == lbl else p for p, lbl in zip(parents_list, labels_list)],
                values=values_list,
                textinfo="label+value+percent root",
                hovertemplate="<b>%{label}</b><br>Value: <b>%{value:,.0f}</b><br>%{percentRoot:.1%} of total<extra></extra>",
                marker=dict(colorscale=[[0, PALETTES.get(cat,PALETTES["General"])[4]],
                                         [1, PALETTES.get(cat,PALETTES["General"])[0]]]),
            ))
        else:
            fig = go.Figure(go.Treemap(
                labels=df[x_col].tolist(),
                parents=[""] * n,
                values=df[y_col].tolist(),
                textinfo="label+value+percent root",
                hovertemplate="<b>%{label}</b><br>Value: <b>%{value:,.0f}</b><extra></extra>",
                marker=dict(colorscale=[[0, PALETTES.get(cat,PALETTES["General"])[4]],
                                         [1, PALETTES.get(cat,PALETTES["General"])[0]]]),
            ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=12, r=4, b=4, l=4),
            font=dict(family="Inter, system-ui, sans-serif", color="#FFFFFF", size=11),
        )

    # ── Histogram ─────────────────────────────────────────────────────────────
    elif chart_type == "histogram":
        color = PALETTES.get(cat, PALETTES["General"])[1]
        r, g, b_val = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fig = go.Figure(go.Bar(
            x=df[x_col],
            y=df[y_col],
            marker=dict(
                color=f"rgba({r},{g},{b_val},0.85)",
                line=dict(color=color, width=0.5),
            ),
            hovertemplate="<b>%{x}</b><br>Count: <b>%{y}</b><extra></extra>",
        ))
        fig.update_layout(**{**BASE, "bargap": 0.05})
        fig.update_layout(
            xaxis_title=x_col.replace("_", " ").title() if x_col else "Downtime Range",
            yaxis_title="Frequency",
        )

    # ── Stacked Bar ───────────────────────────────────────────────────────────
    elif chart_type == "stacked_bar":
        group_col = metric.get("group_col", "component_category")
        groups = df[group_col].unique().tolist() if group_col in df.columns else []
        palette = [
            "#1D4ED8","#7C3AED","#DC2626","#059669","#D97706",
            "#0891B2","#DB2777","#EA580C","#65A30D","#0284C7",
            "#9333EA","#16A34A","#CA8A04","#0369A1","#B91C1C",
        ]
        fig = go.Figure()
        for i, grp in enumerate(groups):
            subset = df[df[group_col] == grp]
            fig.add_trace(go.Bar(
                name=str(grp),
                x=subset[x_col],
                y=subset[y_col],
                marker=dict(color=palette[i % len(palette)], line=dict(width=0)),
                hovertemplate=f"<b>%{{x}}</b><br>{grp}: <b>%{{y:,.0f}}</b><extra></extra>",
            ))
        fig.update_layout(**{**BASE, "barmode": "stack"})
        fig.update_layout(
            xaxis_title=x_col.replace("_", " ").title() if x_col else "Brand",
            yaxis_title=y_col.replace("_", " ").title() if y_col else "Total Cost (MYR)",
        )

    else:
        return _build_chart(df, metric, "bar")

    return _to_json(fig)


class QueryRequest(BaseModel):
    metric_id: str
    chart_type: Optional[str] = None
    filters: Optional[dict] = {}


# Metrics that natively support stacked_bar (have group_col)
STACKED_BAR_METRICS = {"cost_by_brand_and_component", "downtime_by_brand_and_component"}

@router.post("/")
def run_metric(req: QueryRequest):
    # Safety remap: if stacked_bar requested on a non-stacked metric, swap to correct metric
    metric_id = req.metric_id
    if req.chart_type == "stacked_bar" and metric_id not in STACKED_BAR_METRICS:
        metric_id = "downtime_by_brand_and_component" if "downtime" in metric_id else "cost_by_brand_and_component"

    metric = METRICS.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    sql = get_metric_sql(metric_id, req.filters or {})
    if not sql:
        raise HTTPException(status_code=400, detail="Could not build SQL")

    try:
        df = run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

    chart_type = req.chart_type or metric["default_chart"]
    available = metric.get("available_charts", ["bar"])
    if chart_type not in available:
        chart_type = metric["default_chart"]

    chart_json = _build_chart(df, metric, chart_type)
    return {
        "metric_id":        metric_id,
        "title":            metric["title"],
        "category":         metric["category"],
        "chart_type":       chart_type,
        "chart":            chart_json,
        "row_count":        len(df),
        "summary":          df.head(5).to_dict(orient="records"),
        "available_charts": available,
        "sql":              sql,
    }


@router.get("/metrics")
def list_metrics():
    from metrics import get_metrics_index
    return {"metrics": get_metrics_index()}



