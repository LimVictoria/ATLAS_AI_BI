"""
ATLAS BI — Metrics Library
Predefined KPI formulas for fleet maintenance analysis.
LLM reads these from the schema guide and uses them consistently.
All queries use v_maintenance_full — the comprehensive pre-joined view.
"""

METRICS = {
    # ── COST ──────────────────────────────────────────────────────────────────
    "total_cost": {
        "name": "Total Maintenance Cost", "expr": "ROUND(SUM(total_cost_myr), 2)",
        "category": "Cost", "unit": "MYR", "description": "Total cost across all maintenance events",
    },
    "total_parts_cost": {
        "name": "Total Parts Cost", "expr": "ROUND(SUM(parts_cost_myr), 2)",
        "category": "Cost", "unit": "MYR", "description": "Total spend on parts and materials",
    },
    "total_labour_cost": {
        "name": "Total Labour Cost", "expr": "ROUND(SUM(labour_cost_myr), 2)",
        "category": "Cost", "unit": "MYR", "description": "Total spend on labour",
    },
    "avg_cost_per_event": {
        "name": "Average Cost per Event", "expr": "ROUND(AVG(total_cost_myr), 2)",
        "category": "Cost", "unit": "MYR", "description": "Average maintenance cost per event",
    },
    "cost_per_km": {
        "name": "Cost per KM",
        "expr": "ROUND(SUM(total_cost_myr) / NULLIF(SUM(current_mileage_km), 0), 4)",
        "category": "Cost", "unit": "MYR/km", "description": "Maintenance cost per kilometre driven",
    },
    "labour_to_parts_ratio": {
        "name": "Labour to Parts Ratio",
        "expr": "ROUND(SUM(labour_cost_myr) / NULLIF(SUM(parts_cost_myr), 0), 2)",
        "category": "Cost", "unit": "ratio",
        "description": "How much labour costs relative to parts — above 1 means labour-heavy",
    },
    "warranty_recovery_rate": {
        "name": "Warranty Recovery Rate",
        "expr": "ROUND(SUM(CASE WHEN warranty_covered = true THEN total_cost_myr ELSE 0 END) / NULLIF(SUM(total_cost_myr), 0) * 100, 2)",
        "category": "Cost", "unit": "%", "description": "Percentage of total cost covered by warranty",
    },
    "warranty_recovered_amount": {
        "name": "Warranty Recovered Amount",
        "expr": "ROUND(SUM(CASE WHEN warranty_covered = true THEN total_cost_myr ELSE 0 END), 2)",
        "category": "Cost", "unit": "MYR", "description": "Absolute cost recovered through warranty claims",
    },
    # ── DOWNTIME ──────────────────────────────────────────────────────────────
    "total_downtime": {
        "name": "Total Downtime Hours", "expr": "ROUND(SUM(downtime_hours), 2)",
        "category": "Downtime", "unit": "hours", "description": "Total hours vehicles were out of service",
    },
    "avg_downtime_per_event": {
        "name": "Average Downtime per Event", "expr": "ROUND(AVG(downtime_hours), 2)",
        "category": "Downtime", "unit": "hours", "description": "Average downtime hours per maintenance event",
    },
    "total_labour_hours": {
        "name": "Total Labour Hours", "expr": "ROUND(SUM(labour_hours), 2)",
        "category": "Downtime", "unit": "hours", "description": "Total technician hours spent on maintenance",
    },
    "avg_labour_hours_per_event": {
        "name": "Average Labour Hours per Event", "expr": "ROUND(AVG(labour_hours), 2)",
        "category": "Downtime", "unit": "hours", "description": "Average technician hours per maintenance event",
    },
    "downtime_cost_rate": {
        "name": "Cost per Downtime Hour",
        "expr": "ROUND(SUM(total_cost_myr) / NULLIF(SUM(downtime_hours), 0), 2)",
        "category": "Downtime", "unit": "MYR/hour", "description": "Maintenance cost per hour of vehicle downtime",
    },
    # ── RELIABILITY ───────────────────────────────────────────────────────────
    "total_events": {
        "name": "Total Maintenance Events", "expr": "COUNT(*)",
        "category": "Reliability", "unit": "count", "description": "Total number of maintenance events",
    },
    "unscheduled_rate": {
        "name": "Unscheduled Maintenance Rate",
        "expr": "ROUND(COUNT(CASE WHEN is_unscheduled = true THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "category": "Reliability", "unit": "%", "description": "Percentage of maintenance events that were unscheduled",
    },
    "corrective_rate": {
        "name": "Corrective Maintenance Rate",
        "expr": "ROUND(COUNT(CASE WHEN maintenance_type = 'Corrective' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "category": "Reliability", "unit": "%", "description": "Percentage of events that were corrective maintenance",
    },
    "scheduled_pm_rate": {
        "name": "Scheduled PM Rate",
        "expr": "ROUND(COUNT(CASE WHEN maintenance_type = 'Scheduled PM' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "category": "Reliability", "unit": "%", "description": "Percentage of events that were scheduled preventive maintenance",
    },
    "emergency_rate": {
        "name": "Emergency Maintenance Rate",
        "expr": "ROUND(COUNT(CASE WHEN maintenance_type = 'Emergency' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "category": "Reliability", "unit": "%", "description": "Percentage of events that were emergency maintenance",
    },
    "critical_event_rate": {
        "name": "Critical Event Rate",
        "expr": "ROUND(COUNT(CASE WHEN criticality_level = 'Critical' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "category": "Reliability", "unit": "%", "description": "Percentage of maintenance events rated as critical severity",
    },
    "mtbf": {
        "name": "MTBF (Mean Distance Between Failures)",
        "expr": "ROUND(SUM(current_mileage_km) / NULLIF(COUNT(CASE WHEN maintenance_type IN ('Corrective', 'Emergency') THEN 1 END), 0), 0)",
        "category": "Reliability", "unit": "km",
        "description": "Average kilometres driven between corrective or emergency failures",
    },
    # ── UTILISATION ───────────────────────────────────────────────────────────
    "avg_mileage_at_event": {
        "name": "Average Mileage at Event", "expr": "ROUND(AVG(current_mileage_km), 0)",
        "category": "Utilisation", "unit": "km",
        "description": "Average vehicle mileage when maintenance event occurs",
    },
    "avg_technician_count": {
        "name": "Average Technician Count", "expr": "ROUND(AVG(technician_count), 1)",
        "category": "Utilisation", "unit": "count",
        "description": "Average number of technicians assigned per maintenance event",
    },
    # ── WORKSHOP ──────────────────────────────────────────────────────────────
    "authorised_dealer_rate": {
        "name": "Authorised Dealer Usage Rate",
        "expr": "ROUND(COUNT(CASE WHEN workshop_type = 'Authorised Dealer' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)",
        "category": "Workshop", "unit": "%",
        "description": "Percentage of repairs performed at authorised dealers",
    },
}


def get_metrics_guide() -> str:
    """Return a formatted metrics guide for injection into the LLM schema prompt."""
    lines = ["", "PREDEFINED METRICS — always use these exact formulas, never improvise:", ""]

    by_category: dict = {}
    for key, m in METRICS.items():
        cat = m["category"]
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append((key, m))

    for cat, items in by_category.items():
        lines.append(f"{cat.upper()} METRICS:")
        for key, m in items:
            lines.append(f"  {m['name']} [{m['unit']}] — key: {key}")
            lines.append(f"    expr: {m['expr']}")
        lines.append("")

    lines += [
        "YEAR-OVER-YEAR PATTERNS — use when user asks for YoY, growth, change, trend:",
        "  Example: YoY Cost Change by Brand",
        "    SELECT brand,",
        "      ROUND(SUM(CASE WHEN year = (SELECT MAX(year) FROM v_maintenance_full) THEN total_cost_myr ELSE 0 END), 2) AS current_year_cost,",
        "      ROUND(SUM(CASE WHEN year = (SELECT MAX(year) FROM v_maintenance_full) - 1 THEN total_cost_myr ELSE 0 END), 2) AS prior_year_cost,",
        "      ROUND((SUM(CASE WHEN year = (SELECT MAX(year) FROM v_maintenance_full) THEN total_cost_myr ELSE 0 END) -",
        "             SUM(CASE WHEN year = (SELECT MAX(year) FROM v_maintenance_full) - 1 THEN total_cost_myr ELSE 0 END)) /",
        "            NULLIF(SUM(CASE WHEN year = (SELECT MAX(year) FROM v_maintenance_full) - 1 THEN total_cost_myr ELSE 0 END), 0) * 100, 2) AS yoy_change_pct",
        "    FROM v_maintenance_full GROUP BY brand ORDER BY yoy_change_pct DESC",
        "  Apply same pattern for any metric. Use specific years when requested (e.g. 2023 vs 2022).",
        "",
        "USAGE RULES:",
        "  - ALWAYS query v_maintenance_full — never source tables",
        "  - When user asks for a named metric, use the exact expr above",
        "  - Add AS <alias> after the expr in your SELECT",
        "  - Add GROUP BY and ORDER BY as needed",
        "  - WHERE is added separately — never hardcode it in metric expr",
        "",
        "EXAMPLES:",
        "  'show total cost by brand'",
        "    -> SELECT brand, ROUND(SUM(total_cost_myr), 2) AS total_cost",
        "       FROM v_maintenance_full GROUP BY brand ORDER BY total_cost DESC",
        "",
        "  'show MTBF by fleet segment'",
        "    -> SELECT fleet_segment,",
        "            ROUND(SUM(current_mileage_km) / NULLIF(COUNT(CASE WHEN maintenance_type IN ('Corrective', 'Emergency') THEN 1 END), 0), 0) AS mtbf_km",
        "       FROM v_maintenance_full GROUP BY fleet_segment ORDER BY mtbf_km DESC",
    ]

    return "\n".join(lines)


# ── FastAPI router ─────────────────────────────────────────────────────────────
from fastapi import APIRouter, HTTPException
router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/")
def get_all_metrics():
    return {"metrics": METRICS}


@router.get("/{metric_key}")
def get_metric(metric_key: str):
    m = METRICS.get(metric_key)
    if not m:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_key}' not found")
    return {"key": metric_key, **m}
