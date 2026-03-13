"""
ATLAS BI — Semantic Layer
All pre-built metrics. LangGraph agent maps user intent to metric_id.
LLM never writes SQL from scratch — it only picks metric + chart + filters.
"""

from typing import Optional

METRICS = {

    # ── Cost Metrics ──────────────────────────────────────────────────────────

    "total_cost_by_brand": {
        "metric_id":       "total_cost_by_brand",
        "title":           "Total Maintenance Cost by Brand",
        "description":     "Total maintenance cost grouped by truck brand",
        "category":        "Cost",
        "sql":             """
            SELECT brand,
                   ROUND(SUM(total_cost_myr), 2)  AS total_cost,
                   COUNT(*)                         AS event_count,
                   ROUND(AVG(total_cost_myr), 2)   AS avg_cost_per_event
            FROM v_maintenance_full
            {where}
            GROUP BY brand
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["total_cost", "event_count", "avg_cost_per_event"],
        "default_chart":   "bar",
        "available_charts":["bar", "pie", "table", "pareto", "waterfall", "treemap"],
        "x_col":           "brand",
        "y_col":           "total_cost",
    },

    "total_cost_by_component_category": {
        "metric_id":       "total_cost_by_component_category",
        "title":           "Total Maintenance Cost by Component Category",
        "description":     "Which component categories cost the most to maintain",
        "category":        "Cost",
        "sql":             """
            SELECT component_category,
                   ROUND(SUM(total_cost_myr), 2)  AS total_cost,
                   ROUND(SUM(parts_cost_myr), 2)  AS parts_cost,
                   ROUND(SUM(labour_cost_myr), 2) AS labour_cost,
                   COUNT(*)                         AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY component_category
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["component_category"],
        "measures":        ["total_cost", "parts_cost", "labour_cost", "event_count"],
        "default_chart":   "bar",
        "available_charts":["bar", "pie", "table", "pareto", "waterfall", "treemap"],
        "x_col":           "component_category",
        "y_col":           "total_cost",
    },

    "total_cost_by_fleet_segment": {
        "metric_id":       "total_cost_by_fleet_segment",
        "title":           "Total Maintenance Cost by Fleet Segment",
        "description":     "Maintenance cost breakdown by fleet segment",
        "category":        "Cost",
        "sql":             """
            SELECT fleet_segment,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost,
                   COUNT(DISTINCT plate_number)   AS truck_count,
                   ROUND(SUM(total_cost_myr) / COUNT(DISTINCT plate_number), 2) AS cost_per_truck
            FROM v_maintenance_full
            {where}
            GROUP BY fleet_segment
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["fleet_segment"],
        "measures":        ["total_cost", "truck_count", "cost_per_truck"],
        "default_chart":   "bar",
        "available_charts":["bar", "pie", "table", "waterfall", "treemap"],
        "x_col":           "fleet_segment",
        "y_col":           "total_cost",
    },

    "total_cost_by_workshop": {
        "metric_id":       "total_cost_by_workshop",
        "title":           "Total Maintenance Cost by Workshop",
        "description":     "Which workshops are generating the most maintenance spend",
        "category":        "Cost",
        "sql":             """
            SELECT workshop_name,
                   workshop_type,
                   is_authorised_scania,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost,
                   COUNT(*)                       AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY workshop_name, workshop_type, is_authorised_scania
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["workshop_name", "workshop_type"],
        "measures":        ["total_cost", "event_count"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto"],
        "x_col":           "workshop_name",
        "y_col":           "total_cost",
    },

    "cost_trend_by_month": {
        "metric_id":       "cost_trend_by_month",
        "title":           "Maintenance Cost Trend by Month",
        "description":     "Monthly maintenance cost trend over time",
        "category":        "Cost",
        "sql":             """
            SELECT year || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') AS year_month,
                   year,
                   month_name,
                   ROUND(SUM(total_cost_myr), 2)  AS total_cost,
                   COUNT(*)                         AS event_count,
                   ROUND(AVG(total_cost_myr), 2)   AS avg_cost
            FROM v_maintenance_full
            {where}
            GROUP BY year, month, month_name
            ORDER BY year, month
        """,
        "dimensions":      ["year_month", "month_name"],
        "measures":        ["total_cost", "event_count", "avg_cost"],
        "default_chart":   "line",
        "available_charts":["line", "bar", "table", "histogram"],
        "x_col":           "year_month",
        "y_col":           "total_cost",
    },

    "parts_vs_labour_cost": {
        "metric_id":       "parts_vs_labour_cost",
        "title":           "Parts vs Labour Cost Breakdown",
        "description":     "Comparison of parts cost versus labour cost by category",
        "category":        "Cost",
        "sql":             """
            SELECT component_category,
                   ROUND(SUM(parts_cost_myr), 2)  AS parts_cost,
                   ROUND(SUM(labour_cost_myr), 2) AS labour_cost,
                   ROUND(SUM(total_cost_myr), 2)  AS total_cost
            FROM v_maintenance_full
            {where}
            GROUP BY component_category
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["component_category"],
        "measures":        ["parts_cost", "labour_cost", "total_cost"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "waterfall"],
        "x_col":           "component_category",
        "y_col":           "total_cost",
    },

    # ── Downtime Metrics ──────────────────────────────────────────────────────

    "downtime_by_component_category": {
        "metric_id":       "downtime_by_component_category",
        "title":           "Downtime by Component Category",
        "description":     "Average and total downtime hours by component category",
        "category":        "Downtime",
        "sql":             """
            SELECT component_category,
                   ROUND(AVG(downtime_hours), 2) AS avg_downtime,
                   ROUND(SUM(downtime_hours), 2) AS total_downtime,
                   COUNT(*)                       AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY component_category
            ORDER BY avg_downtime DESC
        """,
        "dimensions":      ["component_category"],
        "measures":        ["avg_downtime", "total_downtime", "event_count"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto", "waterfall"],
        "x_col":           "component_category",
        "y_col":           "avg_downtime",
    },

    "downtime_by_failure_type": {
        "metric_id":       "downtime_by_failure_type",
        "title":           "Downtime by Failure Type",
        "description":     "Which failure types cause the most downtime",
        "category":        "Downtime",
        "sql":             """
            SELECT failure_type,
                   ROUND(AVG(downtime_hours), 2) AS avg_downtime,
                   ROUND(SUM(downtime_hours), 2) AS total_downtime,
                   COUNT(*)                       AS occurrences
            FROM v_maintenance_full
            {where}
            AND failure_type IS NOT NULL
            GROUP BY failure_type
            ORDER BY avg_downtime DESC
            LIMIT 15
        """,
        "dimensions":      ["failure_type"],
        "measures":        ["avg_downtime", "total_downtime", "occurrences"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto"],
        "x_col":           "failure_type",
        "y_col":           "avg_downtime",
    },

    "downtime_by_brand": {
        "metric_id":       "downtime_by_brand",
        "title":           "Downtime by Truck Brand",
        "description":     "Average downtime per maintenance event by truck brand",
        "category":        "Downtime",
        "sql":             """
            SELECT brand,
                   ROUND(AVG(downtime_hours), 2) AS avg_downtime,
                   ROUND(SUM(downtime_hours), 2) AS total_downtime,
                   COUNT(*)                       AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY brand
            ORDER BY avg_downtime DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["avg_downtime", "total_downtime", "event_count"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "boxplot"],
        "x_col":           "brand",
        "y_col":           "avg_downtime",
    },

    "downtime_trend_by_month": {
        "metric_id":       "downtime_trend_by_month",
        "title":           "Downtime Trend by Month",
        "description":     "Monthly downtime hours trend over time",
        "category":        "Downtime",
        "sql":             """
            SELECT year || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') AS year_month,
                   ROUND(SUM(downtime_hours), 2)  AS total_downtime,
                   ROUND(AVG(downtime_hours), 2)  AS avg_downtime,
                   COUNT(*)                        AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY year, month
            ORDER BY year, month
        """,
        "dimensions":      ["year_month"],
        "measures":        ["total_downtime", "avg_downtime", "event_count"],
        "default_chart":   "line",
        "available_charts":["line", "bar", "table"],
        "x_col":           "year_month",
        "y_col":           "total_downtime",
    },

    # ── Failure Metrics ───────────────────────────────────────────────────────

    "failure_count_by_component": {
        "metric_id":       "failure_count_by_component",
        "title":           "Failure Count by Component",
        "description":     "Most frequently failing components",
        "category":        "Failure",
        "sql":             """
            SELECT component_name,
                   component_category,
                   criticality_level,
                   COUNT(*) AS failure_count,
                   ROUND(AVG(total_cost_myr), 2) AS avg_repair_cost
            FROM v_maintenance_full
            {where}
            AND is_unscheduled = true
            GROUP BY component_name, component_category, criticality_level
            ORDER BY failure_count DESC
            LIMIT 15
        """,
        "dimensions":      ["component_name", "component_category"],
        "measures":        ["failure_count", "avg_repair_cost"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto", "treemap"],
        "x_col":           "component_name",
        "y_col":           "failure_count",
    },

    "failure_count_by_brand": {
        "metric_id":       "failure_count_by_brand",
        "title":           "Failure Count by Brand",
        "description":     "Number of unscheduled failures per truck brand",
        "category":        "Failure",
        "sql":             """
            SELECT brand,
                   COUNT(*) AS failure_count,
                   COUNT(DISTINCT plate_number) AS truck_count,
                   ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT plate_number), 2) AS failures_per_truck
            FROM v_maintenance_full
            {where}
            AND is_unscheduled = true
            GROUP BY brand
            ORDER BY failures_per_truck DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["failure_count", "truck_count", "failures_per_truck"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto"],
        "x_col":           "brand",
        "y_col":           "failures_per_truck",
    },

    "scheduled_vs_unscheduled": {
        "metric_id":       "scheduled_vs_unscheduled",
        "title":           "Scheduled vs Unscheduled Maintenance",
        "description":     "Ratio of planned preventive maintenance vs reactive repairs",
        "category":        "Failure",
        "sql":             """
            SELECT maintenance_type,
                   COUNT(*)                         AS event_count,
                   ROUND(SUM(total_cost_myr), 2)   AS total_cost,
                   ROUND(AVG(downtime_hours), 2)    AS avg_downtime
            FROM v_maintenance_full
            {where}
            GROUP BY maintenance_type
            ORDER BY event_count DESC
        """,
        "dimensions":      ["maintenance_type"],
        "measures":        ["event_count", "total_cost", "avg_downtime"],
        "default_chart":   "pie",
        "available_charts":["pie", "bar", "table"],
        "x_col":           "maintenance_type",
        "y_col":           "event_count",
    },

    "top_failure_types": {
        "metric_id":       "top_failure_types",
        "title":           "Top Failure Types",
        "description":     "Most common failure types across the fleet",
        "category":        "Failure",
        "sql":             """
            SELECT failure_type,
                   COUNT(*)                         AS occurrences,
                   ROUND(AVG(total_cost_myr), 2)   AS avg_cost,
                   ROUND(AVG(downtime_hours), 2)    AS avg_downtime
            FROM v_maintenance_full
            {where}
            AND failure_type IS NOT NULL
            GROUP BY failure_type
            ORDER BY occurrences DESC
            LIMIT 10
        """,
        "dimensions":      ["failure_type"],
        "measures":        ["occurrences", "avg_cost", "avg_downtime"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto"],
        "x_col":           "failure_type",
        "y_col":           "occurrences",
    },

    "failure_trend_by_quarter": {
        "metric_id":       "failure_trend_by_quarter",
        "title":           "Failure Trend by Quarter",
        "description":     "Quarterly trend of unscheduled failures",
        "category":        "Failure",
        "sql":             """
            SELECT year,
                   quarter,
                   CAST(year AS VARCHAR) || ' Q' || CAST(quarter AS VARCHAR) AS year_quarter,
                   COUNT(*) AS failure_count,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost
            FROM v_maintenance_full
            {where}
            AND is_unscheduled = true
            GROUP BY year, quarter
            ORDER BY year, quarter
        """,
        "dimensions":      ["year_quarter"],
        "measures":        ["failure_count", "total_cost"],
        "default_chart":   "line",
        "available_charts":["line", "bar", "table"],
        "x_col":           "year_quarter",
        "y_col":           "failure_count",
    },

    # ── Fleet Metrics ─────────────────────────────────────────────────────────

    "fleet_summary_by_brand": {
        "metric_id":       "fleet_summary_by_brand",
        "title":           "Fleet Summary by Brand",
        "description":     "Fleet composition and health overview by brand",
        "category":        "Fleet",
        "sql":             """
            SELECT brand,
                   COUNT(DISTINCT plate_number)                              AS truck_count,
                   ROUND(AVG(current_mileage_km), 0)                        AS avg_mileage,
                   ROUND(AVG(year_manufactured), 1)                         AS avg_year,
                   ROUND(SUM(total_cost_myr) / COUNT(DISTINCT plate_number), 2) AS cost_per_truck
            FROM v_maintenance_full
            {where}
            GROUP BY brand
            ORDER BY truck_count DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["truck_count", "avg_mileage", "cost_per_truck"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "scatter"],
        "x_col":           "brand",
        "y_col":           "truck_count",
    },

    "active_trucks_by_segment": {
        "metric_id":       "active_trucks_by_segment",
        "title":           "Active Trucks by Fleet Segment",
        "description":     "Fleet distribution across operational segments",
        "category":        "Fleet",
        "sql":             """
            SELECT fleet_segment,
                   COUNT(DISTINCT plate_number) AS truck_count,
                   ROUND(AVG(current_mileage_km), 0) AS avg_mileage
            FROM v_maintenance_full
            {where}
            GROUP BY fleet_segment
            ORDER BY truck_count DESC
        """,
        "dimensions":      ["fleet_segment"],
        "measures":        ["truck_count", "avg_mileage"],
        "default_chart":   "pie",
        "available_charts":["pie", "bar", "table", "treemap"],
        "x_col":           "fleet_segment",
        "y_col":           "truck_count",
    },

    "cost_by_truck_age": {
        "metric_id":       "cost_by_truck_age",
        "title":           "Maintenance Cost by Truck Age",
        "description":     "How maintenance cost varies with truck age",
        "category":        "Fleet",
        "sql":             """
            SELECT year_manufactured,
                   2024 - year_manufactured                                      AS truck_age_years,
                   COUNT(DISTINCT plate_number)                                   AS truck_count,
                   ROUND(SUM(total_cost_myr) / COUNT(DISTINCT plate_number), 2)  AS cost_per_truck,
                   ROUND(AVG(downtime_hours), 2)                                  AS avg_downtime
            FROM v_maintenance_full
            {where}
            GROUP BY year_manufactured
            ORDER BY year_manufactured
        """,
        "dimensions":      ["year_manufactured", "truck_age_years"],
        "measures":        ["cost_per_truck", "avg_downtime", "truck_count"],
        "default_chart":   "line",
        "available_charts":["line", "bar", "table", "scatter"],
        "x_col":           "truck_age_years",
        "y_col":           "cost_per_truck",
    },

    "warranty_coverage_rate": {
        "metric_id":       "warranty_coverage_rate",
        "title":           "Warranty Coverage Rate",
        "description":     "Proportion of maintenance events covered under warranty",
        "category":        "Fleet",
        "sql":             """
            SELECT brand,
                   COUNT(*) AS total_events,
                   SUM(CASE WHEN warranty_covered THEN 1 ELSE 0 END) AS warranty_events,
                   ROUND(SUM(CASE WHEN warranty_covered THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS warranty_pct
            FROM v_maintenance_full
            {where}
            GROUP BY brand
            ORDER BY warranty_pct DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["warranty_pct", "warranty_events", "total_events"],
        "default_chart":   "bar",
        "available_charts":["bar", "table"],
        "x_col":           "brand",
        "y_col":           "warranty_pct",
    },

    # ── Workshop Metrics ──────────────────────────────────────────────────────

    "repairs_by_workshop": {
        "metric_id":       "repairs_by_workshop",
        "title":           "Repairs by Workshop",
        "description":     "Volume and cost of repairs per workshop",
        "category":        "Workshop",
        "sql":             """
            SELECT workshop_name,
                   workshop_type,
                   workshop_state,
                   COUNT(*)                       AS repair_count,
                   ROUND(SUM(total_cost_myr), 2) AS total_revenue,
                   ROUND(AVG(downtime_hours), 2)  AS avg_downtime
            FROM v_maintenance_full
            {where}
            GROUP BY workshop_name, workshop_type, workshop_state
            ORDER BY repair_count DESC
        """,
        "dimensions":      ["workshop_name", "workshop_type"],
        "measures":        ["repair_count", "total_revenue", "avg_downtime"],
        "default_chart":   "bar",
        "available_charts":["bar", "table", "pareto"],
        "x_col":           "workshop_name",
        "y_col":           "repair_count",
    },

    "authorised_vs_independent": {
        "metric_id":       "authorised_vs_independent",
        "title":           "Authorised vs Independent Workshop Usage",
        "description":     "Cost and downtime comparison between authorised and independent workshops",
        "category":        "Workshop",
        "sql":             """
            SELECT CASE WHEN is_authorised_scania THEN 'Authorised' ELSE 'Independent' END AS workshop_auth,
                   COUNT(*)                        AS event_count,
                   ROUND(AVG(total_cost_myr), 2)  AS avg_cost,
                   ROUND(AVG(downtime_hours), 2)   AS avg_downtime,
                   ROUND(SUM(total_cost_myr), 2)   AS total_cost
            FROM v_maintenance_full
            {where}
            GROUP BY is_authorised_scania
            ORDER BY event_count DESC
        """,
        "dimensions":      ["workshop_auth"],
        "measures":        ["event_count", "avg_cost", "avg_downtime"],
        "default_chart":   "bar",
        "available_charts":["bar", "pie", "table"],
        "x_col":           "workshop_auth",
        "y_col":           "avg_cost",
    },

    "workshop_cost_by_region": {
        "metric_id":       "workshop_cost_by_region",
        "title":           "Workshop Cost by Region",
        "description":     "Maintenance spend distributed by geographic region",
        "category":        "Workshop",
        "sql":             """
            SELECT region,
                   COUNT(*)                        AS event_count,
                   ROUND(SUM(total_cost_myr), 2)  AS total_cost,
                   ROUND(AVG(total_cost_myr), 2)  AS avg_cost,
                   ROUND(AVG(downtime_hours), 2)   AS avg_downtime
            FROM v_maintenance_full
            {where}
            GROUP BY region
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["region"],
        "measures":        ["event_count", "total_cost", "avg_cost"],
        "default_chart":   "bar",
        "available_charts":["bar", "pie", "table", "treemap"],
        "x_col":           "region",
        "y_col":           "total_cost",
    },

    # ── Time-Aware Metrics ────────────────────────────────────────────────────

    "yoy_cost_comparison": {
        "metric_id":       "yoy_cost_comparison",
        "title":           "Year-over-Year Cost Comparison",
        "description":     "Total maintenance cost compared year over year",
        "category":        "Time",
        "sql":             """
            SELECT year,
                   ROUND(SUM(total_cost_myr), 2)  AS total_cost,
                   COUNT(*)                         AS event_count,
                   ROUND(AVG(total_cost_myr), 2)   AS avg_cost,
                   ROUND(SUM(downtime_hours), 2)    AS total_downtime
            FROM v_maintenance_full
            {where}
            GROUP BY year ORDER BY year
        """,
        "dimensions": ["year"], "measures": ["total_cost","event_count","avg_cost"],
        "default_chart": "bar", "available_charts": ["bar","line","table","waterfall"],
        "x_col": "year", "y_col": "total_cost",
    },

    "yoy_failure_comparison": {
        "metric_id":       "yoy_failure_comparison",
        "title":           "Year-over-Year Failure Comparison",
        "description":     "Unscheduled failures compared year over year",
        "category":        "Time",
        "sql":             """
            SELECT year,
                   COUNT(*) AS total_failures,
                   COUNT(DISTINCT plate_number) AS trucks_affected,
                   ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT plate_number), 2) AS failures_per_truck
            FROM v_maintenance_full
            {where}
            AND is_unscheduled = true
            GROUP BY year ORDER BY year
        """,
        "dimensions": ["year"], "measures": ["total_failures","trucks_affected","failures_per_truck"],
        "default_chart": "bar", "available_charts": ["bar","line","table"],
        "x_col": "year", "y_col": "total_failures",
    },

    "qoq_cost_comparison": {
        "metric_id":       "qoq_cost_comparison",
        "title":           "Quarter-over-Quarter Cost Comparison",
        "description":     "Maintenance cost per quarter — reveals seasonal patterns",
        "category":        "Time",
        "sql":             """
            SELECT CAST(year AS VARCHAR) || ' Q' || CAST(quarter AS VARCHAR) AS year_quarter,
                   year, quarter,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost,
                   COUNT(*) AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY year, quarter ORDER BY year, quarter
        """,
        "dimensions": ["year_quarter"], "measures": ["total_cost","event_count"],
        "default_chart": "line", "available_charts": ["line","bar","table"],
        "x_col": "year_quarter", "y_col": "total_cost",
    },

    "last_12_months_trend": {
        "metric_id":       "last_12_months_trend",
        "title":           "Last 12 Months Cost Trend",
        "description":     "Rolling 12-month maintenance cost — the most common executive view",
        "category":        "Time",
        "sql":             """
            SELECT year || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') AS year_month,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost,
                   COUNT(*) AS event_count,
                   ROUND(SUM(downtime_hours), 2) AS total_downtime,
                   SUM(CASE WHEN is_unscheduled THEN 1 ELSE 0 END) AS failures
            FROM v_maintenance_full
            {where}
            AND full_date >= (SELECT MAX(full_date) FROM v_maintenance_full) - INTERVAL 12 MONTH
            GROUP BY year, month ORDER BY year, month
        """,
        "dimensions": ["year_month"], "measures": ["total_cost","event_count","failures"],
        "default_chart": "line", "available_charts": ["line","bar","table"],
        "x_col": "year_month", "y_col": "total_cost",
    },

    "same_quarter_last_year": {
        "metric_id":       "same_quarter_last_year",
        "title":           "This Quarter vs Same Quarter Last Year",
        "description":     "Direct comparison of current quarter vs same quarter prior year",
        "category":        "Time",
        "sql":             """
            SELECT CAST(year AS VARCHAR) || ' Q' || CAST(quarter AS VARCHAR) AS period,
                   year, quarter,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost,
                   COUNT(*) AS event_count,
                   SUM(CASE WHEN is_unscheduled THEN 1 ELSE 0 END) AS failures
            FROM v_maintenance_full
            {where}
            AND quarter = (SELECT EXTRACT(QUARTER FROM MAX(full_date)) FROM v_maintenance_full)
            AND year IN (
                (SELECT MAX(year) FROM v_maintenance_full),
                (SELECT MAX(year) - 1 FROM v_maintenance_full)
            )
            GROUP BY year, quarter ORDER BY year
        """,
        "dimensions": ["period"], "measures": ["total_cost","event_count","failures"],
        "default_chart": "bar", "available_charts": ["bar","table"],
        "x_col": "period", "y_col": "total_cost",
    },

    "cost_growth_rate": {
        "metric_id":       "cost_growth_rate",
        "title":           "Annual Cost Growth Rate (%)",
        "description":     "Year-over-year percentage change in maintenance cost",
        "category":        "Time",
        "sql":             """
            WITH yearly AS (
                SELECT year, ROUND(SUM(total_cost_myr), 2) AS total_cost
                FROM v_maintenance_full {where} GROUP BY year
            )
            SELECT curr.year, curr.total_cost, prev.total_cost AS prev_year_cost,
                   ROUND((curr.total_cost - prev.total_cost) / prev.total_cost * 100, 1) AS growth_pct
            FROM yearly curr
            LEFT JOIN yearly prev ON curr.year = prev.year + 1
            ORDER BY curr.year
        """,
        "dimensions": ["year"], "measures": ["total_cost","prev_year_cost","growth_pct"],
        "default_chart": "bar", "available_charts": ["bar","line","table"],
        "x_col": "year", "y_col": "growth_pct",
    },

    # ── New Specialist Metrics ────────────────────────────────────────────────

    "cost_heatmap_brand_month": {
        "metric_id":       "cost_heatmap_brand_month",
        "title":           "Cost Heatmap: Brand × Month",
        "description":     "Monthly cost intensity grid — spot seasonal patterns per brand",
        "category":        "Cost",
        "sql":             """
            SELECT brand,
                   month_name,
                   month,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost
            FROM v_maintenance_full
            {where}
            GROUP BY brand, month, month_name
            ORDER BY brand, month
        """,
        "dimensions":      ["brand", "month_name"],
        "measures":        ["total_cost"],
        "default_chart":   "heatmap",
        "available_charts":["heatmap", "table"],
        "x_col":           "month_name",
        "y_col":           "brand",
        "z_col":           "total_cost",
    },

    "cost_vs_downtime_scatter": {
        "metric_id":       "cost_vs_downtime_scatter",
        "title":           "Cost vs Downtime by Brand",
        "description":     "Scatter plot revealing relationship between repair cost and downtime per brand",
        "category":        "Cost",
        "sql":             """
            SELECT brand,
                   ROUND(AVG(total_cost_myr), 2)  AS avg_cost,
                   ROUND(AVG(downtime_hours), 2)   AS avg_downtime,
                   COUNT(*)                         AS event_count,
                   ROUND(SUM(total_cost_myr), 2)   AS total_cost
            FROM v_maintenance_full
            {where}
            GROUP BY brand
            ORDER BY avg_cost DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["avg_cost", "avg_downtime", "event_count"],
        "default_chart":   "scatter",
        "available_charts":["scatter", "bar", "table"],
        "x_col":           "avg_downtime",
        "y_col":           "avg_cost",
        "label_col":       "brand",
        "size_col":        "event_count",
    },

    "cost_distribution_by_brand": {
        "metric_id":       "cost_distribution_by_brand",
        "title":           "Cost Distribution by Brand",
        "description":     "Box plot showing cost spread, median, and outliers per brand",
        "category":        "Cost",
        "sql":             """
            SELECT brand,
                   ROUND(MIN(total_cost_myr), 2)                                        AS cost_min,
                   ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_cost_myr), 2) AS cost_q1,
                   ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_cost_myr), 2) AS cost_median,
                   ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_cost_myr), 2) AS cost_q3,
                   ROUND(MAX(total_cost_myr), 2)                                        AS cost_max,
                   ROUND(AVG(total_cost_myr), 2)                                        AS cost_mean,
                   COUNT(*)                                                               AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY brand
            ORDER BY cost_median DESC
        """,
        "dimensions":      ["brand"],
        "measures":        ["cost_min","cost_q1","cost_median","cost_q3","cost_max"],
        "default_chart":   "boxplot",
        "available_charts":["boxplot", "bar", "table"],
        "x_col":           "brand",
        "y_col":           "cost_median",
    },

    "cost_waterfall_by_category": {
        "metric_id":       "cost_waterfall_by_category",
        "title":           "Cost Waterfall by Component Category",
        "description":     "Cumulative cost buildup — see how each category contributes to total",
        "category":        "Cost",
        "sql":             """
            SELECT component_category,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost
            FROM v_maintenance_full
            {where}
            GROUP BY component_category
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["component_category"],
        "measures":        ["total_cost"],
        "default_chart":   "waterfall",
        "available_charts":["waterfall", "bar", "table"],
        "x_col":           "component_category",
        "y_col":           "total_cost",
    },

    "fleet_cost_treemap": {
        "metric_id":       "fleet_cost_treemap",
        "title":           "Fleet Cost Treemap",
        "description":     "Hierarchical cost view: brand → fleet segment",
        "category":        "Fleet",
        "sql":             """
            SELECT brand,
                   fleet_segment,
                   ROUND(SUM(total_cost_myr), 2) AS total_cost,
                   COUNT(*) AS event_count
            FROM v_maintenance_full
            {where}
            GROUP BY brand, fleet_segment
            ORDER BY total_cost DESC
        """,
        "dimensions":      ["brand", "fleet_segment"],
        "measures":        ["total_cost", "event_count"],
        "default_chart":   "treemap",
        "available_charts":["treemap", "bar", "table"],
        "x_col":           "fleet_segment",
        "y_col":           "total_cost",
        "parent_col":      "brand",
    },

    "downtime_histogram": {
        "metric_id":       "downtime_histogram",
        "title":           "Downtime Hours Distribution",
        "description":     "Histogram showing how downtime hours are distributed across all events",
        "category":        "Downtime",
        "sql":             """
            SELECT ROUND(downtime_hours, 1) AS downtime_bucket,
                   COUNT(*) AS event_count,
                   brand
            FROM v_maintenance_full
            {where}
            AND downtime_hours IS NOT NULL AND downtime_hours > 0
            GROUP BY ROUND(downtime_hours, 1), brand
            ORDER BY downtime_bucket
        """,
        "dimensions":      ["downtime_bucket", "brand"],
        "measures":        ["event_count"],
        "default_chart":   "histogram",
        "available_charts":["histogram", "table"],
        "x_col":           "downtime_bucket",
        "y_col":           "event_count",
    },
}


# ── Filter Dimension Registry ─────────────────────────────────────────────────

FILTER_DIMENSIONS = {
    "brand":              {"label": "Brand",              "column": "brand",              "type": "select",    "options_sql": "SELECT DISTINCT brand FROM v_maintenance_full ORDER BY brand"},
    "model":              {"label": "Model",              "column": "model",              "type": "select",    "options_sql": "SELECT DISTINCT model FROM v_maintenance_full ORDER BY model"},
    "year":               {"label": "Year",               "column": "year",               "type": "select",    "options_sql": "SELECT DISTINCT year FROM v_maintenance_full ORDER BY year"},
    "month":              {"label": "Month",              "column": "month_name",         "type": "select",    "options": ["January","February","March","April","May","June","July","August","September","October","November","December"]},
    "quarter":            {"label": "Quarter",            "column": "quarter",            "type": "select",    "options": ["1","2","3","4"]},
    "fleet_segment":      {"label": "Fleet Segment",      "column": "fleet_segment",      "type": "select",    "options_sql": "SELECT DISTINCT fleet_segment FROM v_maintenance_full ORDER BY fleet_segment"},
    "component_category": {"label": "Component Category", "column": "component_category", "type": "select",    "options_sql": "SELECT DISTINCT component_category FROM v_maintenance_full ORDER BY component_category"},
    "workshop_state":     {"label": "Workshop State",     "column": "workshop_state",     "type": "select",    "options_sql": "SELECT DISTINCT workshop_state FROM v_maintenance_full ORDER BY workshop_state"},
    "maintenance_type":   {"label": "Maintenance Type",   "column": "maintenance_type",   "type": "select",    "options_sql": "SELECT DISTINCT maintenance_type FROM v_maintenance_full ORDER BY maintenance_type"},
    "criticality_level":  {"label": "Criticality",        "column": "criticality_level",  "type": "select",    "options": ["Critical","High","Medium"]},
    "date_range":         {"label": "Date Range",         "column": "full_date",          "type": "daterange", "options": None},
}


# ── Time Shortcut Registry ────────────────────────────────────────────────────

TIME_SHORTCUTS = {
    "last_month":     {"label": "Last Month",      "sql_filter": "full_date >= DATE_TRUNC('month', (SELECT MAX(full_date) FROM v_maintenance_full)) - INTERVAL 1 MONTH AND full_date < DATE_TRUNC('month', (SELECT MAX(full_date) FROM v_maintenance_full))",  "examples": ["last month","previous month"]},
    "this_month":     {"label": "This Month",      "sql_filter": "full_date >= DATE_TRUNC('month', (SELECT MAX(full_date) FROM v_maintenance_full))",                                                                                                           "examples": ["this month","month to date","MTD"]},
    "last_quarter":   {"label": "Last Quarter",    "sql_filter": "full_date >= DATE_TRUNC('quarter', (SELECT MAX(full_date) FROM v_maintenance_full)) - INTERVAL 3 MONTH AND full_date < DATE_TRUNC('quarter', (SELECT MAX(full_date) FROM v_maintenance_full))", "examples": ["last quarter","previous quarter"]},
    "this_quarter":   {"label": "This Quarter",    "sql_filter": "full_date >= DATE_TRUNC('quarter', (SELECT MAX(full_date) FROM v_maintenance_full))",                                                                                                         "examples": ["this quarter","QTD","quarter to date"]},
    "last_year":      {"label": "Last Year",       "sql_filter": "year = (SELECT MAX(year) FROM v_maintenance_full) - 1",                                                                                                                                       "examples": ["last year","previous year"]},
    "this_year":      {"label": "This Year",       "sql_filter": "year = (SELECT MAX(year) FROM v_maintenance_full)",                                                                                                                                           "examples": ["this year","YTD","year to date"]},
    "last_30_days":   {"label": "Last 30 Days",    "sql_filter": "full_date >= (SELECT MAX(full_date) FROM v_maintenance_full) - INTERVAL 30 DAY",                                                                                                             "examples": ["last 30 days","past 30 days","recent"]},
    "last_90_days":   {"label": "Last 90 Days",    "sql_filter": "full_date >= (SELECT MAX(full_date) FROM v_maintenance_full) - INTERVAL 90 DAY",                                                                                                             "examples": ["last 90 days","last 3 months"]},
    "last_12_months": {"label": "Last 12 Months",  "sql_filter": "full_date >= (SELECT MAX(full_date) FROM v_maintenance_full) - INTERVAL 12 MONTH",                                                                                                          "examples": ["last 12 months","trailing 12","rolling year"]},
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def build_where(filters: dict) -> str:
    if not filters:
        return ""
    clauses = []
    for dim, value in filters.items():
        if not value:
            continue
        if dim == "time_shortcut" and value in TIME_SHORTCUTS:
            clauses.append(TIME_SHORTCUTS[value]["sql_filter"])
            continue
        cfg = FILTER_DIMENSIONS.get(dim)
        if not cfg:
            continue
        col = cfg["column"]
        if cfg["type"] == "daterange" and isinstance(value, list) and len(value) == 2:
            clauses.append(f"{col} BETWEEN '{value[0]}' AND '{value[1]}'")
        elif isinstance(value, list):
            vals = ", ".join(f"'{v}'" for v in value)
            clauses.append(f"{col} IN ({vals})")
        else:
            clauses.append(f"{col} = '{value}'")
    return "WHERE " + " AND ".join(clauses) if clauses else ""


def get_metric_sql(metric_id: str, filters: dict = None) -> str | None:
    metric = METRICS.get(metric_id)
    if not metric:
        return None
    where = build_where(filters or {})
    sql = metric["sql"].replace("{where}", where)
    sql = sql.replace("WHERE \n            AND", "WHERE").replace("WHERE AND", "WHERE")
    return sql.strip()


def get_metrics_index() -> list[dict]:
    return [
        {"metric_id": m["metric_id"], "title": m["title"],
         "description": m["description"], "category": m["category"],
         "available_charts": m.get("available_charts", [])}
        for m in METRICS.values()
    ]


# Appended: stacked bar metric
METRICS["cost_by_brand_and_component"] = {
    "metric_id":       "cost_by_brand_and_component",
    "title":           "Cost by Brand broken down by Component",
    "description":     "Stacked bar showing each brand's total cost split by component category",
    "category":        "Cost",
    "sql":             """
        SELECT brand,
               component_category,
               ROUND(SUM(total_cost_myr), 2) AS total_cost
        FROM v_maintenance_full
        {where}
        GROUP BY brand, component_category
        ORDER BY brand, total_cost DESC
    """,
    "dimensions":      ["brand", "component_category"],
    "measures":        ["total_cost"],
    "default_chart":   "stacked_bar",
    "available_charts":["stacked_bar", "bar", "table", "treemap"],
    "x_col":           "brand",
    "y_col":           "total_cost",
    "group_col":       "component_category",
}

METRICS["downtime_by_brand_and_component"] = {
    "metric_id":       "downtime_by_brand_and_component",
    "title":           "Downtime by Brand broken down by Component",
    "description":     "Stacked bar showing each brand's total downtime split by component category",
    "category":        "Downtime",
    "sql":             """
        SELECT brand,
               component_category,
               ROUND(SUM(downtime_hours), 2) AS total_downtime
        FROM v_maintenance_full
        {where}
        GROUP BY brand, component_category
        ORDER BY brand, total_downtime DESC
    """,
    "dimensions":      ["brand", "component_category"],
    "measures":        ["total_downtime"],
    "default_chart":   "stacked_bar",
    "available_charts":["stacked_bar", "bar", "table"],
    "x_col":           "brand",
    "y_col":           "total_downtime",
    "group_col":       "component_category",
}
