"""
ATLAS BI — /filters endpoint
Filter dimensions and time shortcuts — no longer depends on metrics.py
"""
from fastapi import APIRouter
from db.duckdb_session import run_query

router = APIRouter(prefix="/filters", tags=["filters"])

# ── Filter dimensions (moved from metrics.py) ─────────────────────────────────

FILTER_DIMENSIONS = {
    "brand":              {"label": "Brand",              "column": "brand",              "type": "select", "cast": None,  "options_sql": "SELECT DISTINCT brand FROM v_maintenance_full ORDER BY brand"},
    "year":               {"label": "Year",               "column": "year",               "type": "select", "cast": "int", "options_sql": "SELECT DISTINCT year FROM v_maintenance_full ORDER BY year"},
    "quarter":            {"label": "Quarter",            "column": "month",              "type": "select", "cast": "int", "quarter": True,
                           "options": ["1","2","3","4"]},
    "fleet_segment":      {"label": "Fleet Segment",      "column": "fleet_segment",      "type": "select", "cast": None,  "options_sql": "SELECT DISTINCT fleet_segment FROM v_maintenance_full ORDER BY fleet_segment"},
    "maintenance_type":   {"label": "Maintenance Type",   "column": "maintenance_type",   "type": "select", "cast": None,  "options_sql": "SELECT DISTINCT maintenance_type FROM v_maintenance_full ORDER BY maintenance_type"},
    "criticality_level":  {"label": "Criticality Level",  "column": "criticality_level",  "type": "select", "cast": None,  "options": ["Critical","High","Medium","Low"]},
    "workshop_type":      {"label": "Workshop Type",      "column": "workshop_type",      "type": "select", "cast": None,  "options_sql": "SELECT DISTINCT workshop_type FROM v_maintenance_full ORDER BY workshop_type"},
    "region":             {"label": "Region",             "column": "region",             "type": "select", "cast": None,  "options_sql": "SELECT DISTINCT region FROM v_maintenance_full ORDER BY region"},
    "component_category": {"label": "Component",          "column": "component_category", "type": "select", "cast": None,  "options_sql": "SELECT DISTINCT component_category FROM v_maintenance_full ORDER BY component_category"},
}

TIME_SHORTCUTS = {
    "last_month":     {"label": "Last Month",     "sql_filter": "year = (SELECT MAX(year) FROM v_maintenance_full) AND month = (SELECT MAX(month) FROM v_maintenance_full) - 1"},
    "this_month":     {"label": "This Month",     "sql_filter": "year = (SELECT MAX(year) FROM v_maintenance_full) AND month = (SELECT MAX(month) FROM v_maintenance_full)"},
    "last_quarter":   {"label": "Last Quarter",   "sql_filter": "year_quarter = (SELECT MAX(year_quarter) FROM v_maintenance_full)"},
    "last_year":      {"label": "Last Year",      "sql_filter": "year = (SELECT MAX(year) FROM v_maintenance_full) - 1"},
    "this_year":      {"label": "This Year",      "sql_filter": "year = (SELECT MAX(year) FROM v_maintenance_full)"},
    "last_12_months": {"label": "Last 12 Months", "sql_filter": "service_date >= DATE_ADD(DATE '2024-12-31', INTERVAL -12 MONTH)"},
}


QUARTER_MONTHS = {
    "1": [1,2,3], "2": [4,5,6], "3": [7,8,9], "4": [10,11,12]
}

def build_where_from_filters(filters: dict) -> str:
    """Build a SQL WHERE clause from a filters dict."""
    if not filters:
        return ""
    clauses = []
    for dim, value in filters.items():
        if value is None or value == "" or value == []:
            continue
        if dim == "time_shortcut" and value in TIME_SHORTCUTS:
            clauses.append(TIME_SHORTCUTS[value]["sql_filter"])
            continue
        cfg = FILTER_DIMENSIONS.get(dim)
        if not cfg:
            # Unknown dim — pass through as direct column filter
            if isinstance(value, list):
                vals = ", ".join(f"'{v}'" for v in value)
                clauses.append(f"{dim} IN ({vals})")
            else:
                clauses.append(f"{dim} = '{value}'")
            continue

        cast = cfg.get("cast")
        is_quarter = cfg.get("quarter", False)

        def fmt(v):
            if cast == "int":
                try: return str(int(v))
                except: return f"'{v}'"
            return f"'{v}'"

        values_list = value if isinstance(value, list) else [value]

        if is_quarter:
            # Convert quarter numbers to month ranges
            months = []
            for q in values_list:
                months.extend(QUARTER_MONTHS.get(str(q), []))
            if months:
                clauses.append(f"month IN ({', '.join(str(m) for m in sorted(set(months)))})")
        elif cfg["type"] == "daterange" and isinstance(value, list) and len(value) == 2:
            clauses.append(f"{cfg['column']} BETWEEN '{value[0]}' AND '{value[1]}'")
        elif isinstance(value, list):
            vals = ", ".join(fmt(v) for v in value)
            clauses.append(f"{cfg['column']} IN ({vals})")
        else:
            clauses.append(f"{cfg['column']} = {fmt(value)}")

    return "WHERE " + " AND ".join(clauses) if clauses else ""


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/")
def get_all_filters():
    result = {}
    for dim_id, cfg in FILTER_DIMENSIONS.items():
        options = cfg.get("options")
        if options is None and cfg.get("options_sql"):
            try:
                df = run_query(cfg["options_sql"])
                options = df.iloc[:, 0].astype(str).tolist()
            except Exception:
                options = []
        result[dim_id] = {
            "label":   cfg["label"],
            "type":    cfg["type"],
            "column":  cfg["column"],
            "options": options,
        }
    return {"filters": result, "time_shortcuts": TIME_SHORTCUTS}


@router.get("/{dim_id}")
def get_filter_options(dim_id: str):
    cfg = FILTER_DIMENSIONS.get(dim_id)
    if not cfg:
        return {"error": f"Filter '{dim_id}' not found"}
    options = cfg.get("options")
    if options is None and cfg.get("options_sql"):
        try:
            df = run_query(cfg["options_sql"])
            options = df.iloc[:, 0].astype(str).tolist()
        except Exception:
            options = []
    return {"dim_id": dim_id, "label": cfg["label"], "type": cfg["type"], "options": options}
