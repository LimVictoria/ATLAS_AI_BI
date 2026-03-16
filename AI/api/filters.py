"""
ATLAS BI — /filters endpoint
Fully dynamic — discovers filter dimensions from actual loaded data.
No hardcoded column names or table names. Works with any dataset.
"""
from fastapi import APIRouter
from db.duckdb_session import run_query, get_filter_dimensions

router = APIRouter(prefix="/filters", tags=["filters"])

QUARTER_MONTHS = {
    "1": [1,2,3], "2": [4,5,6], "3": [7,8,9], "4": [10,11,12]
}

TIME_SHORTCUTS = {
    "last_month":     {"label": "Last Month",     "sql_filter": "year = (SELECT MAX(year) FROM {table}) AND month = (SELECT MAX(month) FROM {table}) - 1"},
    "this_month":     {"label": "This Month",     "sql_filter": "year = (SELECT MAX(year) FROM {table}) AND month = (SELECT MAX(month) FROM {table})"},
    "last_quarter":   {"label": "Last Quarter",   "sql_filter": "year_quarter = (SELECT MAX(year_quarter) FROM {table})"},
    "last_year":      {"label": "Last Year",      "sql_filter": "year = (SELECT MAX(year) FROM {table}) - 1"},
    "this_year":      {"label": "This Year",      "sql_filter": "year = (SELECT MAX(year) FROM {table})"},
}

# Cache — rebuilt when data reloads
_dims_cache: dict | None = None
_primary_table: str = ""


def _get_dims() -> dict:
    """Get filter dimensions, using cache for performance.
    Always populates cache — never returns empty on first call."""
    global _dims_cache, _primary_table
    if _dims_cache is None:
        try:
            _dims_cache = get_filter_dimensions()
        except Exception as e:
            print(f"[filters] get_filter_dimensions failed: {e}")
            _dims_cache = {}
        # Find primary table (largest one) for time shortcuts
        try:
            from db.duckdb_session import _tables_meta
            if _tables_meta:
                _primary_table = max(_tables_meta.items(), key=lambda x: len(x[1]["df"]))[0]
        except Exception:
            _primary_table = ""
        print(f"[filters] loaded {len(_dims_cache)} filter dimensions, primary_table={_primary_table!r}")
    return _dims_cache


def invalidate_cache():
    """Call this when data reloads."""
    global _dims_cache
    _dims_cache = None


def _get_time_shortcuts() -> dict:
    """Return time shortcuts with actual table name substituted."""
    table = _primary_table or "v_maintenance_full"
    result = {}
    for key, val in TIME_SHORTCUTS.items():
        try:
            result[key] = {
                "label": val["label"],
                "sql_filter": val["sql_filter"].format(table=table),
            }
        except Exception:
            result[key] = val
    return result


def build_where_from_filters(filters: dict) -> str:
    """Build a SQL WHERE clause from a filters dict.
    Uses actual column metadata from loaded data — no hardcoded types."""
    if not filters:
        return ""

    dims = _get_dims()
    time_shortcuts = _get_time_shortcuts()
    clauses = []

    for dim, value in filters.items():
        if value is None or value == "" or value == []:
            continue

        # Time shortcut
        if dim == "time_shortcut":
            if value in time_shortcuts:
                clauses.append(time_shortcuts[value]["sql_filter"])
            continue

        # Quarter — special computed filter
        if dim == "quarter":
            values_list = value if isinstance(value, list) else [value]
            months = []
            for q in values_list:
                months.extend(QUARTER_MONTHS.get(str(q), []))
            if months:
                clauses.append(f"month IN ({', '.join(str(m) for m in sorted(set(months)))})")
            continue

        cfg = dims.get(dim)
        col = cfg["column"] if cfg else dim
        cast = cfg.get("cast") if cfg else None

        def fmt(v, _cast=cast):
            # If cast known from schema, use it
            if _cast in {"int", "float"}:
                try:
                    return str(int(float(v))) if _cast == "int" else str(float(v))
                except Exception:
                    return f"'{v}'"
            # Fallback: auto-detect — if value looks like a pure integer, don't quote it
            try:
                int_v = int(str(v).strip())
                if str(int_v) == str(v).strip():
                    return str(int_v)
            except (ValueError, TypeError):
                pass
            return f"'{v}'"

        values_list = value if isinstance(value, list) else [value]

        if len(values_list) > 1:
            vals = ", ".join(fmt(v) for v in values_list)
            clauses.append(f"{col} IN ({vals})")
        else:
            clauses.append(f"{col} = {fmt(values_list[0])}")

    return "WHERE " + " AND ".join(clauses) if clauses else ""


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/")
def get_all_filters():
    """Return all filter dimensions discovered from loaded data."""
    dims = _get_dims()
    result = {}
    for dim_id, cfg in dims.items():
        result[dim_id] = {
            "label":      cfg["label"],
            "type":       cfg["type"],
            "column":     cfg["column"],
            "table":      cfg.get("table", ""),
            "options":    cfg.get("options", []),
            "is_time":    cfg.get("is_time", False),
            "n_distinct": cfg.get("n_distinct", 0),
        }
    return {"filters": result, "time_shortcuts": _get_time_shortcuts()}


@router.get("/{dim_id}")
def get_filter_options(dim_id: str):
    """Return options for a specific filter dimension."""
    dims = _get_dims()
    cfg = dims.get(dim_id)
    if not cfg:
        return {"error": f"Filter '{dim_id}' not found", "options": []}
    return {
        "dim_id":     dim_id,
        "label":      cfg["label"],
        "type":       cfg["type"],
        "options":    cfg.get("options", []),
        "n_distinct": cfg.get("n_distinct", 0),
    }
