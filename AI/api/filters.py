"""
ATLAS BI — /filters endpoint
Returns available filter options for each dimension.
"""
from fastapi import APIRouter
from db.duckdb_session import run_query
from metrics import FILTER_DIMENSIONS, TIME_SHORTCUTS

router = APIRouter(prefix="/filters", tags=["filters"])


@router.get("/")
def get_all_filters():
    """Return all filter definitions with their options."""
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
    """Return options for a single filter dimension."""
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
