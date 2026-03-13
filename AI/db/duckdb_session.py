"""
DuckDB session — loads the full star schema + wide view into in-memory DuckDB.
One shared connection for the whole app lifetime.

Tables available:
  v_maintenance_full      — pre-joined wide view (agent queries this)
  fact_maintenance_event  — raw fact table
  dim_truck               — truck dimension
  dim_component           — component dimension
  dim_workshop            — workshop dimension
  dim_date                — date dimension
"""
import os
import duckdb
import pandas as pd

_conn: duckdb.DuckDBPyConnection | None = None
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

TABLES = {
    "v_maintenance_full":     "v_maintenance_full.csv",
    "fact_maintenance_event": "fact_maintenance_event.csv",
    "dim_truck":              "dim_truck.csv",
    "dim_component":          "dim_component.csv",
    "dim_workshop":           "dim_workshop.csv",
    "dim_date":               "dim_date.csv",
}


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(":memory:")
        for table_name, filename in TABLES.items():
            path = os.path.join(DATA_DIR, filename)
            df = pd.read_csv(path, low_memory=False)
            _conn.register(table_name, df)
            print(f"[DuckDB] Loaded {table_name} — {len(df)} rows x {len(df.columns)} cols")
    return _conn


def run_query(sql: str) -> pd.DataFrame:
    return get_conn().execute(sql).df()
