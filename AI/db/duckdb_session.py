"""
ATLAS BI — DuckDB session
Auto-discovers ALL files in AI/data/, loads them with majority-type coercion,
generates a dynamic SCHEMA_GUIDE, detects data quality issues, and watches
for file changes to reload automatically.

Supported formats: .csv  .parquet  .json  .jsonl  .xlsx  .xls  .tsv
"""
import os
import re
import json
import hashlib
import threading
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

# ── Paths ──────────────────────────────────────────────────────────────────────
_conn:          duckdb.DuckDBPyConnection | None = None
_schema_guide:  str  = ""
_dq_warnings:   list = []          # data quality warnings surfaced in chat
_file_hashes:   dict = {}          # filename → md5 hash for change detection
_tables_meta:   dict = {}          # table_name → {cols, dtypes, stats}
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

SUPPORTED_EXT = {".csv", ".tsv", ".parquet", ".json", ".jsonl", ".xlsx", ".xls"}


# ── File loading ───────────────────────────────────────────────────────────────

def _load_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".csv"}:
        return pd.read_csv(path, low_memory=False)
    elif ext in {".tsv"}:
        return pd.read_csv(path, sep="\t", low_memory=False)
    elif ext == ".parquet":
        return pd.read_parquet(path)
    elif ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    elif ext == ".json":
        try:
            return pd.read_json(path)
        except Exception:
            return pd.read_json(path, lines=True)
    elif ext == ".jsonl":
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported file type: {ext}")


def _file_hash(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Majority-type coercion ─────────────────────────────────────────────────────

def _coerce_column(series: pd.Series) -> tuple[pd.Series, str, list]:
    """
    Detect majority type in a column and coerce to it.
    Returns (coerced_series, detected_type, quality_warnings).
    """
    warnings = []
    name = series.name
    total = len(series)
    non_null = series.dropna()
    if len(non_null) == 0:
        return series, "TEXT", [f"Column '{name}': all values are null"]

    # Try numeric
    numeric_coerced = pd.to_numeric(non_null, errors="coerce")
    numeric_count = numeric_coerced.notna().sum()

    # Try datetime
    date_coerced = pd.to_datetime(non_null, errors="coerce", infer_datetime_format=True)
    date_count = date_coerced.notna().sum()

    # Try boolean
    bool_vals = {"true", "false", "1", "0", "yes", "no", "t", "f"}
    bool_count = non_null.astype(str).str.lower().isin(bool_vals).sum()

    majority = 0.6  # 60% threshold to declare a type

    if numeric_count / len(non_null) >= majority:
        bad = total - numeric_count
        coerced = pd.to_numeric(series, errors="coerce")
        # Determine int vs float
        if (coerced.dropna() % 1 == 0).all():
            coerced = coerced.fillna(0).astype("int64")
            dtype = "INTEGER"
        else:
            coerced = coerced.fillna(0).astype("float64")
            dtype = "FLOAT"
        if bad > 0:
            warnings.append(f"Column '{name}': {bad} value(s) could not be parsed as {dtype} and were replaced with 0")
        return coerced, dtype, warnings

    elif date_count / len(non_null) >= majority:
        bad = total - date_count
        coerced = pd.to_datetime(series, errors="coerce")
        if bad > 0:
            warnings.append(f"Column '{name}': {bad} value(s) could not be parsed as DATE and were set to null")
        return coerced, "DATE", warnings

    elif bool_count / len(non_null) >= majority:
        def to_bool(v):
            if pd.isna(v): return False
            s = str(v).lower()
            return s in {"true", "1", "yes", "t"}
        return series.apply(to_bool), "BOOLEAN", warnings

    else:
        # Text — just clean nulls
        return series.fillna(""), "TEXT", warnings


def _smart_load(df: pd.DataFrame, table_name: str) -> tuple[pd.DataFrame, list]:
    """Apply majority-type coercion to all columns. Return cleaned df + warnings."""
    all_warnings = []
    for col in df.columns:
        coerced, dtype, col_warnings = _coerce_column(df[col])
        df[col] = coerced
        all_warnings.extend(col_warnings)
    return df, all_warnings


# ── Schema guide generation ────────────────────────────────────────────────────

def _describe_column(df: pd.DataFrame, col: str) -> str:
    """Generate a schema guide line for one column."""
    series = df[col]
    dtype_str = str(series.dtype)

    if pd.api.types.is_integer_dtype(series):
        mn, mx = int(series.min()), int(series.max())
        return f"  {col:<30} INTEGER   — range {mn}–{mx}"

    elif pd.api.types.is_float_dtype(series):
        mn, mx = round(float(series.min()), 2), round(float(series.max()), 2)
        return f"  {col:<30} FLOAT     — range {mn}–{mx}"

    elif pd.api.types.is_datetime64_any_dtype(series):
        mn = series.min().strftime("%Y-%m-%d") if not pd.isna(series.min()) else "?"
        mx = series.max().strftime("%Y-%m-%d") if not pd.isna(series.max()) else "?"
        return f"  {col:<30} DATE      — range {mn} to {mx}"

    elif pd.api.types.is_bool_dtype(series):
        return f"  {col:<30} BOOLEAN"

    else:
        # Text — show unique count and sample values
        unique = series.nunique()
        if unique <= 20:
            samples = sorted(series.dropna().unique().tolist())[:12]
            sample_str = ", ".join(str(s) for s in samples)
            return f"  {col:<30} TEXT      — {unique} values: {sample_str}"
        else:
            samples = series.dropna().unique()[:5].tolist()
            sample_str = ", ".join(str(s) for s in samples)
            return f"  {col:<30} TEXT      — {unique} unique values (e.g. {sample_str})"


def _build_schema_guide(tables: dict) -> str:
    """Build a complete SCHEMA_GUIDE string from loaded tables."""
    lines = [
        "AVAILABLE TABLES (query using DuckDB SQL syntax):",
        "",
        "CRITICAL SQL RULES:",
        "- NEVER invent column names — only use names listed below",
        "- NEVER use strftime() — use pre-extracted year/month columns directly",
        "- Use ROUND(value, 2) for monetary/float values",
        "- Use NULLIF(denominator, 0) to avoid division by zero",
        "- GROUP BY all non-aggregated SELECT columns",
        "- For time series: ORDER BY time column ASC",
        "- For category comparisons: ORDER BY measure DESC",
        "- DEFAULT: show ALL data unless user explicitly requests a filter",
        "- NEVER add WHERE on time/category columns unless user specifically asked",
        "- No LIMIT unless user asks for top-N",
        "",
    ]

    for table_name, meta in tables.items():
        df = meta["df"]
        n_rows, n_cols = len(df), len(df.columns)
        lines.append(f"TABLE: {table_name}  ({n_rows:,} rows × {n_cols} columns)")
        lines.append("COLUMNS:")
        for col in df.columns:
            lines.append(_describe_column(df, col))
        lines.append("")

    return "\n".join(lines)


# ── Connection management ──────────────────────────────────────────────────────

def _discover_files() -> dict:
    """Find all supported files in DATA_DIR. Returns {table_name: filepath}."""
    result = {}
    if not os.path.exists(DATA_DIR):
        print(f"[DuckDB] DATA_DIR not found: {DATA_DIR}")
        return result
    for fname in sorted(os.listdir(DATA_DIR)):
        ext = os.path.splitext(fname)[1].lower()
        if ext in SUPPORTED_EXT:
            table_name = re.sub(r"[^a-zA-Z0-9_]", "_", os.path.splitext(fname)[0])
            result[table_name] = os.path.join(DATA_DIR, fname)
    return result


def _load_all_tables(conn: duckdb.DuckDBPyConnection) -> tuple[dict, list, dict]:
    """Load all files, return (tables_meta, all_warnings, file_hashes)."""
    files = _discover_files()
    tables = {}
    all_warnings = []
    hashes = {}

    for table_name, path in files.items():
        try:
            raw_df = _load_file(path)
            df, warnings = _smart_load(raw_df.copy(), table_name)
            conn.register(table_name, df)
            tables[table_name] = {"df": df, "path": path}
            all_warnings.extend(warnings)
            hashes[path] = _file_hash(path)
            print(f"[DuckDB] Loaded {table_name} — {len(df):,} rows × {len(df.columns)} cols")
        except Exception as e:
            print(f"[DuckDB] Failed to load {path}: {e}")
            all_warnings.append(f"⚠️ Could not load file '{os.path.basename(path)}': {e}")

    return tables, all_warnings, hashes


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn, _schema_guide, _dq_warnings, _file_hashes, _tables_meta
    if _conn is None:
        _conn = duckdb.connect(":memory:")
        _tables_meta, _dq_warnings, _file_hashes = _load_all_tables(_conn)
        _schema_guide = _build_schema_guide(_tables_meta)
        _start_file_watcher()
    return _conn


def run_query(sql: str) -> pd.DataFrame:
    return get_conn().execute(sql).df()


# ── Schema guide access ────────────────────────────────────────────────────────

def get_schema_guide() -> str:
    """Return the current dynamic schema guide."""
    get_conn()  # ensure loaded
    return _schema_guide


def get_dq_warnings() -> list:
    """Return data quality warnings from the last load."""
    get_conn()
    return _dq_warnings.copy()


def get_table_names() -> list:
    """Return list of available table names."""
    get_conn()
    return list(_tables_meta.keys())


# ── Reload (for /data/reload endpoint and file watcher) ───────────────────────

def reload_data() -> dict:
    """Reload all files, regenerate schema. Returns summary."""
    global _conn, _schema_guide, _dq_warnings, _file_hashes, _tables_meta
    print("[DuckDB] Reloading all data...")
    _conn = duckdb.connect(":memory:")
    _tables_meta, _dq_warnings, _file_hashes = _load_all_tables(_conn)
    _schema_guide = _build_schema_guide(_tables_meta)
    print(f"[DuckDB] Reload complete — {len(_tables_meta)} tables, {len(_dq_warnings)} warnings")
    return {
        "tables": list(_tables_meta.keys()),
        "warnings": _dq_warnings,
        "schema_preview": _schema_guide[:500],
    }


# ── File watcher ───────────────────────────────────────────────────────────────

def _check_for_changes() -> bool:
    """Return True if any file in DATA_DIR has changed since last load."""
    files = _discover_files()
    current_paths = set(files.values())
    known_paths   = set(_file_hashes.keys())

    if current_paths != known_paths:
        return True  # file added or removed

    for path in current_paths:
        try:
            if _file_hash(path) != _file_hashes.get(path, ""):
                return True
        except Exception:
            pass
    return False


def _watch_loop(interval: int = 30):
    """Background thread: check for file changes every `interval` seconds."""
    import time
    while True:
        time.sleep(interval)
        try:
            if _check_for_changes():
                print("[DuckDB] File change detected — reloading data...")
                reload_data()
        except Exception as e:
            print(f"[DuckDB] Watcher error: {e}")


def _start_file_watcher():
    """Start background file watcher thread (daemon — dies with main process)."""
    t = threading.Thread(target=_watch_loop, args=(30,), daemon=True)
    t.start()
    print("[DuckDB] File watcher started (30s interval)")
