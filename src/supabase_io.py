# src/supabase_io.py
"""
Supabase I/O layer (SQLAlchemy + Supabase Storage)
- All DB interactions use SQLAlchemy engine (SUPABASE_DB_URL)
- Object storage (raw JSONs, model binaries) uses Supabase Storage client
- Schema expectations (created by you):
    - electricity_sales (period PK, sales)
    - logs (id, timestamp, script, status, details)
    - forecasts (id, period, model_id -> models.id, forecast, timestamp)
    - models (id, model_name, saved_location, trained_from, trained_through,
              last_observed, params JSONB, created_at, updated_at)
- NOTE: This module expects RLS to be managed separately. We assume scripts
  use the service role key (SUPABASE_KEY) which bypasses RLS.
"""

import os
import json
import pickle
from datetime import datetime, date
from typing import Optional, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration ----------
DB_URL = os.getenv("SUPABASE_DB_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if DB_URL is None or SUPABASE_URL is None or SUPABASE_KEY is None:
    raise RuntimeError("Please configure SUPABASE_DB_URL, SUPABASE_URL and SUPABASE_KEY in .env")

# create SQLAlchemy engine
engine = create_engine(DB_URL, future=True)

# supabase client for object storage
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# storage buckets (create these in Supabase dashboard)
RAW_BUCKET = "raw-data"
MODEL_BUCKET = "models"


# ---------- Logging ----------
def log_event(script: str, status: str, details: Optional[str] = None) -> None:
    """
    Insert a log row into the logs table.
    status should be one of: 'success', 'warning', 'error', 'no_update'
    """
    ts = datetime.now()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO logs (timestamp, script, status, details)
                VALUES (:ts, :script, :status, :details)
            """),
            {"ts": ts, "script": script, "status": status, "details": details},
        )
    print(f"[LOG] {ts.isoformat()} | {script} | {status} | {details}")


# ---------- Electricity processed data ----------
def upsert_electricity_sales(df: pd.DataFrame) -> None:
    """
    Upsert rows of electricity sales into the electricity_sales table.
    df must contain columns: 'period' (datetime/date) and 'sales' (numeric).
    This uses INSERT ... ON CONFLICT (period) DO UPDATE so repeated runs are idempotent.
    """
    if df.empty:
        return

    # Normalize types
    df = df.copy()
    df["period"] = pd.to_datetime(df["period"]).dt.date
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO electricity_sales (period, sales)
                    VALUES (:period, :sales)
                    ON CONFLICT (period) DO UPDATE SET sales = EXCLUDED.sales
                """),
                {"period": row["period"], "sales": float(row["sales"]) if pd.notnull(row["sales"]) else None},
            )
    print(f"[DATA] Upserted {len(df)} rows into electricity_sales")


def load_electricity_sales() -> pd.DataFrame:
    """Return the entire electricity_sales table as a DataFrame, sorted by period."""
    with engine.begin() as conn:
        df = pd.read_sql(text("SELECT period, sales FROM electricity_sales ORDER BY period"), conn)
    if not df.empty:
        df["period"] = pd.to_datetime(df["period"])
        df["sales"] = pd.to_numeric(df["sales"])
    return df


def get_latest_period() -> Optional[date]:
    """Return the max(period) in electricity_sales or None if empty."""
    with engine.begin() as conn:
        res = conn.execute(text("SELECT MAX(period) as maxp FROM electricity_sales")).mappings().first()
    if res and res["maxp"]:
        return res["maxp"]
    return None


# ---------- Models table helpers ----------
def get_model_row_by_name(model_name: str) -> Optional[dict]:
    """Return model row as dict or None if not found."""
    with engine.begin() as conn:
        row = conn.execute(text("SELECT * FROM models WHERE model_name = :name"), {"name": model_name}).mappings().first()
    return dict(row) if row else None


def ensure_model_row(model_name: str, created_at: Optional[datetime] = None) -> dict:
    """
    Ensure a model row exists. If not, create a minimal placeholder.
    Returns the model row (as dict).
    Useful for benchmark models (seasonal_naive) that have no binary.
    """
    existing = get_model_row_by_name(model_name)
    if existing:
        return existing

    created_at = created_at or datetime.now()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO models (model_name, created_at, updated_at)
                VALUES (:name, :created_at, :updated_at)
            """),
            {"name": model_name, "created_at": created_at, "updated_at": created_at},
        )
        row = conn.execute(text("SELECT * FROM models WHERE model_name = :name"), {"name": model_name}).mappings().first()
    return dict(row)


def upsert_model_metadata(model_name: str, meta: Dict[str, Any]) -> None:
    """
    Insert or update metadata in the models table.
    meta may contain: trained_from (date/string), trained_through, last_observed, params (dict), saved_location (text)
    We upsert by model_name (which is unique by convention).
    """
    now = datetime.now()
    params_json = json.dumps(meta.get("params")) if meta.get("params") is not None else None

    # Use upsert pattern: if model_name exists -> update, else insert
    with engine.begin() as conn:
        # If exists update
        exists = conn.execute(text("SELECT id FROM models WHERE model_name=:name"), {"name": model_name}).fetchone()
        if exists:
            conn.execute(
                text("""
                    UPDATE models
                    SET saved_location = COALESCE(:saved_location, saved_location),
                        trained_from = COALESCE(:trained_from, trained_from),
                        trained_through = COALESCE(:trained_through, trained_through),
                        last_observed = COALESCE(:last_observed, last_observed),
                        params = COALESCE(:params, params),
                        updated_at = :updated_at
                    WHERE model_name = :name
                """),
                {
                    "saved_location": meta.get("saved_location"),
                    "trained_from": meta.get("trained_from"),
                    "trained_through": meta.get("trained_through"),
                    "last_observed": meta.get("last_observed"),
                    "params": params_json,
                    "updated_at": now,
                    "name": model_name,
                },
            )
        else:
            conn.execute(
                text("""
                    INSERT INTO models
                        (model_name, saved_location, trained_from, trained_through, last_observed, params, created_at, updated_at)
                    VALUES
                        (:name, :saved_location, :trained_from, :trained_through, :last_observed, :params::jsonb, :created_at, :updated_at)
                """),
                {
                    "name": model_name,
                    "saved_location": meta.get("saved_location"),
                    "trained_from": meta.get("trained_from"),
                    "trained_through": meta.get("trained_through"),
                    "last_observed": meta.get("last_observed"),
                    "params": params_json,
                    "created_at": now,
                    "updated_at": now,
                },
            )


def load_model_metadata(model_name: str) -> Optional[dict]:
    """Return metadata dict for the model_name or None if missing."""
    with engine.begin() as conn:
        row = conn.execute(text("SELECT * FROM models WHERE model_name=:name"), {"name": model_name}).mappings().first()
    return dict(row) if row else None


# ---------- Model binary storage (pickle) ----------
def save_model_binary(model_obj: object, model_name: str, remote_path: Optional[str] = None) -> str:
    """
    Serialize the model using pickle.dumps and upload bytes to Supabase Storage.
    - model_name: logical name (e.g., 'sarima_v1')
    - remote_path: optional explicit path in bucket, default: '{model_name}.pkl'
    Returns the saved_location (path) used for metadata.
    """
    remote_path = remote_path or f"{model_name}.pkl"
    data = pickle.dumps(model_obj)
    # Upload bytes to Supabase storage
    # NOTE: supabase.storage.from_(bucket).upload signature varies by library version.
    # We attempt to upload bytes directly (this is standard accepted usage).
    supabase.storage.from_(MODEL_BUCKET).upload(remote_path, data, {"content-type": "application/octet-stream"})
    return remote_path


def load_model_binary(model_name: str, remote_path: Optional[str] = None):
    """
    Download model bytes from storage and unpickle.
    - remote_path: optional; if omitted, will attempt to get location from models table.
    Returns (model_obj, metadata_dict)
    """
    if remote_path is None:
        meta = load_model_metadata(model_name)
        if meta is None or not meta.get("saved_location"):
            raise FileNotFoundError(f"No saved_location found for model {model_name}")
        remote_path = meta["saved_location"]

    # Download bytes
    data = supabase.storage.from_(MODEL_BUCKET).download(remote_path)
    model_obj = pickle.loads(data)
    meta = load_model_metadata(model_name)
    return model_obj, meta


# ---------- Forecasts ----------
def append_forecast_record(period_value: date, model_name: str, forecast_value: Optional[float]) -> None:
    """
    Append a forecast into the forecasts table.
    - period_value: date for which the forecast is made (use first-of-month date or string 'YYYY-MM-DD')
    - model_name: lookup model id by name (ensures model row exists)
    - forecast_value: numeric or None
    """
    # Ensure model exists (create minimal if needed)
    model_row = get_model_row_by_name(model_name)
    if not model_row:
        model_row = ensure_model_row(model_name)

    model_id = model_row["id"]

    ts = datetime.now()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO forecasts (period, model_id, forecast, timestamp)
                VALUES (:period, :model_id, :forecast, :ts)
            """),
            {"period": period_value, "model_id": model_id, "forecast": forecast_value, "ts": ts},
        )

    print(f"[FORECAST] model={model_name} period={period_value} forecast={forecast_value}")


# ---------- Raw JSON storage ----------
def save_raw_json(raw_json: dict, remote_path: str) -> None:
    """
    Save raw JSON dictionary to Supabase Storage under RAW_BUCKET.
    remote_path example: 'backfill/2001_2002.json' or 'monthly/202507.json'
    """
    raw_bytes = json.dumps(raw_json, default=str).encode("utf-8")
    supabase.storage.from_(RAW_BUCKET).upload(remote_path, raw_bytes, {"content-type": "application/json"})
    print(f"[RAW] Uploaded raw JSON to {RAW_BUCKET}/{remote_path}")


# ---------- Utility ----------
def iso_month_str_to_date(period_str: str) -> date:
    """
    Convert 'YYYY-MM' or 'YYYY-MM-DD' or pandas Timestamp to Python date (first day of month if 'YYYY-MM').
    """
    if isinstance(period_str, (pd.Timestamp, datetime)):
        return pd.to_datetime(period_str).date()
    if len(period_str) == 7:  # 'YYYY-MM'
        return pd.to_datetime(period_str + "-01").date()
    return pd.to_datetime(period_str).date()
