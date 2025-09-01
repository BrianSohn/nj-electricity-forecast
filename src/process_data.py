# src/process_data.py
import os
import json
import pandas as pd
from glob import glob

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_BACKFILL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "backfill")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

def load_latest_raw():
    files = glob(os.path.join(RAW_BACKFILL_DIR, "backfill_*.json"))
    if not files:
        raise FileNotFoundError("No backfill JSON files found in raw/backfill/")
    latest_file = max(files, key=os.path.getctime)
    print(f"Loading {latest_file}")
    with open(latest_file, "r") as f:
        return json.load(f)

def transform(data):
    # The actual data from EIA is under "response" -> "data"
    records = data.get("data", [])

    df = pd.DataFrame(records)

    # Keep only 'period' and 'sales'
    df = df[["period", "sales"]].copy()
    df["period"] = pd.to_datetime(df["period"])
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")

    # Sort by period
    df = df.sort_values("period").reset_index(drop=True)
    return df

def save_processed(df):
    out_path = os.path.join(PROCESSED_DIR, "electricity_backfill_200101_202506.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Saved processed data to {out_path}")

if __name__ == "__main__":
    raw_data = load_latest_raw()
    df = transform(raw_data)
    save_processed(df)
