import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_BACKFILL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "backfill")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
LOGS_DIR = os.path.join(PROJECT_ROOT, "data", "logs")

os.makedirs(RAW_BACKFILL_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, "pipeline_log.csv")

# API details
API_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"
API_KEY = os.getenv("EIA_API_KEY")

# Parameters
PARAMS = {
    "api_key": API_KEY,
    "frequency": "monthly",
    "data[0]": "sales",
    "facets[stateid][]": "NJ",
    "facets[sectorid][]": "RES",
    "start": "2001-01",
    "end": "2025-06",
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": 0,
    "length": 5000
}

# --- Utility: logging ---
def log_event(script_name: str, status: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = pd.DataFrame([{
        "timestamp": timestamp,
        "script": script_name,
        "status": status
    }])
    if os.path.exists(LOG_FILE):
        log_entry.to_csv(LOG_FILE, mode="a", header=False, index=False)
    else:
        log_entry.to_csv(LOG_FILE, mode="w", header=True, index=False)
    print(f"[LOGGED] {status}")

# --- Fetch historical data ---
def fetch_data():
    response = requests.get(API_URL, params=PARAMS)
    response.raise_for_status()
    return response.json().get("response", {})

# --- Save raw JSON ---
def save_raw(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(RAW_BACKFILL_DIR, f"backfill_{timestamp}_200101_202506.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved raw backfill data to {out_path}")
    return out_path

# --- Transform ---
def transform(data):
    records = data.get("data", [])
    df = pd.DataFrame(records)
    df = df[["period", "sales"]].copy()
    df["period"] = pd.to_datetime(df["period"])
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df = df.sort_values("period").reset_index(drop=True)
    return df

# --- Save processed ---
def save_processed(df):
    parquet_path = os.path.join(PROCESSED_DIR, "electricity_sales.parquet")
    csv_path = os.path.join(PROCESSED_DIR, "electricity_sales.csv")
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    print(f"Saved processed data to {parquet_path} and {csv_path}")

# --- Main workflow ---
if __name__ == "__main__":
    script_name = "backfill_historical.py"

    try:
        data = fetch_data()
        save_raw(data)
        df = transform(data)
        save_processed(df)
        status = f"Backfill completed. {len(df)} rows from {df['period'].min().strftime('%Y-%m')} to {df['period'].max().strftime('%Y-%m')}"
        log_event(script_name, status)
    except Exception as e:
        status = f"ERROR: {str(e)}"
        log_event(script_name, status)
        raise