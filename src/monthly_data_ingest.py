import os
import json
import requests
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

# --- Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_MONTHLY_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "monthly")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
LOGS_DIR = os.path.join(PROJECT_ROOT, "data", "logs")

os.makedirs(RAW_MONTHLY_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, "pipeline_log.csv")

# --- API details ---
API_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"
API_KEY = os.getenv("EIA_API_KEY")

BASE_PARAMS = {
    "api_key": API_KEY,
    "frequency": "monthly",
    "data[0]": "sales",
    "facets[stateid][]": "NJ",
    "facets[sectorid][]": "RES",
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


# --- Fetch for a specific month ---
def fetch_month(period: str):
    params = BASE_PARAMS.copy()
    params["start"] = period # e.g., "2025-07"
    params["end"] = period # e.g., "2025-07"
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json().get("response", {})


# --- Save raw JSON ---
def save_raw(data, period: str):
    out_path = os.path.join(RAW_MONTHLY_DIR, f"monthly_{period}.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved raw monthly data to {out_path}")
    return out_path


# --- Transform into DataFrame ---
def transform(data):
    records = data.get("data", [])
    if not records:
        return pd.DataFrame(columns=["period", "sales"])
    df = pd.DataFrame(records)
    df = df[["period", "sales"]].copy()
    df["period"] = pd.to_datetime(df["period"])
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    return df


# --- Main workflow ---
if __name__ == "__main__":
    script_name = "monthly_data_ingest.py"

    try:
        # Load latest processed data
        processed_path = os.path.join(PROCESSED_DIR, "electricity_sales.parquet")
        if os.path.exists(processed_path):
            df_existing = pd.read_parquet(processed_path)
            latest_stored = df_existing["period"].max()
        else:
            raise RuntimeError("No processed data found. Run backfill first.")

        # Compute next expected month
        next_month = (latest_stored + relativedelta(months=1)).strftime("%Y-%m")

        # Fetch next expected month
        data = fetch_month(next_month)
        df_new = transform(data)

        if df_new.empty:
            status = f"No data update. Expected {next_month}, but none available."
            log_event(script_name, status)

        else:
            # Save raw JSON for this month
            save_raw(data, next_month.replace("-", ""))

            # Append to processed parquet & csv
            df_updated = pd.concat([df_existing, df_new], ignore_index=True)
            df_updated = df_updated.sort_values("period")

            parquet_path = os.path.join(PROCESSED_DIR, "electricity_sales.parquet")
            csv_path = os.path.join(PROCESSED_DIR, "electricity_sales.csv")
            df_updated.to_parquet(parquet_path, index=False)
            df_updated.to_csv(csv_path, index=False)

            status = f"Data updated with new row for {next_month}"
            log_event(script_name, status)

    except Exception as e:
        status = f"ERROR: {str(e)}"
        log_event(script_name, status)
        raise
