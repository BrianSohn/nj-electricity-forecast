import os
import json
from datetime import datetime
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import joblib
from statsmodels.tsa.statespace.sarimax import SARIMAX

load_dotenv()

# --- Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
LOGS_DIR = os.path.join(PROJECT_ROOT, "data", "logs")

PROCESSED_FILE = os.path.join(PROCESSED_DIR, "electricity_sales.parquet")
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "pipeline_log.csv")

# --- Config (from your notebook) ---
ORDER = (1, 1, 2)
SEASONAL_ORDER = (1, 1, 1, 12)
MODEL_NAME = "sarima_v1"
MODEL_PKL = os.path.join(MODELS_DIR, f"{MODEL_NAME}.pkl")
MODEL_META = os.path.join(MODELS_DIR , f"{MODEL_NAME}.meta.json")

def log_event(script_name: str, status: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = pd.DataFrame([{"timestamp": timestamp, "script": script_name, "status": status}])
    if os.path.exists(LOG_FILE):
        row.to_csv(LOG_FILE, mode="a", header=False, index=False)
    else:
        row.to_csv(LOG_FILE, mode="w", header=True, index=False)
    print(f"[LOGGED] {status}")

def load_processed() -> pd.DataFrame:
    if not os.path.exists(PROCESSED_FILE):
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}. Run backfill first.")
    df = pd.read_parquet(PROCESSED_FILE)
    df["period"] = pd.to_datetime(df["period"])
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df = df.sort_values("period").reset_index(drop=True)
    return df

def main():
    script = "train_model.py"
    try:
        df = load_processed()
        y = pd.Series(df["sales"].values, index=df["period"])

        model = SARIMAX(
            y,
            order=ORDER,
            seasonal_order=SEASONAL_ORDER,
            enforce_stationarity=False,
            enforce_invertibility=False
        )
        fitted = model.fit(disp=False)

        # Persist the fitted results
        joblib.dump(fitted, MODEL_PKL)

        # Write metadata so the forecasting script knows the last observation included in the model
        meta = {
            "model_name": MODEL_NAME,
            "order": ORDER,
            "seasonal_order": SEASONAL_ORDER,
            "trained_from": str(df["period"].min().date()),
            "trained_through": str(df["period"].max().date()),  # initial training range
            "last_observed": str(df["period"].max().date()),    # same at first, diverges later
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "processed_file": str(PROCESSED_FILE).replace(PROJECT_ROOT+"/", "")
        }
        with open(MODEL_META, "w") as f:
            json.dump(meta, f, indent=2)

        status = (
            f"Trained SARIMA model {MODEL_NAME} "
            f"on {meta['trained_from']} to {meta['trained_through']} "
            f"(last observed {meta['last_observed']})"
        )
        log_event(script, status)

    except Exception as e:
        log_event(script, f"ERROR: {e}")
        raise

if __name__ == "__main__":
    main()
