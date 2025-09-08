import os
import json
from datetime import datetime
import pandas as pd
from pathlib import Path
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import joblib

load_dotenv()

# --- Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
LOGS_DIR = os.path.join(PROJECT_ROOT, "data", "logs")
FORECASTS_DIR = os.path.join(PROJECT_ROOT, "data", "forecasts")

PROCESSED_FILE = os.path.join(PROCESSED_DIR, "electricity_sales.parquet")
os.makedirs(FORECASTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "pipeline_log.csv")

# --- Model files ---
MODEL_NAME = "sarima_v1"
MODEL_PKL = os.path.join(MODELS_DIR, f"{MODEL_NAME}.pkl")
MODEL_META = os.path.join(MODELS_DIR , f"{MODEL_NAME}.meta.json")

# --- Forecast sinks (cumulative CSVs) ---
SARIMA_OUT = os.path.join(FORECASTS_DIR, "sarima_forecasts.csv")
SNAIVE_OUT = os.path.join(FORECASTS_DIR, "seasonal_naive_forecasts.csv")

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
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}. Run backfill/ingest first.")
    df = pd.read_parquet(PROCESSED_FILE)
    df["period"] = pd.to_datetime(df["period"])
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df = df.sort_values("period").reset_index(drop=True)
    return df

def load_model():
    if not os.path.exists(MODEL_PKL) or not os.path.exists(MODEL_META):
        raise FileNotFoundError("Model or metadata missing. Train the model first.")
    fitted = joblib.load(MODEL_PKL)
    with open(MODEL_META, "r") as f:
        meta = json.load(f)
    return fitted, meta

def save_model(fitted, meta):
    joblib.dump(fitted, MODEL_PKL)
    with open(MODEL_META, "w") as f:
        json.dump(meta, f, indent=2)

def append_forecast_row(path_: Path, period: pd.Timestamp, model_name: str, forecast: float):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = pd.DataFrame([{
        "period": period.strftime("%Y-%m"),
        "model": model_name,
        "forecast": float(forecast),
        "timestamp": ts
    }])
    header = not os.path.exists(path_)
    row.to_csv(path_, mode="a", header=header, index=False)

def main():
    script = "monthly_forecast.py"
    try:
        # Load data & model
        df = load_processed()
        fitted, meta = load_model()

        # Determine which actuals have been added since the model was last saved
        last_in_model = pd.to_datetime(meta["last_observed"])
        new_obs = df[df["period"] > last_in_model][["period", "sales"]]

        # If no new observations, exit early
        if new_obs.empty:
            log_event(script, f"No new obs available. Model up-to-date through {last_in_model.strftime('%Y-%m')}")
            return


        # Update model state with any new observations (without refitting)
        series_to_append = pd.Series(new_obs["sales"].values, index=new_obs["period"])
        fitted = fitted.append(series_to_append, refit=False)
        
        # Update metadata to reflect the latest observation the model has seen
        meta["last_observed"] = str(df["period"].max().date())
        meta["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_model(fitted, meta)
        log_event(script, f"Model state for {MODEL_NAME} updated with {len(new_obs)} new obs through {meta['last_observed']}")

           
        # Compute next forecast period (1-step ahead)
        latest_period = df["period"].max()
        next_period = (latest_period + relativedelta(months=1))

        # --- SARIMA 1-step-ahead forecast ---
        sarima_fc = fitted.forecast(steps=1)
        sarima_val = float(sarima_fc.values[0])
        append_forecast_row(SARIMA_OUT, next_period, MODEL_NAME, sarima_val)

        # --- Seasonal Naive 1-step-ahead forecast ---
        # next month forecast = value from next_period - 12 months
        ref_period = next_period - relativedelta(months=12)
        df_idx = df.set_index("period")
        if ref_period in df_idx.index:
            snaive_val = float(df_idx.loc[ref_period, "sales"])
            append_forecast_row(SNAIVE_OUT, next_period, "seasonal_naive", snaive_val)
            log_event(script, f"Forecasts written for {next_period.strftime('%Y-%m')} (SARIMA & Seasonal Naive)")
        else:
            # If we don't have 12-month lag, skip naive
            append_forecast_row(SNAIVE_OUT, next_period, "seasonal_naive", float("nan"))
            log_event(script, f"Forecast written for {next_period.strftime('%Y-%m')} (SARIMA). Seasonal naive missing ref {ref_period.strftime('%Y-%m')}")

    except Exception as e:
        log_event(script, f"ERROR: {e}")
        raise

if __name__ == "__main__":
    main()
