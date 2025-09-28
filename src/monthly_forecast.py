# src/monthly_forecast.py
"""
Monthly forecasting:
- Load latest model binary + metadata
- If there are new observed months since model.last_observed:
    - Append the new observations to the model state (refit=False)
    - Save updated binary back to storage
    - Update model metadata.last_observed and updated_at
    - Produce 1-step-ahead forecast from the updated model (SARIMA)
    - Produce 1-step seasonal-naive forecast
    - Insert both forecasts into forecasts table
- If no new data available: log 'no_update' and exit
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from supabase_io import (
    load_electricity_sales,
    load_model_binary,
    upsert_model_metadata,
    append_forecast_record,
    ensure_model_row,
    save_model_binary,
    log_event,
    get_model_row_by_name,
)
SCRIPT_NAME = "monthly_forecast.py"
MODEL_NAME = "sarima_v1"
BENCHMARK_NAME = "seasonal_naive"


def main():
    try:
        # Load processed data
        df = load_electricity_sales()
        if df.empty:
            log_event(SCRIPT_NAME, "error", "No processed electricity data available.")
            return

        # Ensure the benchmark model row exists so forecasts refer to a model id
        ensure_model_row(BENCHMARK_NAME)
        # Ensure SARIMA model row is present
        model_row = get_model_row_by_name(MODEL_NAME)
        if not model_row:
            log_event(SCRIPT_NAME, "error", f"Model {MODEL_NAME} not found in models table. Train first.")
            return

        # Load model binary and metadata
        model_obj, meta = load_model_binary(MODEL_NAME)  # returns (model_obj, meta_row_dict)
        last_observed = meta.get("last_observed")
        if last_observed is None:
            # If last_observed not set, use trained_through or earliest available
            last_observed = meta.get("trained_through")
        if last_observed is None:
            # no baseline - recommend training first
            log_event(SCRIPT_NAME, "error", f"Model {MODEL_NAME} metadata missing last_observed and trained_through.")
            return

        # last_in_model = pd.to_datetime('2025-05-01').date() # for one time backfill, comment out lines 71-86 and revive line 61, 92
        last_in_model = pd.to_datetime(last_observed).date()

        # Find any new observations since last_in_model
        new_obs_df = df[df["period"].dt.date > last_in_model].copy()

        if new_obs_df.empty:
            log_event(SCRIPT_NAME, "no_update", f"No new observations since {last_in_model.strftime('%Y-%m')}. No forecast generated.")
            return

        # Append new observations to fitted model state (no refit)
        # Statsmodels SARIMAXResults has .append(endog, refit=False)
        series_to_append = pd.Series(new_obs_df["sales"].values, index=new_obs_df["period"], name="sales")
        updated_model = model_obj.append(series_to_append, refit=False)

        # Save updated model binary back to storage (overwrite)
        remote_path = save_model_binary(updated_model, MODEL_NAME)
        # Update metadata: last_observed -> latest period in df
        new_last = str(df["period"].max().date())
        meta_update = {
            "saved_location": remote_path,
            "last_observed": new_last,
            # "trained_through": meta.get("trained_through"),  # keep original
            # "params": meta.get("params"),
        }
        upsert_model_metadata(MODEL_NAME, meta_update)

        # Forecast 1-step ahead (next month)
        latest_period = df["period"].max()
        next_period = (latest_period + relativedelta(months=1)).date()

        # updated_model = model_obj  # no new data appended for one time backfill

        sarima_fc = updated_model.forecast(steps=1)
        sarima_val = float(sarima_fc.iloc[0])
        append_forecast_record(next_period, MODEL_NAME, sarima_val)

        # Seasonal naive: value = value at next_period - 12 months
        ref_period = (pd.to_datetime(next_period) - pd.DateOffset(months=12)).date()
        df_idx = df.set_index(df["period"].dt.date)
        if ref_period in df_idx.index:
            snaive_val = float(df_idx.loc[ref_period, "sales"])
        else:
            snaive_val = None  # missing
        append_forecast_record(next_period, BENCHMARK_NAME, snaive_val)

        log_event(SCRIPT_NAME, "success", f"Forecasts for {next_period.strftime('%Y-%m')} inserted. Model updated through {new_last}.") 

    except Exception as e:
        log_event(SCRIPT_NAME, "error", str(e))
        raise


if __name__ == "__main__":
    main()
