# src/train_model.py
"""
Train SARIMA model on the entire electricity_sales table and persist model + metadata to Supabase.
- Uses the hyperparameters you've tuned in notebook (order, seasonal_order).
- Saves binary to Supabase Storage and metadata to models table.
"""

from datetime import datetime
import pandas as pd
import statsmodels.api as sm

from supabase_io import load_electricity_sales, save_model_binary, upsert_model_metadata, log_event, ensure_model_row

SCRIPT_NAME = "train_model.py"
MODEL_NAME = "sarima_v1"

# Use the SARIMA parameters you tuned in the notebook (user supplied)
ORDER = (1, 1, 2)
SEASONAL_ORDER = (1, 1, 1, 12)


def main():
    try:
        df = load_electricity_sales()
        if df.empty:
            raise RuntimeError("No processed data available. Run backfill/ingest first.")

        y = df.set_index("period")["sales"].astype(float)

        # Fit SARIMA (statsmodels)
        model = sm.tsa.SARIMAX(y, order=ORDER, seasonal_order=SEASONAL_ORDER,
                               enforce_stationarity=False, enforce_invertibility=False)
        fitted = model.fit(disp=False)

        # Save model binary to storage
        remote_path = save_model_binary(fitted, MODEL_NAME)  # returns path like 'sarima_v1.pkl'

        # Prepare metadata
        meta = {
            "saved_location": remote_path,
            "trained_from": str(y.index.min().date()),
            "trained_through": str(y.index.max().date()), # initial training range
            "last_observed": str(y.index.max().date()), # same at first, diverges later
            "params": {"order": ORDER, "seasonal_order": SEASONAL_ORDER},
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }

        # Ensure model row exists and upsert metadata
        ensure_model_row(MODEL_NAME)
        upsert_model_metadata(MODEL_NAME, meta)

        log_event(SCRIPT_NAME, "success", f"Trained model {MODEL_NAME} on {len(y)} rows; saved to {remote_path}")

    except Exception as e:
        log_event(SCRIPT_NAME, "error", str(e))
        raise


if __name__ == "__main__":
    main()
