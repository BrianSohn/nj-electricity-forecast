# src/backfill_historical.py
"""
Backfill full historical data from EIA and store in Supabase (raw + processed).
Use this once to populate electricity_sales table from 2001-01 up to latest available.
"""

from datetime import datetime
import pandas as pd

from eia_client import fetch_range  # fetch_range returns raw,df
from supabase_io import upsert_electricity_sales, log_event, save_raw_json  # reuse helpers

SCRIPT_NAME = "backfill_historical.py"


def main():
    try:
        # Configure your full range here -- adjust if you need a different range
        start = "2001-01"
        end = "2025-06"

        # Fetch range
        raw, df = fetch_range(start, end)
        if df.empty:
            log_event(SCRIPT_NAME, "no_update", f"EIA returned no data for {start} to {end}")
            return

        # Save raw JSON to storage (backfill path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        remote_path = f"backfill/backfill_{ts}_{start.replace('-','')}_{end.replace('-','')}.json"
        save_raw_json(raw, remote_path)

        # Upsert processed rows
        upsert_electricity_sales(df)

        log_event(SCRIPT_NAME, "success", f"Backfilled {len(df)} rows ({start} to {end}). Raw saved to {remote_path}")

    except Exception as e:
        log_event(SCRIPT_NAME, "error", str(e))
        raise


if __name__ == "__main__":
    main()
