# src/monthly_data_ingest.py
"""
Monthly ingest script.
- Checks latest stored period in electricity_sales.
- Attempts to fetch the immediate next month from EIA.
- If data exists, saves raw JSON to storage and upserts processed row into DB.
- Logs all events.
"""

from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from supabase_io import get_latest_period, upsert_electricity_sales, log_event, save_raw_json
from eia_client import fetch_month

SCRIPT_NAME = "monthly_data_ingest.py"


def next_month_str(latest_date):
    """Return 'YYYY-MM' string for the month after latest_date (latest_date is a date)."""
    nm = latest_date + relativedelta(months=1)
    return nm.strftime("%Y-%m")


def main():
    try:
        latest = get_latest_period()
        if latest is None:
            raise RuntimeError("No existing processed data found; run backfill first.")

        target_period = next_month_str(latest)

        # Fetch only target_period
        raw, df = fetch_month(target_period)
        if df.empty:
            log_event(SCRIPT_NAME, "no_update", f"No new data available for expected period {target_period}")
            return

        # Save raw JSON to storage
        remote_raw_path = f"monthly/{target_period.replace('-','')}.json"
        save_raw_json(raw, remote_raw_path)

        # Upsert processed rows (may be one row)
        upsert_electricity_sales(df)

        log_event(SCRIPT_NAME, "success", f"Ingested new month {target_period} and saved raw to {remote_raw_path}")

    except Exception as e:
        log_event(SCRIPT_NAME, "error", str(e))
        raise


if __name__ == "__main__":
    main()
