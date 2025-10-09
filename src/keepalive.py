# src/keepalive.py

"""
Simple script to ping Supabase so the project doesn't get paused.
Executes a trivial SELECT against the electricity_sales table.
"""

from supabase_io import load_electricity_sales_row, log_event, delete_logs_from_script
from datetime import datetime
import time

script = "keepalive.py"

def main(): 
    load_electricity_sales_row()
    log_event(script=script, status="success", details="Keepalive ping successful.")
    time.sleep(2)  # wait a bit before cleanup
    delete_logs_from_script(script=script)

    print(f"{datetime.now()}: Keepalive ping executed.")

if __name__ == "__main__":
    main()