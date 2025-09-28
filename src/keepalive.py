# src/keepalive.py

"""
Simple script to ping Supabase so the project doesn't get paused.
Executes a trivial SELECT against the electricity_sales table.
"""

from supabase_io import load_electricity_sales_row
from datetime import datetime

def main(): 
    load_electricity_sales_row()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Pinged Supabase at {ts}")

if __name__ == "__main__":
    main()