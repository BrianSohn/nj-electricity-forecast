# src/backfill_historical.py
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_BACKFILL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "backfill")
os.makedirs(RAW_BACKFILL_DIR, exist_ok=True)

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

def fetch_data():
    response = requests.get(API_URL, params=PARAMS)
    response.raise_for_status()
    return response.json()

def save_raw(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(RAW_BACKFILL_DIR, f"backfill_{timestamp}_200101_202506.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved raw backfill data to {out_path}")
    return out_path

if __name__ == "__main__":
    data = fetch_data()
    save_raw(data)
