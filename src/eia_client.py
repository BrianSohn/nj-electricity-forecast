# src/eia_client.py
"""
EIA API client and transformer.
Functions:
- fetch_month(period: 'YYYY-MM') -> raw JSON and DataFrame
- fetch_range(start:'YYYY-MM', end:'YYYY-MM') -> raw JSON and DataFrame
- transform_raw_to_df(raw) -> DataFrame with columns ['period','sales']
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import Tuple, Optional

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
# Endpoint used earlier in your project (EIA v2 retail-sales)
EIA_BASE = "https://api.eia.gov/v2/electricity/retail-sales/data/"

if EIA_API_KEY is None:
    raise RuntimeError("EIA_API_KEY not set in .env")


def fetch_month(period: str, stateid: str = "NJ", sectorid: str = "RES") -> Tuple[Optional[dict], pd.DataFrame]:
    """
    Fetch a single month from the EIA v2 retail-sales endpoint.
    - period: 'YYYY-MM' (e.g., '2025-07')
    Returns: (raw_response_dict or None, dataframe)
    """
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "monthly",
        "data[0]": "sales",
        "facets[stateid][]": stateid,
        "facets[sectorid][]": sectorid,
        "start": period,
        "end": period,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 100
    }
    resp = requests.get(EIA_BASE, params=params)
    resp.raise_for_status()
    raw = resp.json().get("response", {})
    df = transform_raw_to_df(raw)
    return raw, df


def fetch_range(start: str, end: str, stateid: str = "NJ", sectorid: str = "RES") -> Tuple[Optional[dict], pd.DataFrame]:
    """
    Fetch a range of months (start and end as 'YYYY-MM').
    """
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "monthly",
        "data[0]": "sales",
        "facets[stateid][]": stateid,
        "facets[sectorid][]": sectorid,
        "start": start,
        "end": end,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 5000
    }
    resp = requests.get(EIA_BASE, params=params)
    resp.raise_for_status()
    raw = resp.json().get("response", {})
    df = transform_raw_to_df(raw)
    return raw, df


def transform_raw_to_df(raw: dict) -> pd.DataFrame:
    """
    Transform the EIA 'response' dict into a DataFrame with columns 'period' (datetime) and 'sales'.
    EIA v2 retail-sales 'response' structure: {'data': [{...}], ...}
    """
    if not raw:
        return pd.DataFrame(columns=["period", "sales"])
    records = raw.get("data", [])
    if not records:
        return pd.DataFrame(columns=["period", "sales"])
    df = pd.DataFrame(records)
    # Keep only period & sales (some fields might be named 'sales' already)
    if "period" not in df.columns or "sales" not in df.columns:
        # try to detect actual columns
        # but for safety return empty
        return pd.DataFrame(columns=["period", "sales"])
    df = df[["period", "sales"]].copy()
    df["period"] = pd.to_datetime(df["period"])
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df = df.sort_values("period").reset_index(drop=True)
    return df