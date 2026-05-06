#!/usr/bin/env python3
"""
Fetch the exchange rate CSV from dolarbluebolivia.click and write daily averages
for both the official rate (column C / index 2) and parallel rate (column E / index 4)
to data/parallel.json.  Column A (index 0) contains the timestamp.
"""
import json
import os
from datetime import datetime
from io import StringIO

import pandas as pd
import requests

PARALLEL_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vR2lRBAKrqBFtv_Y8glwaBq28banI80eg3wTOE9Y63LR8iVOjVhpxS3dpeBiqREYM3z1TgA0fdg_h7B"
    "/pub?gid=0&single=true&output=csv"
)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def fetch_exchange_rates():
    """Download CSV, extract cols A/C/E, return daily-averaged official & parallel series."""
    resp = requests.get(PARALLEL_CSV_URL, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    print(f"CSV shape: {df.shape}")
    print(f"CSV columns: {list(df.columns)}")
    print(df.head(5).to_string())

    if df.shape[1] < 5:
        raise ValueError(f"Expected at least 5 columns, got {df.shape[1]}: {list(df.columns)}")

    # Positional columns: A=0 (datetime), C=2 (official), E=4 (parallel)
    date_col     = df.columns[0]
    official_col = df.columns[2]
    parallel_col = df.columns[4]
    print(f"Using: date='{date_col}', official='{official_col}', parallel='{parallel_col}'")

    records = []
    for _, row in df.iterrows():
        try:
            dt       = pd.to_datetime(str(row[date_col]))
            date_str = dt.strftime("%Y-%m-%d")
            off      = float(str(row[official_col]).replace(",", "."))
            par      = float(str(row[parallel_col]).replace(",", "."))
            if off > 0 and par > 0:
                records.append({"date": date_str, "official": off, "parallel": par})
        except Exception:
            continue

    if not records:
        raise ValueError("No valid rows parsed from CSV")

    df2   = pd.DataFrame(records)
    daily = df2.groupby("date")[["official", "parallel"]].mean().reset_index()
    daily = daily.sort_values("date")

    official_series = [
        {"date": row["date"], "value": round(row["official"], 4)}
        for _, row in daily.iterrows()
    ]
    parallel_series = [
        {"date": row["date"], "value": round(row["parallel"], 4)}
        for _, row in daily.iterrows()
    ]

    print(f"Parsed {len(official_series)} daily data points")
    return official_series, parallel_series


def main():
    print("Fetching exchange rate data (dolarbluebolivia)...")
    official, parallel = fetch_exchange_rates()

    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "exchange_rate_official": official,
        "exchange_rate_parallel": parallel,
    }

    out_path = os.path.join(DATA_DIR, "parallel.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(official)} official + {len(parallel)} parallel pts → {out_path}")


if __name__ == "__main__":
    main()
