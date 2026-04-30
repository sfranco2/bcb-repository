#!/usr/bin/env python3
"""
Fetch the parallel (black market) USD/BOB exchange rate from the
dolarbluebolivia.click Google Sheets CSV and write to data/parallel.json.
This script runs daily, independently of the weekly BCB data fetch.
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


def fetch_parallel_rate():
    """Download and parse the parallel rate CSV."""
    resp = requests.get(PARALLEL_CSV_URL, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    print(f"CSV columns: {list(df.columns)}")
    print(df.head(3).to_string())

    # Detect date column
    date_col = None
    for col in df.columns:
        if col.strip().lower() in ("fecha", "date", "datetime", "dia", "time", "periodo"):
            date_col = col
            break
    if date_col is None:
        for col in df.columns:
            try:
                pd.to_datetime(str(df[col].iloc[0]))
                date_col = col
                break
            except Exception:
                pass

    # Detect rate column — prefer sell-side (venta)
    rate_col = None
    for col in df.columns:
        if col.strip().lower() in ("venta", "sell", "venta_usd", "precio_venta",
                                   "paralelo", "value", "rate", "precio", "valor"):
            rate_col = col
            break
    if rate_col is None:
        for col in df.select_dtypes(include="number").columns:
            if col != date_col:
                rate_col = col
                break
        if rate_col is None and len(df.columns) > 1:
            rate_col = df.columns[1]

    if date_col is None or rate_col is None:
        raise ValueError(f"Cannot identify date/rate columns. Found: {list(df.columns)}")

    print(f"Using date_col='{date_col}', rate_col='{rate_col}'")

    seen = {}
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(str(row[date_col]))
            v = float(str(row[rate_col]).replace(",", "."))
            if pd.isna(v) or v < 1.0 or v > 1000.0:
                continue
            seen[d.strftime("%Y-%m-%d")] = round(v, 4)
        except Exception:
            continue

    result = [{"date": d, "value": v} for d, v in sorted(seen.items())]
    print(f"Parsed {len(result)} data points")
    return result


def main():
    print("Fetching parallel exchange rate (dolarbluebolivia)...")
    data = fetch_parallel_rate()

    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "exchange_rate_parallel": data,
    }

    out_path = os.path.join(DATA_DIR, "parallel.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(data)} parallel rate points → {out_path}")


if __name__ == "__main__":
    main()
