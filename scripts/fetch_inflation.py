#!/usr/bin/env python3
"""
Download INE Bolivia CPI file and parse the 12-month variation sheet
(CUADRO Nº 1.4 BOL VAR 12 MESES) for all divisions.
Output: data/inflation.json
"""
import json
import os
import requests
from datetime import datetime

import pandas as pd

INE_URL  = "https://nube.ine.gob.bo/index.php/s/J4dSH7CTeHwL8SS/download"
SHEET    = "CUADRO Nº 1.4 BOL VAR 12 MESES"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

MONTH_MAP = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03", "ABRIL": "04",
    "MAYO": "05", "JUNIO": "06", "JULIO": "07", "AGOSTO": "08",
    "SEPTIEMBRE": "09", "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12",
}


def month_num(raw):
    """Convert a (possibly abbreviated) Spanish month name to '01'-'12'."""
    raw = str(raw).strip().upper()
    for full, num in MONTH_MAP.items():
        if full.startswith(raw[:3]):
            return num
    return None


def parse_inflation(path):
    df = pd.read_excel(path, sheet_name=SHEET, header=None)
    print(f"Sheet shape: {df.shape}")

    # ── Build date labels from rows 4 (year) and 5 (month) ──────────────────
    dates = []        # list of "YYYY-MM" strings in column order
    col_idx = []      # corresponding column indices

    row4 = df.iloc[4]
    row5 = df.iloc[5]
    current_year = None

    for j in range(2, df.shape[1]):
        yr = str(row4.iloc[j]).strip()
        if yr not in ("nan", "") and yr.replace(".", "").isdigit():
            current_year = str(int(float(yr)))   # "2018.0" → "2018"

        mo_num = month_num(row5.iloc[j])
        if current_year and mo_num:
            dates.append(f"{current_year}-{mo_num}")
            col_idx.append(j)

    print(f"Date columns found: {len(dates)}  ({dates[0]} → {dates[-1]})")

    # ── Extract divisions (rows 7 to second-to-last) ─────────────────────────
    series = {}
    for i in range(7, df.shape[0] - 1):
        code = str(df.iloc[i, 0]).strip()
        name = str(df.iloc[i, 1]).strip()
        if name in ("nan", "") or code in ("nan", ""):
            continue

        values = []
        for j in col_idx:
            try:
                v = float(df.iloc[i, j])
                values.append(round(v, 4))
            except Exception:
                values.append(None)

        series[name] = values
        print(f"  Division '{name}': {len([v for v in values if v is not None])} data points")

    return dates, series


def main():
    print("Downloading INE CPI file…")
    resp = requests.get(INE_URL, timeout=60)
    resp.raise_for_status()
    tmp = "/tmp/ine_inflation.xlsx"
    with open(tmp, "wb") as f:
        f.write(resp.content)

    print("Parsing inflation sheet…")
    dates, series = parse_inflation(tmp)

    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dates": dates,
        "series": series,
    }
    out_path = os.path.join(DATA_DIR, "inflation.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(series)} divisions × {len(dates)} months → {out_path}")


if __name__ == "__main__":
    main()
