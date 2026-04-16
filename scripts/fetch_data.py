#!/usr/bin/env python3
"""
Fetch the latest BCB weekly statistics Excel file and extract
reserves and exchange rate data into data/data.json
"""
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

BCB_PAGE = "https://www.bcb.gob.bo/?q=estad-sticas-semanales"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-BO,es;q=0.9,en;q=0.8",
    "Referer": "https://www.bcb.gob.bo/",
}

# Row indices (0-based) in the Excel sheet
ROW_RESERVES   = 6   # Reservas internacionales brutas del BCB (millions USD)
ROW_TC_OFFICIAL = 82  # Tipo de cambio de venta en el Bolsín (Bs/$us)
ROW_TC_MARKET   = 84  # Valor referencial de venta del dólar estadounidense (Bs/$us)

YEARS_BACK = 5
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def get_latest_excel_url():
    resp = requests.get(BCB_PAGE, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Only match the weekly "Semanal" statistics files, not other BCB Excel files
        if "estadisticassemanales" in href and "Semanal" in href and ".xlsx" in href.lower():
            if not href.startswith("http"):
                href = "https://www.bcb.gob.bo" + href
            return href
    raise ValueError("No Semanal weekly Excel file found on BCB page")


def download_excel(url):
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    tmp = "/tmp/bcb_semanal.xlsx"
    with open(tmp, "wb") as f:
        f.write(resp.content)
    return tmp


def parse_value(v):
    """Convert a cell value to float, handling Spanish comma decimals."""
    if pd.isna(v):
        return None
    if isinstance(v, str):
        cleaned = v.strip().replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def parse_excel(path):
    df = pd.read_excel(path, sheet_name=0, header=None)

    row2 = df.iloc[2, 3:]
    row3 = df.iloc[3, 3:]

    cutoff = datetime.now() - timedelta(days=YEARS_BACK * 365)

    reserves = []
    tc_official = []
    tc_market = []

    for i in range(len(row2)):
        # Prefer row3 (actual weekly dates) over row2 (monthly dates / "Semana X" labels)
        d = None
        if isinstance(row3.iloc[i], datetime):
            d = row3.iloc[i]
        elif isinstance(row2.iloc[i], datetime):
            d = row2.iloc[i]

        if d is None or d < cutoff:
            continue

        date_str = d.strftime("%Y-%m-%d")

        r = parse_value(df.iloc[ROW_RESERVES, 3 + i])
        if r is not None:
            reserves.append({"date": date_str, "value": round(r, 2)})

        tc_off = parse_value(df.iloc[ROW_TC_OFFICIAL, 3 + i])
        if tc_off is not None:
            tc_official.append({"date": date_str, "value": round(tc_off, 4)})

        tc_mkt = parse_value(df.iloc[ROW_TC_MARKET, 3 + i])
        if tc_mkt is not None and 1.0 < tc_mkt < 100.0:
            tc_market.append({"date": date_str, "value": round(tc_mkt, 4)})

    return reserves, tc_official, tc_market


def main():
    print("Fetching latest Excel URL from BCB...")
    url = get_latest_excel_url()
    print(f"Found: {url}")

    filename = url.split("/")[-1]
    print(f"Downloading {filename}...")
    path = download_excel(url)

    print("Parsing data...")
    reserves, tc_official, tc_market = parse_excel(path)

    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_file": filename,
        "source_url": url,
        "reserves": reserves,
        "exchange_rate_official": tc_official,
        "exchange_rate_market": tc_market,
    }

    out_path = os.path.join(DATA_DIR, "data.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(reserves)} reserve points, "
          f"{len(tc_official)} official rate points, "
          f"{len(tc_market)} market rate points")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
