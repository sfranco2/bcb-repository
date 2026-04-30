#!/usr/bin/env python3
"""
Fetch the latest BCB weekly statistics Excel file and extract:
  - International reserves (total, FX, gold) since 2000
  - Exchange rates (official Bolsín, market reference) — last 5 years
  - Monetary aggregates (Base, M'1, M'2, M'3) since 2000
Output: data/data.json
"""
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

BCB_PAGE    = "https://www.bcb.gob.bo/?q=estad-sticas-semanales"
INE_GDP_URL = "https://www.ine.gob.bo/referencia2017/CUADROS/pagina_web/2.7.4.xlsx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-BO,es;q=0.9,en;q=0.8",
    "Referer": "https://www.bcb.gob.bo/",
}

# Row indices (0-based) in the Excel sheet
ROW_RESERVES    = 6   # Reservas internacionales brutas del BCB (millions USD)
ROW_FX          = 7   # Divisas / Foreign exchange reserves (millions USD)
ROW_GOLD        = 9   # Oro (gold reserves, millions USD)
ROW_TC_OFFICIAL = 82  # Tipo de cambio de venta en el Bolsín (Bs/$us)
ROW_TC_MARKET   = 84  # Valor referencial de venta del dólar estadounidense (Bs/$us)
ROW_MON_BASE    = 26  # Base monetaria (millions Bs)
ROW_M1          = 35  # M'1 (millions Bs)
ROW_M2          = 36  # M'2 (millions Bs)
ROW_M3          = 37  # M'3 (millions Bs)

YEARS_BACK = 5                          # for exchange rate data
RESERVES_CUTOFF = datetime(2000, 1, 1)  # reserves, gold, monetary data go back to 2000
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

    tc_cutoff = datetime.now() - timedelta(days=YEARS_BACK * 365)

    reserves   = []
    fx         = []
    gold       = []
    tc_official = []
    tc_market   = []
    mon_base    = []
    m1          = []
    m2          = []
    m3          = []

    for i in range(len(row2)):
        # Prefer row3 (actual weekly dates) over row2 (monthly dates / "Semana X" labels)
        d = None
        if isinstance(row3.iloc[i], datetime):
            d = row3.iloc[i]
        elif isinstance(row2.iloc[i], datetime):
            d = row2.iloc[i]

        if d is None:
            continue

        date_str = d.strftime("%Y-%m-%d")

        if d >= RESERVES_CUTOFF:
            r = parse_value(df.iloc[ROW_RESERVES, 3 + i])
            if r is not None:
                reserves.append({"date": date_str, "value": round(r, 2)})

            f = parse_value(df.iloc[ROW_FX, 3 + i])
            if f is not None:
                fx.append({"date": date_str, "value": round(f, 2)})

            g = parse_value(df.iloc[ROW_GOLD, 3 + i])
            if g is not None:
                gold.append({"date": date_str, "value": round(g, 2)})

            mb = parse_value(df.iloc[ROW_MON_BASE, 3 + i])
            if mb is not None:
                mon_base.append({"date": date_str, "value": round(mb, 2)})

            v1 = parse_value(df.iloc[ROW_M1, 3 + i])
            if v1 is not None:
                m1.append({"date": date_str, "value": round(v1, 2)})

            v2 = parse_value(df.iloc[ROW_M2, 3 + i])
            if v2 is not None:
                m2.append({"date": date_str, "value": round(v2, 2)})

            v3 = parse_value(df.iloc[ROW_M3, 3 + i])
            if v3 is not None:
                m3.append({"date": date_str, "value": round(v3, 2)})

        if d >= tc_cutoff:
            tc_off = parse_value(df.iloc[ROW_TC_OFFICIAL, 3 + i])
            if tc_off is not None:
                tc_official.append({"date": date_str, "value": round(tc_off, 4)})

            tc_mkt = parse_value(df.iloc[ROW_TC_MARKET, 3 + i])
            if tc_mkt is not None and 1.0 < tc_mkt < 100.0:
                tc_market.append({"date": date_str, "value": round(tc_mkt, 4)})

    return reserves, fx, gold, tc_official, tc_market, mon_base, m1, m2, m3


def fetch_gdp_data():
    """Download and parse INE GDP contributions table."""
    resp = requests.get(INE_GDP_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    tmp = "/tmp/ine_gdp.xlsx"
    with open(tmp, "wb") as f:
        f.write(resp.content)

    df = pd.read_excel(tmp, sheet_name=0, header=None)

    years = []
    for v in df.iloc[10, 2:9]:
        if isinstance(v, float) and not pd.isna(v):
            years.append(str(int(v)))
        else:
            years.append(str(v).strip())

    def row_vals(row_idx):
        return [round(float(v), 4) if pd.notna(v) else None for v in df.iloc[row_idx, 2:9]]

    imports_raw = row_vals(20)
    imports_neg = [(-v if v is not None else None) for v in imports_raw]

    return {
        "years": years,
        "total_growth": row_vals(12),
        "components": {
            "Household consumption":  row_vals(14),
            "Government consumption": row_vals(15),
            "Inventory changes":      row_vals(16),
            "Fixed investment":       row_vals(17),
            "Valuables":              row_vals(18),
            "Exports":                row_vals(19),
            "Less imports":           imports_neg,
        },
    }


def main():
    print("Fetching latest Excel URL from BCB...")
    url = get_latest_excel_url()
    print(f"Found: {url}")

    filename = url.split("/")[-1]
    print(f"Downloading {filename}...")
    path = download_excel(url)

    print("Parsing BCB data...")
    reserves, fx, gold, tc_official, tc_market, mon_base, m1, m2, m3 = parse_excel(path)

    print("Fetching INE GDP data...")
    try:
        gdp = fetch_gdp_data()
        print(f"GDP data: {len(gdp['years'])} years")
    except Exception as e:
        print(f"Warning: could not fetch GDP data: {e}")
        gdp = None

    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_file": filename,
        "source_url": url,
        "reserves": reserves,
        "fx": fx,
        "gold": gold,
        "exchange_rate_official": tc_official,
        "exchange_rate_market": tc_market,
        "monetary_base": mon_base,
        "m1": m1,
        "m2": m2,
        "m3": m3,
        "gdp": gdp,
    }

    out_path = os.path.join(DATA_DIR, "data.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(reserves)} reserve pts, {len(fx)} FX pts, {len(gold)} gold pts, "
          f"{len(mon_base)} monetary base pts, {len(m1)} M'1 pts, "
          f"{len(tc_official)} official rate pts, {len(tc_market)} market rate pts")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
