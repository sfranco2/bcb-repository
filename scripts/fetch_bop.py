#!/usr/bin/env python3
"""
Download BCB Balance of Payments file and extract annual net figures for:
  - Current account   (Cuenta corriente)
  - Capital account   (Cuenta Capital)
  - Financial account excl. reserves  (Cuenta financiera − Activos de reserva)
  - Reserve assets    (Activos de reserva)  [increase = negative]
  - Net errors & omissions  (Errores y Omisiones)
Output: data/bop.json

NOTE: BCB updates this file annually. If the URL changes (e.g. BOP_2026.xlsx),
update BOP_URL below.
"""
import json
import os
import requests
from datetime import datetime

import pandas as pd

BOP_URL  = "https://www.bcb.gob.bo/webdocs/publicacionesbcb/1.%20BOP_2025.xlsx"
SHEET    = "BP Agregada"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# Row indices (0-based)
ROW_CURRENT   = 5   # Cuenta corriente
ROW_CAPITAL   = 27  # Cuenta Capital
ROW_FINANCIAL = 36  # Cuenta financiera (total, incl. reserves)
ROW_RESERVES  = 52  # Activos de reserva  (increase = -)
ROW_ERRORS    = 57  # Errores y Omisiones


def safe_float(df, row, col):
    try:
        v = float(df.iloc[row, col])
        return round(v, 2) if v == v else None   # NaN check
    except Exception:
        return None


def parse_bop(path):
    df = pd.read_excel(path, sheet_name=SHEET, header=None)
    print(f"Sheet shape: {df.shape}")

    row3 = [str(df.iloc[3, j]).strip() for j in range(df.shape[1])]
    row4 = [str(df.iloc[4, j]).strip() for j in range(df.shape[1])]

    # Build sorted set of Neto column indices (row4 contains 'Neto')
    neto_cols = sorted(j for j, v in enumerate(row4) if "neto" in v.lower())

    # For each annual label in row3, find the next Neto column at or after it
    annual = {}    # year_label → neto_col_index
    for j, val in enumerate(row3):
        if val in ("nan", ""):
            continue
        stripped = val.replace("p", "").strip()
        if stripped.isdigit() and 2000 <= int(stripped) <= 2100:
            neto_col = next((c for c in neto_cols if c >= j), None)
            if neto_col is not None:
                annual[val] = neto_col
                print(f"Year {val}: label col {j}, Neto col {neto_col}")

    years_sorted = sorted(annual.keys(), key=lambda y: int(y.replace("p", "")))

    results = {
        "years":             [],
        "current_account":   [],
        "capital_account":   [],
        "financial_account": [],   # excl. reserves
        "reserves":          [],   # increase = negative
        "errors":            [],
    }

    for yr in years_sorted:
        col = annual[yr]
        ca   = safe_float(df, ROW_CURRENT,   col)
        cap  = safe_float(df, ROW_CAPITAL,   col)
        fin  = safe_float(df, ROW_FINANCIAL, col)
        res  = safe_float(df, ROW_RESERVES,  col)
        err  = safe_float(df, ROW_ERRORS,    col)

        fin_excl = round(fin - res, 2) if fin is not None and res is not None else None

        results["years"].append(yr)
        results["current_account"].append(ca)
        results["capital_account"].append(cap)
        results["financial_account"].append(fin_excl)
        results["reserves"].append(res)
        results["errors"].append(err)

        print(f"  {yr}: CA={ca}, Cap={cap}, Fin={fin_excl}, Res={res}, Err={err}")

    return results


def main():
    print("Downloading BCB BOP file…")
    resp = requests.get(BOP_URL, timeout=60)
    resp.raise_for_status()
    tmp = "/tmp/bcb_bop.xlsx"
    with open(tmp, "wb") as f:
        f.write(resp.content)

    print("Parsing BOP sheet…")
    data = parse_bop(tmp)

    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        **data,
    }
    out_path = os.path.join(DATA_DIR, "bop.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(data['years'])} years of BOP data → {out_path}")


if __name__ == "__main__":
    main()
