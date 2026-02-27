from __future__ import annotations

import pandas as pd
import requests

FRED_OBS = "https://api.stlouisfed.org/fred/series/observations"


def fetch_series(series_id: str, api_key: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    r = requests.get(FRED_OBS, params=params, timeout=60)
    r.raise_for_status()
    obs = r.json()["observations"]
    df = pd.DataFrame(obs)[["date", "value"]]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["value"])
    return df


def annualize(df: pd.DataFrame, name: str) -> pd.DataFrame:
    # Annual average of monthly index
    out = df.copy()
    out["year"] = out["date"].dt.year.astype("Int64")
    out = out.groupby("year", as_index=False)["value"].mean().rename(columns={"value": name})
    out = out.sort_values("year")
    out[f"{name}_yoy"] = out[name].pct_change()
    out[f"{name}_3yr_cum"] = out[name] / out[name].shift(3) - 1
    return out


def build_inflation(api_key: str) -> pd.DataFrame:
    # Series IDs (official pages):
    # CPIMEDSL = CPI Medical Care :contentReference[oaicite:7]{index=7}
    # PCU622622 = PPI Hospitals :contentReference[oaicite:8]{index=8}
    # WPU511101 = PPI Physician Care (commodity) :contentReference[oaicite:9]{index=9}
    series = {
        "cpi_medical": "CPIMEDSL",
        "ppi_hospitals": "PCU622622",
        "ppi_physician": "WPU511101",
    }

    frames = []
    for name, sid in series.items():
        raw = fetch_series(sid, api_key)
        frames.append(annualize(raw, name))

    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="year", how="outer")
    return out.sort_values("year").reset_index(drop=True)
