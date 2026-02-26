from __future__ import annotations
import numpy as np
import pandas as pd


def add_premium_weights(
    df: pd.DataFrame,
    premium_col: str = "earned_premium",
    *,
    year_col: str = "year",
    w_cap: float = 10.0,
) -> pd.DataFrame:
    out = df.copy()
    prem = pd.to_numeric(out[premium_col], errors="coerce").astype(float)

    # global weights
    med = float(np.nanmedian(prem.values)) if len(prem) else np.nan
    if not np.isfinite(med) or med <= 0:
        out["premium_weight"] = 1.0
        out["w"] = 1.0
    else:
        out["premium_weight"] = (prem / med).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out["w"] = out["premium_weight"].clip(lower=0.0, upper=float(w_cap))

    # year-relative weights
    if year_col in out.columns:
        yrs = pd.to_numeric(out[year_col], errors="coerce").astype("Int64")
        out[year_col] = yrs

        year_medians = (
            pd.DataFrame({year_col: yrs, "_prem": prem})
            .dropna(subset=[year_col, "_prem"])
            .groupby(year_col, as_index=False)["_prem"]
            .median()
            .rename(columns={"_prem": "_prem_med_year"})
        )
        out = out.merge(year_medians, on=year_col, how="left")
        denom = pd.to_numeric(out["_prem_med_year"], errors="coerce").astype(float)

        out["premium_weight_year"] = (prem / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out["w_year"] = out["premium_weight_year"].clip(lower=0.0, upper=float(w_cap))
        out = out.drop(columns=["_prem_med_year"])
    else:
        out["premium_weight_year"] = out["premium_weight"]
        out["w_year"] = out["w"]

    return out


def filter_stable_panel(
    df: pd.DataFrame,
    *,
    key_cols: tuple[str, str, str] = ("issuer_id", "state", "market"),
    year_col: str = "year",
    min_years: int = 3,
) -> pd.DataFrame:
    out = df.copy()
    out[year_col] = pd.to_numeric(out[year_col], errors="coerce").astype("Int64")

    counts = (
        out.dropna(subset=[year_col])
        .groupby(list(key_cols), as_index=False)[year_col]
        .nunique()
        .rename(columns={year_col: "n_years"})
    )
    out = out.merge(counts, on=list(key_cols), how="left")
    out = out[out["n_years"].fillna(0).astype(int) >= int(min_years)].copy()
    return out.drop(columns=["n_years"])