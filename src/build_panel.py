from __future__ import annotations

import numpy as np
import pandas as pd

from .quality import require_columns, basic_mcr_checks
from .mcr import compute_mcr


# ---------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------

def _audit_drops(
    df: pd.DataFrame,
    *,
    min_premium: float,
    mcr_cap: float | None,
) -> None:
    total = len(df)

    ep = pd.to_numeric(df.get("earned_premium"), errors="coerce")
    cl = pd.to_numeric(df.get("incurred_claims"), errors="coerce")
    yr = pd.to_numeric(df.get("year"), errors="coerce")

    n_bad_year = int(yr.isna().sum())
    n_bad_ep = int(ep.isna().sum())
    n_bad_cl = int(cl.isna().sum())
    n_le_minprem = int((ep <= min_premium).sum())
    n_neg_claims = int((cl < 0).sum())

    mcr_raw = cl / ep.replace({0.0: np.nan})
    n_bad_mcr = int(mcr_raw.isna().sum()) if total else 0
    n_mcr_cap = int((mcr_raw > mcr_cap).sum()) if (mcr_cap is not None) else 0

    print("[cyan]Step 4 audit:[/cyan]")
    print(f"  input rows: {total:,}")
    print(f"  rows w/ NaN year: {n_bad_year:,}")
    print(f"  rows w/ NaN earned_premium: {n_bad_ep:,}")
    print(f"  rows w/ NaN incurred_claims: {n_bad_cl:,}")
    print(f"  rows w/ earned_premium <= {min_premium}: {n_le_minprem:,}")
    print(f"  rows w/ incurred_claims < 0 (before clamp): {n_neg_claims:,}")
    if mcr_cap is not None:
        print(f"  rows w/ raw mcr > {mcr_cap}: {n_mcr_cap:,}")


# ---------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------

EXPECTED_OUTPUT_COLS = [
    "issuer_id", "issuer_name", "state", "market", "year",
    "earned_premium", "incurred_claims", "log_premium", "mcr",
    "baseline_mcr", "mcr_delta",
    "premium_yoy", "premium_yoy_lag1", "premium_yoy_lag2",
    "cpi_medical", "cpi_medical_yoy", "cpi_medical_3yr_cum",
    "ppi_hospitals", "ppi_hospitals_yoy", "ppi_hospitals_3yr_cum",
    "ppi_physician", "ppi_physician_yoy", "ppi_physician_3yr_cum",
    "pricing_gap_hosp",
]


# ---------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------

def build_panel(
    mlr: pd.DataFrame,
    infl: pd.DataFrame,
    *,
    baseline_start: int = 2017,
    baseline_end: int = 2019,
    min_premium: float = 1_000.0,
    clamp_negative_claims: bool = True,
    mcr_cap: float | None = 5.0,
) -> pd.DataFrame:

    if mlr is None or mlr.empty:
        return pd.DataFrame(columns=EXPECTED_OUTPUT_COLS)

    required = {
        "issuer_id",
        "state",
        "market",
        "year",
        "earned_premium",
        "incurred_claims",
    }
    missing = required - set(mlr.columns)
    if missing:
        raise ValueError(f"MLR dataframe missing required columns: {sorted(missing)}")

    df = mlr.copy()

    # -----------------------------------------------------------------
    # Normalize keys
    # -----------------------------------------------------------------
    df["issuer_id"] = df["issuer_id"].astype(str).str.strip()
    df["issuer_name"] = (
        df["issuer_name"].astype(str)
        if "issuer_name" in df.columns
        else ""
    )
    df["state"] = df["state"].astype(str).str.upper().str.strip()
    df["market"] = df["market"].astype(str).str.strip()

    # -----------------------------------------------------------------
    # Numeric coercion
    # -----------------------------------------------------------------
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["earned_premium"] = pd.to_numeric(df["earned_premium"], errors="coerce")
    df["incurred_claims"] = pd.to_numeric(df["incurred_claims"], errors="coerce")

    # Audit BEFORE filtering
    _audit_drops(df, min_premium=min_premium, mcr_cap=mcr_cap)

    # Drop missing core values
    df = df.dropna(subset=["year", "earned_premium", "incurred_claims"]).copy()

    # Premium floor
    df = df[df["earned_premium"] > min_premium].copy()

    # Clamp negative claims
    if clamp_negative_claims:
        df["incurred_claims"] = df["incurred_claims"].clip(lower=0)

    # Log premium (modeling feature)
    df["log_premium"] = np.log(df["earned_premium"])

    # -----------------------------------------------------------------
    # Compute MCR
    # -----------------------------------------------------------------
    df = compute_mcr(
        df,
        claims_col="incurred_claims",
        premiums_col="earned_premium",
        out_col="mcr",
        clip=(0.0, mcr_cap if mcr_cap is not None else 5.0),
    )

    require_columns(df, ["incurred_claims", "earned_premium", "mcr"])
    df = basic_mcr_checks(df, "mcr")

    if mcr_cap is not None:
        df = df[df["mcr"].between(0, mcr_cap)].copy()

    print(f"[green]Post-filter rows:[/green] {len(df):,}")

    # -----------------------------------------------------------------
    # Baseline median MCR
    # -----------------------------------------------------------------
    base_mask = df["year"].between(baseline_start, baseline_end)

    base = (
        df.loc[base_mask]
        .groupby(["issuer_id", "state", "market"], as_index=False)["mcr"]
        .median()
        .rename(columns={"mcr": "baseline_mcr"})
    )

    df = df.merge(
        base,
        on=["issuer_id", "state", "market"],
        how="left",
    )

    df["mcr_delta"] = df["mcr"] - df["baseline_mcr"]

    # -----------------------------------------------------------------
    # Premium YoY + lags
    # -----------------------------------------------------------------
    df = df.sort_values(
        ["issuer_id", "state", "market", "year"]
    ).copy()

    g = df.groupby(["issuer_id", "state", "market"], sort=False)

    df["premium_yoy"] = g["earned_premium"].pct_change()
    df["premium_yoy_lag1"] = g["premium_yoy"].shift(1)
    df["premium_yoy_lag2"] = g["premium_yoy"].shift(2)

    # -----------------------------------------------------------------
    # Merge inflation
    # -----------------------------------------------------------------
    if infl is not None and not infl.empty:
        infl2 = infl.copy()
        infl2["year"] = pd.to_numeric(infl2["year"], errors="coerce").astype("Int64")
        df = df.merge(infl2, on="year", how="left")
    else:
        for c in [
            "cpi_medical", "cpi_medical_yoy", "cpi_medical_3yr_cum",
            "ppi_hospitals", "ppi_hospitals_yoy", "ppi_hospitals_3yr_cum",
            "ppi_physician", "ppi_physician_yoy", "ppi_physician_3yr_cum",
        ]:
            df[c] = pd.NA

    # -----------------------------------------------------------------
    # Derived features
    # -----------------------------------------------------------------
    df["ppi_hospitals_yoy"] = pd.to_numeric(
        df.get("ppi_hospitals_yoy"), errors="coerce"
    )
    df["premium_yoy_lag1"] = pd.to_numeric(
        df["premium_yoy_lag1"], errors="coerce"
    )

    df["pricing_gap_hosp"] = (
        df["ppi_hospitals_yoy"] - df["premium_yoy_lag1"]
    )

    # -----------------------------------------------------------------
    # Enforce schema
    # -----------------------------------------------------------------
    for c in EXPECTED_OUTPUT_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    return df[EXPECTED_OUTPUT_COLS].reset_index(drop=True)