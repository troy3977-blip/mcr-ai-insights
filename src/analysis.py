# src/mcr_ai_insights/analysis.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Sequence

import numpy as np
import pandas as pd

Market = Literal["Individual", "Small Group", "Large Group"]


REQUIRED_COLS_BASE: tuple[str, ...] = (
    "issuer_id",
    "state",
    "market",
    "year",
    "earned_premium",
    "mcr",
)

OPTIONAL_COLS: tuple[str, ...] = (
    "issuer_name",
    "incurred_claims",
    "w",
    "w_year",
)


@dataclass(frozen=True)
class ThresholdRiskConfig:
    """Configuration for identifying issuers near an MLR threshold."""

    threshold: float = 0.80
    band: float = 0.02  # +/- band around threshold (e.g., 2 percentage points)
    market: str | None = "Individual"
    min_earned_premium: float | None = None
    years: Sequence[int] | None = None


def _ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def load_panel(path: str | Path) -> pd.DataFrame:
    """
    Load a parquet panel artifact.

    Typical inputs:
      - data/processed/panel.parquet
      - data/processed/panel_model.parquet
      - data/processed/panel_stable.parquet
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Panel not found: {p}")
    df = pd.read_parquet(p)
    return df


def filter_panel(
    df: pd.DataFrame,
    market: str | None = None,
    states: Sequence[str] | None = None,
    issuer_ids: Sequence[str] | None = None,
    years: Sequence[int] | None = None,
    min_earned_premium: float | None = None,
) -> pd.DataFrame:
    """Convenience filters for common slices."""
    out = df.copy()

    if market is not None:
        out = out.loc[out["market"] == market]

    if states:
        out = out.loc[out["state"].isin(list(states))]

    if issuer_ids:
        out = out.loc[out["issuer_id"].isin(list(issuer_ids))]

    if years:
        out = out.loc[out["year"].isin(list(years))]

    if min_earned_premium is not None:
        out = out.loc[out["earned_premium"] >= float(min_earned_premium)]

    return out


def add_threshold_features(
    df: pd.DataFrame,
    threshold: float = 0.80,
) -> pd.DataFrame:
    """
    Add standard threshold diagnostics:
      - distance_to_threshold = mcr - threshold
      - abs_distance_to_threshold = |mcr - threshold|
      - at_or_above_threshold = mcr >= threshold
    """
    _ensure_columns(df, ("mcr",))
    out = df.copy()
    out["distance_to_threshold"] = out["mcr"] - float(threshold)
    out["abs_distance_to_threshold"] = out["distance_to_threshold"].abs()
    out["at_or_above_threshold"] = out["mcr"] >= float(threshold)
    return out


def identify_mlr_threshold_risk(
    df: pd.DataFrame,
    config: ThresholdRiskConfig = ThresholdRiskConfig(),
) -> pd.DataFrame:
    """
    Identify issuer/state/market/year rows within +/- band of the MLR threshold.

    Returns a filtered dataframe with added threshold diagnostic columns, sorted by
    (abs_distance_to_threshold asc, earned_premium desc).
    """
    _ensure_columns(df, REQUIRED_COLS_BASE)

    out = filter_panel(
        df,
        market=config.market,
        years=config.years,
        min_earned_premium=config.min_earned_premium,
    )
    out = add_threshold_features(out, threshold=config.threshold)

    band = float(config.band)
    out = out.loc[out["abs_distance_to_threshold"] <= band].copy()

    out = out.sort_values(
        by=["abs_distance_to_threshold", "earned_premium"],
        ascending=[True, False],
    ).reset_index(drop=True)

    return out


def summarize_threshold_risk(
    df_at_risk: pd.DataFrame,
    threshold: float = 0.80,
    group_by: Sequence[str] = ("issuer_id", "state", "market"),
) -> pd.DataFrame:
    """
    Summarize risk proximity and exposure for at-risk rows.

    Metrics:
      - n_rows, years_min/max
      - premium_sum
      - premium_weighted_mcr
      - min/median/max mcr
      - min abs_distance_to_threshold
    """
    _ensure_columns(df_at_risk, REQUIRED_COLS_BASE)

    df_feat = add_threshold_features(df_at_risk, threshold=threshold)

    keys = list(group_by)
    grp = df_feat.groupby(keys, dropna=False)

    def _wavg(x: pd.Series, w: pd.Series) -> float:
        w = w.astype(float)
        x = x.astype(float)
        denom = float(w.sum())
        return float((x * w).sum() / denom) if denom > 0 else float("nan")

    rows = []
    for k, g in grp:
        g = g.copy()
        premium_sum = float(g["earned_premium"].sum())
        rows.append(
            {
                **{keys[i]: k[i] for i in range(len(keys))} if isinstance(k, tuple) else {keys[0]: k},
                "n_rows": int(len(g)),
                "year_min": int(pd.to_numeric(g["year"], errors="coerce").min()),
                "year_max": int(pd.to_numeric(g["year"], errors="coerce").max()),
                "premium_sum": premium_sum,
                "premium_weighted_mcr": _wavg(g["mcr"], g["earned_premium"]),
                "mcr_min": float(g["mcr"].min()),
                "mcr_median": float(g["mcr"].median()),
                "mcr_max": float(g["mcr"].max()),
                "min_abs_distance": float(g["abs_distance_to_threshold"].min()),
                "share_at_or_above": float(g["at_or_above_threshold"].mean()),
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(
            by=["min_abs_distance", "premium_sum"],
            ascending=[True, False],
        ).reset_index(drop=True)

    return out


def premium_exposure_by_state(
    df: pd.DataFrame,
    threshold: float = 0.80,
    band: float = 0.02,
    market: str | None = "Individual",
    years: Sequence[int] | None = None,
) -> pd.DataFrame:
    """
    State-level view: premium exposure within a threshold band.

    Returns a dataframe with:
      - premium_total
      - premium_at_risk (within band)
      - share_premium_at_risk
      - premium_weighted_mcr_at_risk
    """
    _ensure_columns(df, REQUIRED_COLS_BASE)

    base = filter_panel(df, market=market, years=years)
    base = add_threshold_features(base, threshold=threshold)

    premium_total = (
        base.groupby("state", dropna=False)["earned_premium"]
        .sum()
        .rename("premium_total")
        .reset_index()
    )

    at_risk = base.loc[base["abs_distance_to_threshold"] <= float(band)].copy()

    if at_risk.empty:
        out = premium_total.assign(
            premium_at_risk=0.0,
            share_premium_at_risk=0.0,
            premium_weighted_mcr_at_risk=np.nan,
        )
        return out.sort_values("premium_total", ascending=False).reset_index(drop=True)

    risk_sum = (
        at_risk.groupby("state", dropna=False)["earned_premium"]
        .sum()
        .rename("premium_at_risk")
        .reset_index()
    )

    def _pw_mcr(g: pd.DataFrame) -> float:
        denom = float(g["earned_premium"].sum())
        return float((g["mcr"] * g["earned_premium"]).sum() / denom) if denom > 0 else float("nan")

    pw = (
        at_risk.groupby("state", dropna=False)
        .apply(_pw_mcr, include_groups=False)
        .rename("premium_weighted_mcr_at_risk")
        .reset_index()
    )

    out = premium_total.merge(risk_sum, on="state", how="left").merge(pw, on="state", how="left")
    out["premium_at_risk"] = out["premium_at_risk"].fillna(0.0)
    out["share_premium_at_risk"] = np.where(
        out["premium_total"] > 0,
        out["premium_at_risk"] / out["premium_total"],
        0.0,
    )

    return out.sort_values("premium_at_risk", ascending=False).reset_index(drop=True)


def simulate_policy_scenario(
    df: pd.DataFrame,
    premium_multiplier: float = 1.0,
    claims_multiplier: float = 1.0,
    *,
    mcr_cap: float = 5.0,
) -> pd.DataFrame:
    """
    Simple scenario tool to stress MCR under policy-driven shifts.

    Examples:
      - Subsidy expansion may increase enrollment and shift premium mix:
        premium_multiplier > 1 (if average premium increases) or < 1.
      - Utilization shock / risk mix shift:
        claims_multiplier > 1.

    MCR is recomputed as:
      mcr_scn = (incurred_claims * claims_multiplier) / (earned_premium * premium_multiplier)

    Requirements:
      - 'earned_premium'
      - 'incurred_claims' (if not present, raises)
    """
    _ensure_columns(df, ("earned_premium", "incurred_claims"))

    out = df.copy()
    prem = out["earned_premium"].astype(float) * float(premium_multiplier)
    clm = out["incurred_claims"].astype(float) * float(claims_multiplier)

    mcr_scn = clm / prem.replace({0.0: np.nan})
    mcr_scn = mcr_scn.clip(lower=0.0, upper=float(mcr_cap))

    out["earned_premium_scn"] = prem
    out["incurred_claims_scn"] = clm
    out["mcr_scn"] = mcr_scn

    return out


def top_at_risk_report(
    df: pd.DataFrame,
    threshold: float = 0.80,
    band: float = 0.02,
    market: str | None = "Individual",
    years: Sequence[int] | None = None,
    top_n: int = 25,
) -> pd.DataFrame:
    """
    Convenience: return a compact top-N report for issuers nearest threshold,
    weighted by premium exposure.

    Output columns:
      issuer_id, state, market, year, earned_premium, mcr,
      distance_to_threshold, abs_distance_to_threshold
    """
    cfg = ThresholdRiskConfig(
        threshold=threshold,
        band=band,
        market=market,
        years=years,
    )
    at_risk = identify_mlr_threshold_risk(df, cfg)

    cols = [
        "issuer_id",
        "state",
        "market",
        "year",
        "earned_premium",
        "mcr",
        "distance_to_threshold",
        "abs_distance_to_threshold",
    ]
    # keep optional identifiers if present
    if "issuer_name" in at_risk.columns:
        cols.insert(1, "issuer_name")

    return at_risk.loc[:, [c for c in cols if c in at_risk.columns]].head(int(top_n))


__all__ = [
    "ThresholdRiskConfig",
    "load_panel",
    "filter_panel",
    "add_threshold_features",
    "identify_mlr_threshold_risk",
    "summarize_threshold_risk",
    "premium_exposure_by_state",
    "simulate_policy_scenario",
    "top_at_risk_report",
]