# src/mcr_ai_insights/export_panel.py
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED_PANEL_COLS: tuple[str, ...] = (
    "issuer_id",
    "state",
    "market",
    "year",
    "earned_premium",
    "mcr",
)


@dataclass(frozen=True)
class ExportPaths:
    processed_dir: Path
    input_path: Path
    model_path: Path
    stable_path: Path

    @classmethod
    def from_dir(cls, processed_dir: Path, input_name: str) -> ExportPaths:
        processed_dir = Path(processed_dir)
        return cls(
            processed_dir=processed_dir,
            input_path=processed_dir / input_name,
            model_path=processed_dir / "panel_model.parquet",
            stable_path=processed_dir / "panel_stable.parquet",
        )


def _ensure_required_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Input panel missing columns: {missing}")


def _coerce_year_int64(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    return out


def _safe_median(series: pd.Series) -> float:
    """Return a strictly positive median, or 1.0 as a safe fallback."""
    med = float(series.median()) if len(series) else 1.0
    if not np.isfinite(med) or med <= 0:
        return 1.0
    return med


def export_panel(
    processed_dir: Path | str,
    input_name: str = "panel.parquet",
    min_years: int = 3,
    w_cap: float = 10.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create model-ready artifacts from data/processed/<input_name>.

    Outputs written to processed_dir:
      - panel_model.parquet: adds premium-based weights (premium_weight, w) and
        within-year weights (premium_weight_year, w_year), plus n_years.
      - panel_stable.parquet: subset of issuer/state/market keys with
        >= min_years distinct years.

    Returns
    -------
    (model_df, stable_df)
    """
    paths = ExportPaths.from_dir(Path(processed_dir), input_name)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)

    if not paths.input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {paths.input_path}")

    out = pd.read_parquet(paths.input_path)

    _ensure_required_columns(out, REQUIRED_PANEL_COLS)
    out = _coerce_year_int64(out)

    # ---------------------------------------------------------------------
    # 1) Global premium weights
    # ---------------------------------------------------------------------
    global_med = _safe_median(out["earned_premium"])
    out["premium_weight"] = out["earned_premium"] / global_med
    out["w"] = out["premium_weight"].clip(lower=0.0, upper=float(w_cap))

    # ---------------------------------------------------------------------
    # 2) Year-normalized premium weights
    #
    # premium_weight_year uses earned_premium / median(premium) per year.
    # If the per-year median is missing/invalid, the global median is used
    # as the denominator, so w_year remains comparable across years.
    # ---------------------------------------------------------------------
    year_meds = (
        out.groupby("year", dropna=False)["earned_premium"]
        .median()
        .rename("year_median_premium")
        .reset_index()
    )

    merged = out.merge(year_meds, on="year", how="left")

    denom = merged["year_median_premium"].astype(float)
    denom = denom.where(np.isfinite(denom) & (denom > 0), other=global_med)

    merged["premium_weight_year"] = merged["earned_premium"] / denom
    merged["w_year"] = merged["premium_weight_year"].clip(lower=0.0, upper=float(w_cap))
    merged = merged.drop(columns=["year_median_premium"])

    # ---------------------------------------------------------------------
    # 3) Stable subset (min distinct years)
    # ---------------------------------------------------------------------
    keys = ["issuer_id", "state", "market"]
    years_n = merged.groupby(keys, dropna=False)["year"].nunique().rename("n_years").reset_index()

    merged = merged.merge(years_n, on=keys, how="left")
    stable = merged.loc[merged["n_years"] >= int(min_years)].copy()

    # ---------------------------------------------------------------------
    # 4) Write artifacts
    # ---------------------------------------------------------------------
    merged.to_parquet(paths.model_path, index=False)
    stable.to_parquet(paths.stable_path, index=False)

    return merged, stable
