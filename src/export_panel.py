# src/export_panel.py - create model-ready artifacts from data/processed/panel.parquet:
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ExportPaths:
    processed_dir: Path
    input_name: str = "panel.parquet"

    @property
    def input_path(self) -> Path:
        return self.processed_dir / self.input_name

    @property
    def model_path(self) -> Path:
        return self.processed_dir / "panel_model.parquet"

    @property
    def stable_path(self) -> Path:
        return self.processed_dir / "panel_stable.parquet"


def _safe_median(x: pd.Series) -> float:
    x2 = pd.to_numeric(x, errors="coerce")
    m = float(x2.median(skipna=True))
    return m if np.isfinite(m) and m > 0 else 1.0


def export_panel(
    processed_dir: Path,
    *,
    input_name: str = "panel.parquet",
    min_years: int = 3,
    w_cap: float = 10.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create model-ready artifacts from data/processed/panel.parquet:
      - panel_model.parquet: adds premium-based weights (global + year-relative)
      - panel_stable.parquet: subset of issuer/state/market with >= min_years distinct years
    """
    paths = ExportPaths(processed_dir=processed_dir, input_name=input_name)

    if not paths.input_path.exists():
        raise FileNotFoundError(f"Missing input parquet: {paths.input_path}")

    df = pd.read_parquet(paths.input_path)

    required = {"issuer_id", "state", "market", "year", "earned_premium", "mcr"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input panel missing columns: {sorted(missing)}")

    out = df.copy()

    # -----------------------------
    # 1) Global premium weight (w):  premium / median(premium)
    # -----------------------------
    prem = pd.to_numeric(out["earned_premium"], errors="coerce").replace(
        [np.inf, -np.inf], np.nan
    )
    global_med = _safe_median(prem)
    out["premium_weight"] = prem / global_med
    out["w"] = out["premium_weight"].clip(lower=0.0, upper=float(w_cap))

    # ----------------------------------------
    # 2) Year-relative premium weight (w_year): premium / median(premium) per year
    # median(premium_weight_year) per year ~= 1 (since global median is used as denominator), so w_year is roughly comparable across years
    # ----------------------------------------
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")

    year_meds = (
        out.groupby("year", dropna=False)["earned_premium"]
        .apply(_safe_median)
        .rename("year_median_premium")
        .reset_index()
    )
    out = out.merge(year_meds, on="year", how="left")
    out["premium_weight_year"] = prem / out["year_median_premium"].replace({0.0: 1.0})
    out["w_year"] = out["premium_weight_year"].clip(lower=0.0, upper=float(w_cap))

    # ----------------------------------------
    # 3) Stable subset (min distinct years): issuer/state/market combinations with at least min_years distinct years of data
    # ----------------------------------------
    grp = out.groupby(["issuer_id", "state", "market"], dropna=False)
    years_n = grp["year"].nunique().rename("n_years").reset_index()
    out2 = out.merge(years_n, on=["issuer_id", "state", "market"], how="left")
    stable = out2[out2["n_years"] >= int(min_years)].copy()

    # Write artifacts
    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    out2.to_parquet(paths.model_path, index=False)
    stable.to_parquet(paths.stable_path, index=False)

    return out2, stable
