from __future__ import annotations

import pandas as pd


def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def basic_mcr_checks(df: pd.DataFrame, mcr_col: str = "mcr") -> pd.DataFrame:
    out = df.copy()
    out["mcr_flag_negative"] = out[mcr_col] < 0
    out["mcr_flag_high"] = out[mcr_col] > 2.0
    return out
