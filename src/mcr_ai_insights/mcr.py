from __future__ import annotations
import numpy as np
import pandas as pd


def compute_mcr(
    df: pd.DataFrame,
    claims_col: str = "incurred_claims",
    premiums_col: str = "earned_premium",
    out_col: str = "mcr",
    clip: tuple[float, float] | None = (0.0, 5.0),
) -> pd.DataFrame:
    out = df.copy()
    denom = (
        pd.to_numeric(out[premiums_col], errors="coerce")
        .replace({0.0: np.nan})
        .astype(float)
    )
    num = pd.to_numeric(out[claims_col], errors="coerce").astype(float)
    out[out_col] = num / denom

    if clip is not None:
        lo, hi = clip
        out[out_col] = out[out_col].clip(lower=lo, upper=hi)

    return out
