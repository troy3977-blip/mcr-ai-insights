import numpy as np
import pandas as pd

from mcr_ai_insights.mcr import compute_mcr


def test_compute_mcr_handles_zero_premium():
    df = pd.DataFrame(
        {
            "earned_premium": [0.0, 100.0],
            "incurred_claims": [50.0, 50.0],
        }
    )
    out = compute_mcr(df, claims_col="incurred_claims", premiums_col="earned_premium")

    assert np.isnan(out.loc[0, "mcr"])
    assert out.loc[1, "mcr"] == 0.5


def test_compute_mcr_clips_negative_to_zero_by_default():
    df = pd.DataFrame(
        {
            "earned_premium": [100.0],
            "incurred_claims": [-10.0],
        }
    )
    out = compute_mcr(df, claims_col="incurred_claims", premiums_col="earned_premium")

    # compute_mcr does not clamp claims, but it clips mcr to (0.0, 5.0) by default
    assert out.loc[0, "incurred_claims"] == -10.0
    assert out.loc[0, "mcr"] == 0.0