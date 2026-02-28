from __future__ import annotations

import numpy as np
import pandas as pd

from mcr_ai_insights.analysis import (
    ThresholdRiskConfig,
    identify_mlr_threshold_risk,
    premium_exposure_by_state,
    simulate_policy_scenario,
    summarize_threshold_risk,
    top_at_risk_report,
)


def _sample_panel() -> pd.DataFrame:
    # Minimal panel that satisfies analysis.py requirements
    return pd.DataFrame(
        [
            # Close to 0.80 (within 0.02 band)
            {
                "issuer_id": "A",
                "issuer_name": "Issuer A",
                "state": "FL",
                "market": "Individual",
                "year": 2022,
                "earned_premium": 1_000_000.0,
                "incurred_claims": 790_000.0,
                "mcr": 0.79,
            },
            {
                "issuer_id": "A",
                "issuer_name": "Issuer A",
                "state": "FL",
                "market": "Individual",
                "year": 2023,
                "earned_premium": 1_200_000.0,
                "incurred_claims": 984_000.0,
                "mcr": 0.82,
            },
            # Far from 0.80 (outside band)
            {
                "issuer_id": "B",
                "issuer_name": "Issuer B",
                "state": "FL",
                "market": "Individual",
                "year": 2023,
                "earned_premium": 800_000.0,
                "incurred_claims": 560_000.0,
                "mcr": 0.70,
            },
            # Different state, still within band
            {
                "issuer_id": "C",
                "issuer_name": "Issuer C",
                "state": "TX",
                "market": "Individual",
                "year": 2023,
                "earned_premium": 2_000_000.0,
                "incurred_claims": 1_620_000.0,
                "mcr": 0.81,
            },
            # Different market should be excluded when market="Individual"
            {
                "issuer_id": "D",
                "issuer_name": "Issuer D",
                "state": "FL",
                "market": "Small Group",
                "year": 2023,
                "earned_premium": 900_000.0,
                "incurred_claims": 720_000.0,
                "mcr": 0.80,
            },
        ]
    )


def test_identify_threshold_risk_filters_to_band_and_market() -> None:
    df = _sample_panel()
    cfg = ThresholdRiskConfig(threshold=0.80, band=0.02, market="Individual")

    at_risk = identify_mlr_threshold_risk(df, cfg)

    # Should include only Individual rows within +/- 0.02 of 0.80
    assert not at_risk.empty
    assert set(at_risk["market"].unique()) == {"Individual"}
    assert (at_risk["abs_distance_to_threshold"] <= 0.02).all()

    # Should exclude issuer B (0.70) and Small Group row
    assert "B" not in set(at_risk["issuer_id"])
    assert "D" not in set(at_risk["issuer_id"])


def test_summarize_threshold_risk_produces_expected_columns() -> None:
    df = _sample_panel()
    at_risk = identify_mlr_threshold_risk(
        df, ThresholdRiskConfig(threshold=0.80, band=0.02, market="Individual")
    )

    summary = summarize_threshold_risk(at_risk, threshold=0.80)

    expected_cols = {
        "issuer_id",
        "state",
        "market",
        "n_rows",
        "year_min",
        "year_max",
        "premium_sum",
        "premium_weighted_mcr",
        "mcr_min",
        "mcr_median",
        "mcr_max",
        "min_abs_distance",
        "share_at_or_above",
    }
    assert expected_cols.issubset(set(summary.columns))
    assert (summary["premium_sum"] > 0).all()


def test_premium_exposure_by_state_has_reasonable_shares() -> None:
    df = _sample_panel()
    out = premium_exposure_by_state(
        df,
        threshold=0.80,
        band=0.02,
        market="Individual",
        years=[2023],
    )

    assert set(out.columns) >= {
        "state",
        "premium_total",
        "premium_at_risk",
        "share_premium_at_risk",
        "premium_weighted_mcr_at_risk",
    }

    # Shares must be between 0 and 1
    assert ((out["share_premium_at_risk"] >= 0) & (out["share_premium_at_risk"] <= 1)).all()


def test_simulate_policy_scenario_recomputes_and_clips_mcr() -> None:
    df = _sample_panel()

    # Force an extreme scenario to ensure clipping occurs
    scn = simulate_policy_scenario(
        df,
        premium_multiplier=0.5,  # premiums cut in half
        claims_multiplier=2.0,  # claims doubled
        mcr_cap=5.0,
    )

    assert {"earned_premium_scn", "incurred_claims_scn", "mcr_scn"}.issubset(scn.columns)

    # mcr_scn should be finite or NaN (if premium is 0, which we don't have here)
    assert scn["mcr_scn"].notna().all()
    assert (scn["mcr_scn"] <= 5.0).all()

    # Should differ from original for at least one row
    assert not np.allclose(scn["mcr_scn"].to_numpy(), df["mcr"].to_numpy())


def test_top_at_risk_report_returns_expected_shape() -> None:
    df = _sample_panel()
    report = top_at_risk_report(df, threshold=0.80, band=0.02, market="Individual", top_n=10)

    assert not report.empty
    assert "issuer_id" in report.columns
    assert "abs_distance_to_threshold" in report.columns
    assert len(report) <= 10
