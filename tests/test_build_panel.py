import pandas as pd

from mcr_ai_insights.build_panel import EXPECTED_OUTPUT_COLS, build_panel


def test_build_panel_smoke_columns():
    # premiums must be > 1000.0 to survive the default min_premium filter
    mlr = pd.DataFrame(
        {
            "issuer_id": ["A", "A"],
            "issuer_name": ["Issuer A", "Issuer A"],
            "state": ["FL", "FL"],
            "market": ["Individual", "Individual"],
            "year": [2020, 2021],
            "earned_premium": [2000.0, 2500.0],
            "incurred_claims": [1000.0, 1200.0],
        }
    )

    infl = pd.DataFrame()  # allowed; build_panel will fill inflation columns with NA

    panel = build_panel(mlr, infl)

    assert len(panel) == 2
    # build_panel enforces an output schema; validate it
    assert list(panel.columns) == EXPECTED_OUTPUT_COLS
    assert panel["mcr"].notna().all()