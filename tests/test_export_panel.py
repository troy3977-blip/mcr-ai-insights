import pandas as pd

from mcr_ai_insights.export_panel import export_panel


def test_export_panel_creates_weights_and_outputs(tmp_path):
    # export_panel requires 'mcr' already present in the input parquet
    df = pd.DataFrame(
        {
            "issuer_id": ["A", "A", "A"],
            "state": ["FL", "FL", "FL"],
            "market": ["Individual", "Individual", "Individual"],
            "year": [2020, 2021, 2022],
            "earned_premium": [2000.0, 2500.0, 10_000.0],
            "mcr": [0.50, 0.55, 0.60],
        }
    )

    in_file = tmp_path / "panel.parquet"
    df.to_parquet(in_file, index=False)

    model_df, stable_df = export_panel(
        processed_dir=tmp_path,
        input_name="panel.parquet",
        min_years=2,
        w_cap=10.0,
    )

    # Weight columns created
    for col in ["premium_weight", "w", "premium_weight_year", "w_year"]:
        assert col in model_df.columns

    assert model_df["w"].max() <= 10.0
    assert model_df["w_year"].max() <= 10.0

    # Stable subset should exist because issuer/state/market has 3 distinct years
    assert len(stable_df) > 0
