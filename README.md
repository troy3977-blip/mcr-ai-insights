
# 1️⃣ mcr-ai-insights

![CI](https://github.com/troy3977-blip/mcr-ai-insights/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Lint](https://img.shields.io/badge/lint-ruff-success)
![Tests](https://img.shields.io/badge/tests-pytest-success)

## 2️⃣ Executive Summary

## Overview

`mcr-ai-insights` is a reproducible Python pipeline that converts raw CMS Medical Loss Ratio (MLR) public-use files into a deterministic issuer–state–market–year analytical panel.

The system:

- Downloads and normalizes CMS MLR ZIP files
- Applies audit filtering and premium thresholds
- Computes Medical Loss Ratios (MCR) with guardrails
- Optionally integrates CPI/PPI inflation from FRED
- Produces modeling-ready and stable panel datasets

This project emphasizes reproducibility, data contracts, and production-grade packaging.

## 3️⃣ Architecture

data/raw/
    ↓
ingest_mlr → normalization + audit filtering
    ↓
build_panel → feature engineering (MCR, thresholds, inflation)
    ↓
panel.parquet
    ↓
export_panel
    ↓
panel_model.parquet
panel_stable.parquet

## Pipeline flow

1. Raw CMS ZIP files stored in data/raw/
2. Normalization into issuer–state–market–year structure
3. Deterministic filtering and feature engineering
4. Optional inflation integration (CPI/PPI via FRED)
5. Export to model-ready and stable analytical panels

## 4️⃣ Data Contracts

### Required Columns (MLR Input)

- issuer_id
- issuer_name
- state
- market
- year
- earned_premium
- incurred_claims

### Output Schema

`build_panel()` enforces a deterministic output schema:

- issuer_id
- issuer_name
- state
- market
- year
- earned_premium
- incurred_claims
- mcr
- cpi (optional)
- ppi (optional)
- derived features

## 5️⃣ Installation

```bash
git clone https://github.com/troy3977-blip/mcr-ai-insights
cd mcr-ai-insights
pip install -e ".[dev]"
```

Requires Python 3.10+.

## 6️⃣ Usage Examples

1)Subsidy Expansion or Reduction

Changes to premium tax credits (e.g., ARPA enhancements or sunset scenarios) may alter enrollment composition and average risk levels.  
Using the issuer-level MCR panel, analysts can:

- Measure pre- and post-policy MCR shifts
- Evaluate changes in premium growth relative to claims growth
- Identify state-level heterogeneity in impact

2)Medical Loss Ratio (MLR) Rebate Rule Adjustments

If regulatory thresholds or rebate calculations change, insurers may alter pricing or benefit design strategies.  
This dataset enables:

- Historical comparison of MCR distribution by market
- Identification of issuers operating near regulatory thresholds
- Sensitivity modeling under alternate MCR caps

3)Risk Adjustment or Market Stabilization Policy Changes

Modifications to risk corridors, reinsurance programs, or adjustment formulas affect claims volatility and pricing adequacy.  
The stable panel output supports:

- Longitudinal issuer performance analysis
- Variance and volatility modeling
- Cross-state structural comparisons

## 6️⃣ Usage

---

## Usage

## Build panel (no inflation)

```bash
mcr-ai --no-inflation
```

## Build panel with inflation (FRED)

```bash
mcr-ai --fred-api-key YOUR_KEY
```

## Export model artifacts

```bash
mcr-ai export --min-years 3 --w-cap 10.0
```

---

## 7️⃣ Example CLI Output

```markdown
1) Downloading CMS MLR ZIPs (cached in data/raw/)...
2) Building MLR panel (issuer-state-market-year)...
MLR rows: 10,504
3) Pulling CPI/PPI from FRED...
4) Building final panel + features...
Wrote: data/processed/panel.parquet rows=10,001
```

---

## 8️⃣ Testing and CI

## Quality Controls

- Ruff linting & formatting
- Pytest validation of MCR logic and panel schema
- GitHub Actions CI on every push

Run locally:

```bash
ruff check .
pytest -q
```

---

## 9️⃣ Why This Matters

Healthcare financial analytics often suffer from inconsistent preprocessing and undocumented filtering logic.

This project demonstrates:

- Deterministic data engineering
- Transparent audit filtering
- Reproducible analytical pipelines
- Production-grade packaging & CI enforcement
