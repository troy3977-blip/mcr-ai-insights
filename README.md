# ACA Medical Claims Ratio (MCR) Regime Analysis

## Overview

This project analyzes whether elevated ACA Medical Claims Ratios (MCR) represent a temporary post-COVID distortion or a structural regime shift driven by persistent medical cost inflation and constrained premium pricing.
Rather than assuming mean reversion, the model is explicitly designed to handle the possibility that MCR never normalizes under current economic and regulatory conditions.
The analysis is built entirely on public U.S. government data, using a reproducible ingestion pipeline that avoids committing large raw datasets to GitHub.
Key Questions
Has ACA MCR permanently shifted to a higher regime?
Is premium growth keeping pace with medical cost inflation?
Under what conditions would MCR normalization be mathematically possible?
If normalization does not occur, what structural forces explain persistence?
Data Sources (Public, Authoritative)

1. CMS Medical Loss Ratio (MLR) Public Use Files
Issuer-level ACA filings with earned premiums and incurred claims.
Source:
<https://www.cms.gov/marketplace/resources/data/medical-loss-ratio-data-systems-resources>
Years used: 2017–latest available
Markets: Individual and Small Group
2. Bureau of Labor Statistics (via FRED)
Medical cost inflation proxies:
CPI Medical Care (CPIMEDSL)
PPI Hospitals (PCU622622)
PPI Physician Care (WPU511101)
Accessed via FRED API:
<https://fred.stlouisfed.org>
Design Principles
No raw data committed to GitHub
CMS PUFs are large and numerous; this repo commits code only, not ZIPs or CSVs.
Fully reproducible
Anyone can rebuild the dataset locally using public sources.
Separation of concerns
Ingest = schema-preserving
Features = economic assumptions
Models = interpretation only
Audit-friendly
Every transformation is explicit and documented.
Repository Structure
aca-mcr-normalization/
├── src/
│   ├── cli.py              # End-to-end pipeline runner
│   ├── config.py           # Paths and environment config
│   ├── ingest_mlr.py       # CMS MLR ZIP discovery & parsing
│   ├── ingest_fred.py      # CPI / PPI ingestion from FRED
│   └── build_panel.py      # Feature construction & joins
├── data_raw/               # Local cache for CMS ZIPs (gitignored)
├── data_processed/         # Small model-ready artifacts (optional commit)
├── .env                    # FRED API key (gitignored)
├── .gitignore
└── README.md
Environment Setup
3. Create a virtual environment
python -m venv .venv
source .venv/bin/activate
4. Install dependencies
pip install -r requirements.txt
5. Create .env with FRED API key
FRED_API_KEY=your_api_key_here
Only the API key characters go in .env — not full URLs.
Running the Pipeline (End-to-End)
This single command:
Downloads CMS MLR ZIPs (cached locally)
Builds issuer-state-year MCR panel
Pulls CPI/PPI inflation data
Constructs pricing and inflation features
Writes a compact panel.parquet
python -m src.cli run --start-year 2017 --end-year 2024
Output:
data_processed/panel.parquet
This file is small enough to inspect locally or commit if desired.
Core Features Constructed
Claims & Premiums
mcr = incurred_claims / earned_premium
baseline_mcr = issuer-state median (2017–2019)
mcr_delta = deviation from baseline
premium_yoy
premium_yoy_lag1, premium_yoy_lag2
Inflation
CPI Medical YoY
PPI Hospital YoY
PPI Physician YoY
3-year cumulative inflation
Structural Stress Indicator
Pricing Gap
pricing_gap_hosp = ppi_hospitals_yoy − premium_yoy_lag1
Persistent positive pricing gaps imply structural non-normalization.
Why Exchange Plan-Level Data Is Not Included
Plan-level Exchange PUFs are:
Extremely large
Operationally noisy
Not required to detect regime-level behavior
Premium growth is inferred directly from earned premiums in MLR filings, which:
Aligns with the MCR denominator
Reflects real pricing + enrollment effects
Avoids unnecessary ingestion complexity
Exchange data can be added later as an optional module if metal mix or benefit richness is required.
What This Project Demonstrates
Real-world health economics reasoning
Reproducible public-data engineering
Separation of ingest vs feature logic
Modeling that does not assume mean reversion
Professional handling of large external datasets
Intended Extensions
Regime-switching or state-space models
Issuer exit / participation analysis
Scenario-based inflation vs pricing stress tests
Visualization of normalization probability over time
Disclaimer
This project uses public, aggregated data only.
No PHI, proprietary claims, or confidential filings are included.
