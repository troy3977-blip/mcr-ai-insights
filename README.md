
# 🏥 MCR AI Insights #

Issuer-State-Market Medical Loss Ratio Panel Builder (2017–Present)
Production-grade data pipeline that transforms CMS Medical Loss Ratio (MLR) Public Use Files into a clean, model-ready issuer-level panel with inflation normalization, audit controls, and year-relative premium weighting.

## 📌 Overview ##

This project ingests raw CMS MLR ZIP files and produces a structured issuer-state-market-year panel suitable for:

- Regulatory analysis
- Loss ratio modeling
- Pricing strategy research
- Market concentration studies
- Risk-adjusted forecasting
- Longitudinal insurer stability analysis

It solves common MLR PUF challenges:

- Fact/dimension table separation
- Schema variation across years
- Inconsistent row codes
- Negative claims anomalies
- Outlier MCR values
- Multi-year panel stability filtering
- Inflation normalization (CPI/PPI via FRED)

## 🧱 Architecture ##

CMS MLR ZIP Archives (2017--Present) │ ▼ Extract + Normalize Raw CSVs │ ▼ Fact Table: Part1_2_Summary_Data_Premium_Claims.csv │ ▼ Header Dimensions: MR_Submission_Template_Header.csv │ ▼ Issuer--State--Market--Year Analytical Panel │ ▼ Inflation Merge (FRED CPI / PPI) │ ▼ Feature Engineering + Audit Filtering │ ▼ Model-Ready Artifacts (Parquet / CSV / Feature Store)

## 📂 Project Structure ##

mcr-ai-insights/
│
├── src/                         # Core application logic
│   │
│   ├── cli.py                   # Typer-based CLI entrypoint
│   │                             # Orchestrates end-to-end pipeline execution
│   │
│   ├── ingest_mlr.py            # CMS MLR ZIP extraction & normalization
│   │                             # Builds canonical issuer-state-market-year panel
│   │
│   ├── ingest_fred.py           # CPI / PPI inflation ingestion (FRED API)
│   │                             # Produces inflation normalization layer
│   │
│   ├── build_panel.py           # Deterministic feature engineering
│   │                             # Audit filtering + derived metrics
│   │
│   ├── export_panel.py          # Model-ready artifact generation
│   │                             # Stable subsets + weight-ready exports
│   │
│   └── config.py                # Centralized configuration management
│                                 # Paths, environment variables, API keys
│
├── data/
│   │
│   ├── raw/                     # Immutable source inputs (gitignored)
│   │                             # CMS ZIP downloads
│   │
│   └── processed/               # Versioned analytical outputs
│                                 # Parquet artifacts for modeling
│
├── .env                         # Optional runtime configuration (gitignored)
│                                 # FRED_API_KEY and local overrides
│
├── requirements.txt             # Explicit dependency specification
└── README.md                    # Project documentation

| Layer          | Module            | Responsibility                    |
| -------------- | ----------------- | --------------------------------- |
| Interface      | `cli.py`          | Orchestration & execution control |
| Ingestion      | `ingest_mlr.py`   | Raw CMS normalization             |
| Macroeconomic  | `ingest_fred.py`  | Inflation index acquisition       |
| Transformation | `build_panel.py`  | Feature engineering + audit logic |
| Export         | `export_panel.py` | Model-ready artifact generation   |
| Configuration  | `config.py`       | Environment + runtime settings    |

## 🚀 Quick Start ##

1️⃣ Install Dependencies
pip install -r requirements.txt

2️⃣ (Optional) Add FRED API Key
Create .env:
FRED_API_KEY=your_key_here

3️⃣ Build Panel
Single year:

- python -m src.cli --start-year 2017 --end-year 2017 --diagnostics

Full range:

- python -m src.cli --start-year 2017 --end-year 2024

Skip inflation:

- python -m src.cli --no-inflation

Output:

- data/processed/panel.parquet

4️⃣ Export Model Artifacts:

- python -m src.cli export --min-years 3 --w-cap 10

Outputs:

- panel_model.parquet
- panel_stable.parquet

## 🔍 Audit & Data Controls ##

During panel construction, automated audits flag and filter:

| Check                      | Purpose                |
| -------------------------- | ---------------------- |
| NaN year                   | Invalid dimension join |
| NaN premium                | Broken pivot           |
| Negative claims            | CMS restatements       |
| Earned premium ≤ threshold | Micro-issuer noise     |
| Raw MCR > 5                | Extreme data artifacts |

Example audit output:

- input rows: 10,504
- rows w/ incurred_claims < 0: 1,217
- rows w/ raw mcr > 5.0: 410
- Post-filter rows: 10,001

## 📊 Feature Engineering ##

Final panel includes:

- earned_premium
- incurred_claims
- mcr (Medical Loss Ratio)
- log_premium
- CPI / PPI inflation adjustments
- premium_weight (global)
- premium_weight_year (year-relative)
- w (capped global weight)
- w_year (capped year-relative weight)

## 🧮 Weighting Strategy ##

Global Premium Weight:

- w = earned_premium / global_median

Year-Relative Weight:

- premium_weight_year = earned_premium / median(earned_premium within year)

Ensures:

- Median weight per year ≈ 1.0
- Controls for macro growth effects
- Prevents 2024 dominating 2017
- Weights are capped at configurable w_cap.

## 🏛 Stable Panel Construction ##

Stable subset includes issuer-state-market groups with:

- '>=' min_years distinct years
- Default: 3 years

Output:

- panel_stable.parquet

Designed for:

- Fixed-effects models
- Panel regressions
- Longitudinal volatility analysis

## ⚙️ CLI Commands ##

Build Panel:

- python -m src.cli [OPTIONS]

Options:

| Option                  | Description                |
| ----------------------- | -------------------------- |
| `--start-year`          | First reporting year       |
| `--end-year`            | Last reporting year        |
| `--diagnostics`         | Print ingest diagnostics   |
| `--include-large-group` | Include Large Group market |
| `--no-inflation`        | Skip FRED CPI/PPI          |

Export Model Artifacts:

- python -m src.cli export

Options:

| Option         | Description            |
| -------------- | ---------------------- |
| `--min-years`  | Stable panel threshold |
| `--w-cap`      | Weight cap             |
| `--input-name` | Source parquet         |

## 🧠 Why This Matters ##

CMS MLR PUF data is:

- Multi-file
- Schema-variable
- Row-code encoded
- Fact/dim separated
- Not analysis-ready

This project transforms it into:

- ✔ Clean
- ✔ Audited
- ✔ Weighted
- ✔ Inflation-adjusted
- ✔ Panel-structured
- ✔ Modeling-ready

## 📈 Example Use Cases ##

- ✔ Insurer-level MCR forecasting
- ✔ Risk corridor / rebate modeling
- ✔ Market concentration analysis
- ✔ Premium growth normalization
- ✔ Inflation-adjusted profitability modeling
- ✔ Actuarial panel regressions

## 🔐 Data Handling ##

- Raw ZIP files not committed
- Parquet artifacts ignored in git
- .env excluded
- Designed for Azure / container deployment

## 🛠 Production Design Principles ##

- Idempotent downloads
- Deterministic joins
- Schema-robust column matching
- Explicit anomaly filtering
- Year-inference fallback
- CLI-first reproducibility
- Modeling artifacts separated from base panel

## 📌 Example Output Sizes (2017–2024) ##

| Stage                  | Rows   |
| ---------------------- | ------ |
| Raw extracted          | 10,504 |
| Post-audit filtered    | 10,001 |
| Stable subset (≥3 yrs) | 9,359  |

## 📄 License ##

Internal / research use
