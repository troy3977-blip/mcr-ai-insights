from __future__ import annotations
import duckdb
import pandas as pd

def build_panel(mlr: pd.DataFrame, infl: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()

    con.register("mlr", mlr)
    con.register("infl", infl)

    # Compute MCR + baseline (2017-2019 median) + premium YoY + lags
    q = r"""
    with base as (
      select
        issuer_id,
        state,
        year::int as year,
        market,
        earned_premium,
        incurred_claims,
        (incurred_claims / nullif(earned_premium, 0)) as mcr,
        earned_premium as premium_level
      from mlr
    ),
    baseline as (
      select
        issuer_id,
        state,
        median(mcr) as baseline_mcr
      from base
      where year between 2017 and 2019
      group by 1,2
    ),
    priced as (
      select
        b.*,
        bl.baseline_mcr,
        (b.mcr - bl.baseline_mcr) as mcr_delta,
        (premium_level / nullif(lag(premium_level) over(partition by issuer_id, state order by year), 0) - 1) as premium_yoy,
        lag((premium_level / nullif(lag(premium_level) over(partition by issuer_id, state order by year), 0) - 1), 1)
          over(partition by issuer_id, state order by year) as premium_yoy_lag1,
        lag((premium_level / nullif(lag(premium_level) over(partition by issuer_id, state order by year), 0) - 1), 2)
          over(partition by issuer_id, state order by year) as premium_yoy_lag2
      from base b
      left join baseline bl using (issuer_id, state)
    )
    select
      p.*,
      i.cpi_medical,
      i.cpi_medical_yoy,
      i.ppi_hospitals,
      i.ppi_hospitals_yoy,
      i.ppi_physician,
      i.ppi_physician_yoy,
      -- Pricing gap example (hospital PPI - lagged premium growth)
      (i.ppi_hospitals_yoy - p.premium_yoy_lag1) as pricing_gap_hosp
    from priced p
    left join infl i on i.year = p.year
    order by issuer_id, state, year
    """
    return con.execute(q).df()