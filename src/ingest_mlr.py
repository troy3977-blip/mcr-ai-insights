# src/ingest_mlr.py
from __future__ import annotations

import re
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from rich import print

CMS_MLR_PAGE = "https://www.cms.gov/marketplace/resources/data/medical-loss-ratio-data-systems-resources"

HEADER_CSV = "MR_Submission_Template_Header.csv"
PART12_CSV = "Part1_2_Summary_Data_Premium_Claims.csv"


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _snake(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"__+", "_", s)
    return s.lower()


def _year_from_zip_name(zip_name: str) -> int:
    m = re.search(r"(\d{4})", zip_name)
    if not m:
        raise ValueError(f"Could not infer year from zip filename: {zip_name}")
    return int(m.group(1))


def _download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def discover_mlr_zip_links() -> list[tuple[int, str]]:
    html = requests.get(CMS_MLR_PAGE, timeout=60).text
    soup = BeautifulSoup(html, "lxml")

    found: list[tuple[int, str]] = []
    for a in soup.find_all("a", href=True):
        href_raw = a.get("href")
        if href_raw is None:
            continue
        href = str(href_raw).strip()
        text = str(a.get_text() or "").strip()

        m = re.search(r"Public Use File for (\d{4})", text)
        if not m:
            continue
        if not href.lower().endswith(".zip"):
            continue

        year = int(m.group(1))
        url = href if href.startswith("http") else "https://www.cms.gov" + href
        found.append((year, url))

    dedup: dict[int, str] = {}
    for y, u in found:
        dedup.setdefault(y, u)
    return sorted(dedup.items(), key=lambda x: x[0])


def download_mlr_zips(raw_dir: Path, years: Iterable[int] | None = None) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    links = discover_mlr_zip_links()

    if years is not None:
        years_set = set(int(y) for y in years)
        links = [(y, u) for (y, u) in links if y in years_set]

    if not links:
        raise RuntimeError(
            "No ZIP links discovered from CMS page. "
            "If CMS changed the page, download ZIPs manually into data/raw/."
        )

    out: list[Path] = []
    for y, url in links:
        p = raw_dir / f"mlr_puf_{y}.zip"
        _download(url, p)
        out.append(p)
    return out


def _pick(df: pd.DataFrame, zip_name: str, *names: str) -> str:
    for n in names:
        if n in df.columns:
            return n
    raise KeyError(
        f"Missing expected columns in {zip_name}; tried={list(names)}; "
        f"columns={df.columns.tolist()[:80]}..."
    )


def _pick_fact_csv(zf: zipfile.ZipFile, zip_name: str) -> str:
    names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    for n in names:
        if Path(n).name.lower() == PART12_CSV.lower():
            return n
    raise KeyError(f"Expected fact file '{PART12_CSV}' not found in {zip_name}. members={names}")


def _pick_dims_csv(zf: zipfile.ZipFile, zip_name: str, *, exclude_member: str) -> str:
    """
    Choose a dims CSV that contains:
      mr_submission_template_id + issuer + state (+ year if present)

    For your 2017 zip, this correctly selects MR_Submission_Template_Header.csv.
    """
    names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    excl = exclude_member.lower()

    issuer_like = {"issuer_id", "hios_issuer_id", "hiosissuerid", "issuerid"}
    year_like = {"reporting_year", "mlr_reporting_year", "mlr_year", "report_year"}
    # NOTE: header uses business_state / domiciliary_state, not "state"
    def score_cols(cols: set[str]) -> int:
        s = 0
        if "mr_submission_template_id" in cols:
            s += 5
        if cols & issuer_like:
            s += 4
        if any("state" in c for c in cols) or ("business_state" in cols) or ("domiciliary_state" in cols):
            s += 3
        if cols & year_like:
            s += 3
        # discourage fact/value tables
        if "row_lookup_code" in cols:
            s -= 6
        if any(c.startswith("cmm_") for c in cols):
            s -= 6
        return s

    best: tuple[int, str] | None = None
    for n in names:
        if n.lower() == excl:
            continue
        try:
            with zf.open(n) as f:
                head = pd.read_csv(f, nrows=1, dtype=str)
            head.columns = [_snake(c) for c in head.columns]
            cols = set(head.columns)
            sc = score_cols(cols)
            if best is None or sc > best[0]:
                best = (sc, n)
        except Exception:
            continue

    if best is None or best[0] < 10:
        raise KeyError(
            f"Could not locate dims CSV in {zip_name}. Exclude={exclude_member}. Members={names}"
        )

    return best[1]


def _pick_market_value_col(df: pd.DataFrame, zip_name: str, market_key: str) -> str:
    # Prefer yearly -> cy -> total
    c1 = f"cmm_{market_key}_yearly"
    if c1 in df.columns:
        return c1
    c2 = f"cmm_{market_key}_cy"
    if c2 in df.columns:
        return c2
    c3 = f"cmm_{market_key}_total"
    if c3 in df.columns:
        return c3

    candidates = [c for c in df.columns if c.startswith("cmm_") and market_key in c]
    if candidates:
        pref = (
            [c for c in candidates if c.endswith("_yearly")]
            + [c for c in candidates if c.endswith("_cy")]
            + [c for c in candidates if c.endswith("_total")]
        )
        return pref[0] if pref else sorted(candidates, key=len)[0]

    raise KeyError(
        f"Could not find market value column for market_key='{market_key}' in {zip_name}. "
        f"columns={df.columns.tolist()[:80]}..."
    )


# ---------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------
def build_mlr_panel(
    zip_paths: list[Path],
    *,
    include_large_group: bool = False,
    diagnostics: bool = False,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    PREMIUM_CODE = "TOTAL_DIRECT_PREMIUM_EARNED"
    CLAIM_CODE_CANDIDATES = [
        "TOTAL_INCURRED_CLAIMS_PT2",
        "TOTAL_INCURRED_CLAIMS",
        "TOTAL_INCURRED_CLAIMS_PT1",
    ]

    for zp in zip_paths:
        zip_year = _year_from_zip_name(zp.name)

        with zipfile.ZipFile(zp) as zf:
            fact_member = _pick_fact_csv(zf, zp.name)
            dims_member = _pick_dims_csv(zf, zp.name, exclude_member=fact_member)

            with zf.open(fact_member) as f:
                fact = pd.read_csv(f, dtype=str)
            fact.columns = [_snake(c) for c in fact.columns]

            with zf.open(dims_member) as f:
                dims = pd.read_csv(f, dtype=str)
            dims.columns = [_snake(c) for c in dims.columns]

        if diagnostics:
            print(" fact_member=" + Path(fact_member).name)
            print(" dims_member=" + Path(dims_member).name)
            print(f" dims cols sample={dims.columns.tolist()[:35]}")

        # fact keys
        id_fact = _pick(fact, zp.name, "mr_submission_template_id")
        row_code_col = _pick(fact, zp.name, "row_lookup_code", "lookup_code", "row_code")

        # dims keys
        id_dims = _pick(dims, zp.name, "mr_submission_template_id")

        issuer_id_col = _pick(
            dims,
            zp.name,
            "hios_issuer_id",
            "issuer_id",
            "hiosissuerid",
            "issuerid",
        )

        # header-specific state fields
        state_col = _pick(
            dims,
            zp.name,
            "business_state",
            "domiciliary_state",
            "state",
            "state_code",
        )

        # year may not exist in header; if missing, we infer from zip name
        year_col = None
        for cand in ("mlr_reporting_year", "reporting_year", "mlr_year", "report_year"):
            if cand in dims.columns:
                year_col = cand
                break

        issuer_name_col = None
        for c in ("company_name", "issuer_name", "issuer_legal_name", "legal_entity_name", "dba_marketing_name"):
            if c in dims.columns:
                issuer_name_col = c
                break

        # join dims onto fact
        dims_keep = [id_dims, issuer_id_col, state_col] + ([issuer_name_col] if issuer_name_col else [])
        if year_col:
            dims_keep.append(year_col)

        dims2 = dims[dims_keep].copy().rename(columns={id_dims: "mr_submission_template_id"})
        if not year_col:
            dims2["year"] = zip_year
            year_col_use = "year"
        else:
            year_col_use = year_col

        fact2 = fact.copy().rename(columns={id_fact: "mr_submission_template_id"})
        merged = fact2.merge(dims2, on="mr_submission_template_id", how="left")

        if merged[issuer_id_col].isna().all():
            raise RuntimeError(
                f"Join failed for {zp.name}. dims_member={dims_member} did not provide issuer_id. "
                f"dims cols sample={dims.columns.tolist()[:50]}"
            )

        # choose claim code present
        available_codes = set(merged[row_code_col].dropna().astype(str))
        claim_code = None
        for cc in CLAIM_CODE_CANDIDATES:
            if cc in available_codes:
                claim_code = cc
                break
        if claim_code is None:
            raise KeyError(
                f"No claim row code found in {zp.name}. Tried {CLAIM_CODE_CANDIDATES}; "
                f"sample codes={list(sorted(available_codes))[:30]}"
            )
        if diagnostics:
            print(f"[{zp.name}] claim row code: {claim_code}")

        # market columns
        market_cols: list[tuple[str, str]] = [
            ("Individual", _pick_market_value_col(fact, zp.name, "individual")),
            ("Small Group", _pick_market_value_col(fact, zp.name, "small_group")),
        ]
        if include_large_group:
            try:
                market_cols.append(("Large Group", _pick_market_value_col(fact, zp.name, "large_group")))
            except KeyError:
                pass

        # pivot keys
        base_keys = ["mr_submission_template_id", issuer_id_col, state_col, year_col_use]
        if issuer_name_col:
            base_keys.insert(2, issuer_name_col)

        for market_name, value_col in market_cols:
            sub = merged[base_keys + [row_code_col, value_col]].copy()
            sub[value_col] = pd.to_numeric(sub[value_col], errors="coerce")

            wide = (
                sub.pivot_table(
                    index=base_keys,
                    columns=row_code_col,
                    values=value_col,
                    aggfunc="first",
                )
                .reset_index()
            )

            if PREMIUM_CODE not in wide.columns:
                raise KeyError(
                    f"Missing premium row code '{PREMIUM_CODE}' after pivot for {zp.name} ({market_name}). "
                    f"Available={list(wide.columns)[:80]}..."
                )
            if claim_code not in wide.columns:
                raise KeyError(
                    f"Missing claim row code '{claim_code}' after pivot for {zp.name} ({market_name}). "
                    f"Available={list(wide.columns)[:80]}..."
                )

            out = pd.DataFrame(
                {
                    "issuer_id": wide[issuer_id_col].astype(str).str.strip(),
                    "issuer_name": wide[issuer_name_col].astype(str).str.strip() if issuer_name_col else "",
                    "state": wide[state_col].astype(str).str.upper().str.strip(),
                    "market": market_name,
                    "year": pd.to_numeric(wide[year_col_use], errors="coerce").astype("Int64"),
                    "earned_premium": pd.to_numeric(wide[PREMIUM_CODE], errors="coerce"),
                    "incurred_claims": pd.to_numeric(wide[claim_code], errors="coerce"),
                }
            )
            frames.append(out)

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.dropna(subset=["issuer_id", "state", "year"]).copy()
    panel = panel[panel["earned_premium"].fillna(0) > 0].copy()

    keep = {"Individual", "Small Group"}
    if include_large_group:
        keep.add("Large Group")
    panel = panel[panel["market"].isin(sorted(keep))].copy()

    return panel.reset_index(drop=True)