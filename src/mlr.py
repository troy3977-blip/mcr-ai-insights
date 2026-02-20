from __future__ import annotations

import re
import zipfile
from pathlib import Path
import requests
import pandas as pd
from bs4 import BeautifulSoup

CMS_MLR_PAGE = "https://www.cms.gov/marketplace/resources/data/medical-loss-ratio-data-systems-resources"

def _download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def discover_mlr_zip_links() -> list[tuple[str, str]]:
    """
    Returns [(year, zip_url), ...] discovered from CMS page.
    """
    html = requests.get(CMS_MLR_PAGE, timeout=60).text
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip()
        # Look for "Public Use File for 20xx" and ZIP links
        m = re.search(r"Public Use File for (\d{4})", text)
        if m and href.lower().endswith(".zip"):
            year = m.group(1)
            url = href if href.startswith("http") else "https://www.cms.gov" + href
            links.append((year, url))
    # Deduplicate by year, keep first
    out = {}
    for y, u in links:
        out.setdefault(y, u)
    # Sort by year
    return sorted(out.items(), key=lambda x: x[0])

def download_mlr_zips(raw_dir: Path, years: list[int] | None = None) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    found = discover_mlr_zip_links()
    if years:
        found = [(y, u) for (y, u) in found if int(y) in set(years)]
    if not found:
        raise RuntimeError("No ZIP links discovered. CMS page structure may have changed. Download manually as fallback.")
    paths = []
    for y, url in found:
        out_path = raw_dir / f"mlr_puf_{y}.zip"
        _download(url, out_path)
        paths.append(out_path)
    return paths

def _read_first_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise ValueError(f"No CSV found inside {zip_path.name}")
        with z.open(csvs[0]) as f:
            return pd.read_csv(f)

def _snake(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s)
    return s.lower()

def build_mlr_panel(zip_paths: list[Path]) -> pd.DataFrame:
    frames = []
    for zp in zip_paths:
        df = _read_first_csv_from_zip(zp)
        df.columns = [_snake(c) for c in df.columns]

        # Required columns (CMS naming can vary slightly across years)
        # We try common variants.
        def pick(*names: str) -> str:
            for n in names:
                if n in df.columns:
                    return n
            raise KeyError(f"Missing expected columns in {zp.name}; columns={df.columns.tolist()[:25]}...")

        issuer_id = pick("issuer_id")
        issuer_name = df["issuer_name"] if "issuer_name" in df.columns else None
        state = pick("state")
        market = pick("market")
        year = pick("mlr_reporting_year", "reporting_year", "mlr_year")
        earned = pick("earned_premiums", "earned_premium", "earnedpremium")
        incurred = pick("incurred_claims", "incurredclaims")
        qi = df["quality_improvement_expenses"] if "quality_improvement_expenses" in df.columns else pd.NA

        out = pd.DataFrame({
            "issuer_id": df[issuer_id].astype(str),
            "state": df[state].astype(str).str.upper().str.strip(),
            "market": df[market].astype(str).str.strip(),
            "year": pd.to_numeric(df[year], errors="coerce").astype("Int64"),
            "earned_premium": pd.to_numeric(df[earned], errors="coerce"),
            "incurred_claims": pd.to_numeric(df[incurred], errors="coerce"),
            "qi_expenses": pd.to_numeric(qi, errors="coerce") if not isinstance(qi, type(pd.NA)) else pd.NA,
        })
        if issuer_name is not None:
            out["issuer_name"] = issuer_name.astype(str)

        frames.append(out)

    panel = pd.concat(frames, ignore_index=True)
    panel = panel[panel["market"].isin(["Individual", "Small Group"])].copy()
    panel = panel.dropna(subset=["issuer_id", "state", "year"])
    panel = panel[panel["earned_premium"] > 0].copy()
    return panel