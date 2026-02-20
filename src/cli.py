from __future__ import annotations
import typer
from rich import print
from dotenv import load_dotenv

from src.config import get_paths, fred_key
from src.ingest_mlr import download_mlr_zips, build_mlr_panel
from src.ingest_fred import build_inflation
from src.build_panel import build_panel

app = typer.Typer(add_completion=False)

@app.command()
def run(
    start_year: int = 2017,
    end_year: int = 2024,
    commit_processed: bool = False
):
    """
    End-to-end: download MLR zips -> build MLR panel -> pull inflation -> build model panel.
    """
    load_dotenv()
    paths = get_paths()

    years = list(range(start_year, end_year + 1))

    print("[cyan]1) Downloading CMS MLR ZIPs (cached in data_raw/)...[/cyan]")
    zip_paths = download_mlr_zips(paths.raw, years=years)

    print("[cyan]2) Building MLR panel (issuer-state-year)...[/cyan]")
    mlr = build_mlr_panel(zip_paths)

    print("[cyan]3) Pulling CPI/PPI from FRED...[/cyan]")
    infl = build_inflation(fred_key())

    print("[cyan]4) Building final panel + features...[/cyan]")
    panel = build_panel(mlr, infl)

    out = paths.processed / "panel.parquet"
    panel.to_parquet(out, index=False)
    print(f"[green]Wrote:[/green] {out}  rows={len(panel):,}")

    if commit_processed:
        print("[yellow]Note:[/yellow] You chose commit_processed=True — consider tracking only panel.parquet, not raw data.")

if __name__ == "__main__":
    app()