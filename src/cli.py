from __future__ import annotations

from rich import print
from dotenv import load_dotenv
import typer

from src.config import get_paths, fred_key
from src.ingest_mlr import download_mlr_zips, build_mlr_panel
from src.ingest_fred import build_inflation
from src.build_panel import build_panel

app = typer.Typer(add_completion=False)


@app.callback(invoke_without_command=True)
def main(
    start_year: int = typer.Option(2017, help="First MLR reporting year"),
    end_year: int = typer.Option(2024, help="Last MLR reporting year"),
    diagnostics: bool = typer.Option(True, help="Print MLR ingest diagnostics"),
    include_large_group: bool = typer.Option(False, help="Include Large Group market in extraction"),
    no_inflation: bool = typer.Option(
        False,
        "--no-inflation",
        help="Skip FRED inflation features (does not require FRED_API_KEY).",
    ),
):
    paths = get_paths()
    load_dotenv(paths.root / ".env")

    years = list(range(start_year, end_year + 1))

    print("[cyan]1) Downloading CMS MLR ZIPs (cached in data/raw/)...[/cyan]")
    zip_paths = download_mlr_zips(paths.raw, years=years)

    print("[cyan]2) Building MLR panel (issuer-state-market-year)...[/cyan]")
    mlr = build_mlr_panel(
        zip_paths,
        include_large_group=include_large_group,
        diagnostics=diagnostics,
    )
    print(f"[green]MLR rows:[/green] {len(mlr):,}")

    infl = None
    if no_inflation:
        print("[yellow]3) Skipping CPI/PPI from FRED (--no-inflation).[/yellow]")
    else:
        print("[cyan]3) Pulling CPI/PPI from FRED...[/cyan]")
        key = fred_key()  # will raise if missing
        print(f"[yellow]FRED key loaded (prefix):[/yellow] {key[:6]}...")
        infl = build_inflation(key)
        print(f"[green]Inflation years:[/green] {infl['year'].nunique():,}")

    print("[cyan]4) Building final panel + features...[/cyan]")
    panel = build_panel(mlr, infl)

    out = paths.processed / "panel.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out, index=False)
    print(f"[bold green]Wrote:[/bold green] {out}  rows={len(panel):,}")


if __name__ == "__main__":
    app()