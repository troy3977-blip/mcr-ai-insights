from __future__ import annotations

import typer
from dotenv import load_dotenv
from rich import print

from mcr_ai_insights.build_panel import build_panel
from mcr_ai_insights.config import fred_key, get_paths
from mcr_ai_insights.export_panel import export_panel as export_panel_artifacts
from mcr_ai_insights.ingest_fred import build_inflation
from mcr_ai_insights.ingest_mlr import build_mlr_panel, download_mlr_zips

app = typer.Typer(add_completion=False)


@app.callback(invoke_without_command=True)
def main(
    start_year: int = typer.Option(2017, help="First MLR reporting year"),
    end_year: int = typer.Option(2024, help="Last MLR reporting year"),
    diagnostics: bool = typer.Option(True, help="Print MLR ingest diagnostics"),
    include_large_group: bool = typer.Option(
        False, help="Include Large Group market in extraction"
    ),
    no_inflation: bool = typer.Option(
        False,
        "--no-inflation",
        help="Skip FRED inflation features (does not require FRED_API_KEY).",
    ),
    fred_api_key: str | None = typer.Option(
        None,
        "--fred-api-key",
        envvar="FRED_API_KEY",
        help=(
            "FRED API key (overrides env/.env). You can also set FRED_API_KEY "
            "in your environment or .env."
        ),
        show_default=False,
    ),
):
    """
    Build the primary panel (data/processed/panel.parquet) from CMS MLR PUF
    + optional FRED inflation.
    """
    paths = get_paths()
    load_dotenv(paths.root / ".env")

    if start_year > end_year:
        typer.secho("start-year must be <= end-year.", fg=typer.colors.RED)
        raise typer.Exit(code=2)

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
        # Prefer explicit CLI/envvar via typer (fred_api_key). Fall back to your config helper.
        key = fred_api_key
        if not key:
            try:
                key = fred_key()
            except RuntimeError:
                typer.secho(
                    "Missing FRED_API_KEY.\n\n"
                    "Fix options:\n"
                    "  1) Run without inflation: mcr-ai --no-inflation\n"
                    "  2) Set it in .env: FRED_API_KEY=... (repo root)\n"
                    "  3) Provide it inline: mcr-ai --fred-api-key <KEY>\n",
                    fg=typer.colors.RED,
                )
                raise typer.Exit(code=1) from None

        print("[cyan]3) Pulling CPI/PPI from FRED...[/cyan]")
        print(f"[yellow]FRED key loaded (prefix):[/yellow] {key[:6]}...")
        infl = build_inflation(key)
        print(f"[green]Inflation years:[/green] {infl['year'].nunique():,}")

    print("[cyan]4) Building final panel + features...[/cyan]")
    panel = build_panel(mlr, infl)

    out = paths.processed / "panel.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out, index=False)
    print(f"[bold green]Wrote:[/bold green] {out}  rows={len(panel):,}")


@app.command()
def export(
    min_years: int = typer.Option(
        3,
        help="Min distinct years per issuer/state/market to include in stable panel",
    ),
    w_cap: float = typer.Option(10.0, help="Cap for premium-based modeling weights"),
    input_name: str = typer.Option(
        "panel.parquet",
        help="Input parquet in data/processed/",
    ),
):
    """
    Create model-ready artifacts from data/processed/<input_name>:
      - panel_model.parquet (adds premium_weight, w, premium_weight_year, w_year)
      - panel_stable.parquet (subset with >= min_years distinct years)
    """
    paths = get_paths()
    load_dotenv(paths.root / ".env")

    model_df, stable_df = export_panel_artifacts(
        paths.processed,
        input_name=input_name,
        min_years=min_years,
        w_cap=w_cap,
    )

    out_model = paths.processed / "panel_model.parquet"
    out_stable = paths.processed / "panel_stable.parquet"

    print(f"[bold green]Wrote:[/bold green] {out_model} rows={len(model_df):,}")
    print(f"[bold green]Wrote:[/bold green] {out_stable} rows={len(stable_df):,}")


if __name__ == "__main__":
    app()
