"""Dataset builder CLI commands."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import typer

from mt5pipe.config.loader import load_config
from mt5pipe.utils.logging import setup_logging

dataset_app = typer.Typer(help="Build model-ready datasets")


def _build_compiler(config_path: str):
    cfg = load_config(Path(config_path))
    setup_logging(cfg.logging.level, cfg.logging.json_output)

    from mt5pipe.catalog.sqlite import CatalogDB
    from mt5pipe.compiler.service import DatasetCompiler
    from mt5pipe.storage.parquet_store import ParquetStore
    from mt5pipe.storage.paths import StoragePaths

    paths = StoragePaths(cfg.storage.root)
    store = ParquetStore(cfg.storage.compression, cfg.storage.parquet_row_group_size)
    catalog = CatalogDB(paths.catalog_db_path())
    compiler = DatasetCompiler(cfg, paths, store, catalog)
    return compiler, catalog


@dataset_app.command("build")
def build_dataset_cmd(
    symbol: str = typer.Option("XAUUSD"),
    date_from: str = typer.Option(..., "--from"),
    date_to: str = typer.Option(..., "--to"),
    name: str = typer.Option("default", help="Dataset name"),
    base_timeframe: str = typer.Option("", help="Override base timeframe from config"),
    config_path: str = typer.Option("config/pipeline.yaml", "--config"),
) -> None:
    """Build a model-ready dataset with features and labels."""
    cfg = load_config(Path(config_path))
    setup_logging(cfg.logging.level, cfg.logging.json_output)

    from mt5pipe.features.dataset import build_dataset
    from mt5pipe.storage.parquet_store import ParquetStore
    from mt5pipe.storage.paths import StoragePaths

    paths = StoragePaths(cfg.storage.root)
    store = ParquetStore(cfg.storage.compression, cfg.storage.parquet_row_group_size)

    start = dt.date.fromisoformat(date_from)
    end = dt.date.fromisoformat(date_to)

    dataset_cfg = cfg.dataset
    if base_timeframe:
        dataset_cfg = dataset_cfg.model_copy(update={"base_timeframe": base_timeframe})

    df = build_dataset(symbol, start, end, paths, store, dataset_cfg, name)
    if df.is_empty():
        typer.echo("No data available for dataset.")
    else:
        from mt5pipe.quality.report import dataset_quality_report, format_quality_report
        qr = dataset_quality_report(df)
        typer.echo(format_quality_report(qr))
        typer.echo(f"\nDataset '{name}' built: {len(df):,} rows, {len(df.columns)} columns")
        typer.echo(f"Columns: {', '.join(df.columns[:20])}{'...' if len(df.columns) > 20 else ''}")


@dataset_app.command("compile-dataset")
def compile_dataset_cmd(
    spec_path: str = typer.Option(..., "--spec", help="Path to DatasetSpec YAML/JSON"),
    config_path: str = typer.Option("config/pipeline.yaml", "--config"),
) -> None:
    """Compile a versioned dataset artifact from a DatasetSpec."""
    compiler, catalog = _build_compiler(config_path)
    try:
        result = compiler.compile_dataset(Path(spec_path))
        typer.echo(f"artifact_id: {result.artifact_id}")
        typer.echo(f"manifest: {result.manifest_path}")
        typer.echo(f"truth_report: {result.truth_report_path}")
        typer.echo(f"status: {result.manifest.status}")
        typer.echo(f"trust_score_total: {result.trust_report.trust_score_total:.2f}")
        typer.echo(f"split_rows: {result.split_row_counts}")
        if not result.truth_report.accepted_for_publication:
            raise typer.Exit(code=5)
    finally:
        catalog.close()


@dataset_app.command("inspect-dataset")
def inspect_dataset_cmd(
    artifact_ref: str = typer.Option(..., "--artifact", help="Artifact id, logical ref, or manifest path"),
    config_path: str = typer.Option("config/pipeline.yaml", "--config"),
) -> None:
    """Inspect a compiled dataset artifact and its trust report."""
    compiler, catalog = _build_compiler(config_path)
    try:
        result = compiler.inspect_dataset(artifact_ref)
        typer.echo(compiler.format_inspect_result(result))
    finally:
        catalog.close()


@dataset_app.command("diff-dataset")
def diff_dataset_cmd(
    left_ref: str = typer.Option(..., "--left", help="Left artifact id, logical ref, or manifest path"),
    right_ref: str = typer.Option(..., "--right", help="Right artifact id, logical ref, or manifest path"),
    config_path: str = typer.Option("config/pipeline.yaml", "--config"),
) -> None:
    """Compare two compiled dataset artifacts."""
    compiler, catalog = _build_compiler(config_path)
    try:
        result = compiler.diff_datasets(left_ref, right_ref)
        typer.echo(compiler.format_diff_result(result))
    finally:
        catalog.close()
