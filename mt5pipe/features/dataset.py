"""Dataset assembly — builds model-ready datasets from bars + features + labels."""

from __future__ import annotations

import datetime as dt
import shutil
from typing import Any

import polars as pl

from mt5pipe.config.models import DatasetConfig
from mt5pipe.features.context import add_lagged_bar_features
from mt5pipe.features.quality import add_spread_quality_features
from mt5pipe.features.session import add_session_features
from mt5pipe.features.time import add_time_features
from mt5pipe.features.labels import (
    add_direction_labels,
    add_future_returns,
    add_triple_barrier_labels,
)
from mt5pipe.quality.cleaning import clean_dataset, validate_bars
from mt5pipe.quality.gaps import detect_gaps, fill_bar_gaps
from mt5pipe.quality.report import dataset_quality_report, format_quality_report
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths
from mt5pipe.utils.logging import get_logger

log = get_logger(__name__)


def build_dataset(
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    paths: StoragePaths,
    store: ParquetStore,
    cfg: DatasetConfig,
    dataset_name: str = "default",
) -> pl.DataFrame:
    """Build a complete model-ready dataset.
    
    1. Load base timeframe bars (M1)
    2. Validate & clean bars (OHLC integrity, remove invalids)
    3. Detect & fill gaps (weekend-aware, forward-fill small gaps)
    4. Add time/session features
    5. Add spread/quality features
    6. Join higher-TF context (last *closed* bar only — shifted by bar duration)
    7. Add labels
    8. Filter out synthetic / filled rows (not trainable)
    9. Final dataset cleaning (drop high-null cols, replace inf, drop constant cols)
    10. Quality report
    11. Split into train/val/test
    12. Write to disk
    """
    from mt5pipe.bars.builder import timeframe_to_seconds

    # 1. Load base bars
    base_tf = cfg.base_timeframe
    base_bars = _load_bars_range(symbol, base_tf, start_date, end_date, paths, store)

    if base_bars.is_empty():
        log.warning("dataset_no_base_bars", symbol=symbol, tf=base_tf)
        return pl.DataFrame()

    log.info("dataset_base_loaded", symbol=symbol, rows=len(base_bars))

    # 2. Validate bar integrity
    base_bars = validate_bars(base_bars)
    log.info("dataset_bars_validated", rows=len(base_bars))

    # 3. Detect & fill gaps
    tf_secs = timeframe_to_seconds(base_tf)
    gap_report = detect_gaps(base_bars, base_tf, tf_secs)
    if gap_report.missing_bars > 0:
        log.info(
            "dataset_gaps_detected",
            missing=gap_report.missing_bars,
            completeness=f"{gap_report.completeness_pct:.1f}%",
        )
        base_bars = fill_bar_gaps(base_bars, base_tf, tf_secs)
        log.info("dataset_gaps_filled", rows=len(base_bars))
    else:
        if "_filled" not in base_bars.columns:
            base_bars = base_bars.with_columns(pl.lit(False).alias("_filled"))

    # 4. Time features
    base_bars = add_time_features(base_bars)

    # 5. Session features
    base_bars = add_session_features(base_bars)

    # 6. Spread/quality features
    base_bars = add_spread_quality_features(base_bars)

    # 7. Higher-TF context (shifted by bar duration — prevents look-ahead)
    for htf in cfg.context_timeframes:
        htf_bars = _load_bars_range(symbol, htf, start_date, end_date, paths, store)
        if not htf_bars.is_empty():
            htf_bars = validate_bars(htf_bars)
            htf_secs = timeframe_to_seconds(htf)
            base_bars = add_lagged_bar_features(
                base_bars, htf_bars, htf, bar_duration_seconds=htf_secs,
            )
            log.info("dataset_htf_joined", htf=htf, bar_dur_s=htf_secs, cols_added=True)

    # 8. Labels
    base_bars = add_future_returns(base_bars, cfg.horizons_minutes)
    base_bars = add_direction_labels(base_bars, cfg.horizons_minutes)
    base_bars = add_triple_barrier_labels(
        base_bars, cfg.horizons_minutes,
        tp_bps=cfg.triple_barrier_tp_bps,
        sl_bps=cfg.triple_barrier_sl_bps,
        vol_scale_window=cfg.triple_barrier_vol_lookback,
        vol_multiplier=cfg.triple_barrier_vol_multiplier,
    )

    # Drop rows with null labels (end of dataset where forward returns unavailable)
    # Use purge zone: remove max_horizon rows from the end to avoid label leakage
    max_horizon = max(cfg.horizons_minutes)
    purge_rows = max_horizon + 1  # +1 safety margin for edge effects
    if len(base_bars) > purge_rows:
        base_bars = base_bars.head(len(base_bars) - purge_rows)
    else:
        log.warning("dataset_too_short_for_labels", rows=len(base_bars), purge_needed=purge_rows)
        return pl.DataFrame()

    # 8b. Filter out synthetic / filled rows (not trainable for ML)
    pre_filter = len(base_bars)
    if "_filled" in base_bars.columns:
        base_bars = base_bars.filter(~pl.col("_filled"))
    if "source_count" in base_bars.columns:
        base_bars = base_bars.filter(pl.col("source_count") > 0)
    filtered_out = pre_filter - len(base_bars)
    if filtered_out > 0:
        log.info(
            "dataset_filled_rows_removed",
            removed=filtered_out,
            remaining=len(base_bars),
        )

    # 9. Final dataset-level cleaning
    base_bars, clean_stats = clean_dataset(base_bars)

    # 10. Quality report
    qr = dataset_quality_report(base_bars)
    log.info("dataset_built", symbol=symbol, rows=len(base_bars),
             cols=len(base_bars.columns), quality_score=qr.get("quality_score", 0))

    # 11. Split with embargo zones to prevent data leakage
    # Embargo = max label horizon bars between splits (hedge fund standard)
    embargo = max(cfg.horizons_minutes)
    n = len(base_bars)
    train_end = int(n * cfg.train_ratio)
    val_start = train_end + embargo
    val_end = int(n * (cfg.train_ratio + cfg.val_ratio))
    test_start = val_end + embargo

    train_df = base_bars.slice(0, train_end)
    val_df = base_bars.slice(val_start, max(0, val_end - val_start)) if val_start < n else pl.DataFrame()
    test_df = base_bars.slice(test_start, max(0, n - test_start)) if test_start < n else pl.DataFrame()

    log.info(
        "dataset_split_info",
        train=len(train_df),
        val=len(val_df),
        test=len(test_df),
        embargo_rows=embargo,
    )

    # Ensure split outputs are clean for this build so old runs cannot leak across splits.
    for split_name in ("train", "val", "test"):
        split_dir = paths.dataset_dir(dataset_name, split_name)
        if split_dir.exists():
            shutil.rmtree(split_dir)

    # Write splits
    for split_name, split_df in [("train", train_df), ("val", val_df), ("test", test_df)]:
        if not split_df.is_empty():
            path = paths.dataset_file(dataset_name, split_name)
            store.write(split_df, path)
            log.info(
                "dataset_split_written",
                split=split_name,
                rows=len(split_df),
                path=str(path),
            )

    return base_bars


def _load_bars_range(
    symbol: str,
    timeframe: str,
    start_date: dt.date,
    end_date: dt.date,
    paths: StoragePaths,
    store: ParquetStore,
) -> pl.DataFrame:
    """Load built bars for a date range."""
    frames: list[pl.DataFrame] = []
    current = start_date
    while current <= end_date:
        bar_dir = paths.built_bars_dir(symbol, timeframe, current)
        df = store.read_dir(bar_dir)
        if not df.is_empty():
            frames.append(df)
        current += dt.timedelta(days=1)

    if not frames:
        return pl.DataFrame()

    result = pl.concat(frames, how="diagonal_relaxed")
    return result.sort("time_utc")


def walk_forward_splits(
    df: pl.DataFrame,
    n_splits: int = 5,
    train_pct: float = 0.6,
    gap_pct: float = 0.02,
    embargo_rows: int = 240,
) -> list[tuple[pl.DataFrame, pl.DataFrame]]:
    """Generate walk-forward train/test splits for time-series cross-validation.
    
    Returns list of (train, test) DataFrames.
    No data leakage: train always precedes test with an embargo gap.
    Embargo default = 240 rows (4 hours at M1 frequency).
    """
    n = len(df)
    splits: list[tuple[pl.DataFrame, pl.DataFrame]] = []
    step = int(n * (1.0 - train_pct) / n_splits)
    gap = max(int(n * gap_pct), embargo_rows)

    for i in range(n_splits):
        test_start = int(n * train_pct) + i * step
        test_end = min(test_start + step, n)
        train_end = test_start - gap

        if train_end < 1 or test_start >= n:
            continue

        train = df.slice(0, train_end)
        test = df.slice(test_start, test_end - test_start)

        if train.is_empty() or test.is_empty():
            continue

        splits.append((train, test))

    return splits
