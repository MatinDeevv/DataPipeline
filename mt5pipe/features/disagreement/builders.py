"""Machine-native disagreement features built from dual-source market state."""

from __future__ import annotations

import polars as pl

from mt5pipe.features.internal.statistics import rolling_shannon_entropy


def add_disagreement_features(
    df: pl.DataFrame,
    *,
    time_col: str | None = None,
    zscore_window: int = 60,
    burst_window: int = 15,
    entropy_window: int = 30,
) -> pl.DataFrame:
    """Add PIT-safe disagreement features using only the current and trailing rows."""
    resolved_time_col = _resolve_time_col(df, time_col)
    working = df.sort(resolved_time_col)

    reference_price = _column_or_value(
        working,
        preferred=["close", "mid"],
        default=1.0,
    ).clip(lower_bound=1e-9)
    spread_value = _column_or_value(
        working,
        preferred=["spread_mean", "spread"],
        default=0.0,
    ).clip(lower_bound=0.0)
    spread_bps = (spread_value / reference_price) * 10_000.0

    tick_count = _column_or_value(working, preferred=["tick_count"], default=1.0).clip(lower_bound=1.0)
    conflict_count = _column_or_value(working, preferred=["conflict_count"], default=0.0).clip(lower_bound=0.0)
    conflict_event = (conflict_count > 0).cast(pl.Float64)
    dual_source_ratio = _column_or_value(working, preferred=["dual_source_ratio"], default=0.0).clip(
        lower_bound=0.0,
        upper_bound=1.0,
    )
    dual_source_gap = (1.0 - dual_source_ratio).clip(lower_bound=0.0, upper_bound=1.0)
    secondary_present = _column_or_value(working, preferred=["secondary_present_ticks"], default=0.0).clip(lower_bound=0.0)
    dual_source_ticks = _column_or_value(working, preferred=["dual_source_ticks"], default=0.0).clip(lower_bound=0.0)
    disagreement_bps = _column_or_value(working, preferred=["disagreement_bps"], default=None)

    secondary_gap_ratio = ((secondary_present - dual_source_ticks).clip(lower_bound=0.0) / tick_count).clip(
        lower_bound=0.0,
        upper_bound=1.0,
    )
    conflict_ratio = (conflict_count / tick_count).clip(lower_bound=0.0, upper_bound=1.0)

    working = working.with_columns(
        dual_source_gap.alias("_dual_source_gap"),
        conflict_event.alias("_conflict_event"),
        secondary_gap_ratio.alias("_secondary_gap_ratio"),
        conflict_ratio.alias("_conflict_ratio"),
    )
    working = working.with_columns(
        pl.when(disagreement_bps.is_not_null())
        .then(disagreement_bps.abs())
        .otherwise((pl.col("_dual_source_gap") * spread_bps).clip(lower_bound=0.0))
        .alias("mid_divergence_proxy_bps"),
        ((pl.col("_secondary_gap_ratio") + pl.col("_conflict_ratio")) * spread_bps).clip(lower_bound=0.0).alias(
            "spread_divergence_proxy_bps"
        ),
    )
    working = working.with_columns(
        (
            pl.col("mid_divergence_proxy_bps")
            + pl.col("spread_divergence_proxy_bps")
            + (pl.col("_conflict_event") * spread_bps)
        ).alias("disagreement_pressure_bps")
    )
    working = working.with_columns(
        pl.col("disagreement_pressure_bps")
        .rolling_mean(window_size=burst_window, min_samples=burst_window)
        .alias("disagreement_burst_15"),
        pl.col("_conflict_event")
        .rolling_mean(window_size=burst_window, min_samples=burst_window)
        .alias("conflict_burst_15"),
        pl.col("_secondary_gap_ratio")
        .rolling_mean(window_size=burst_window, min_samples=burst_window)
        .alias("staleness_asymmetry_15"),
    )
    working = working.with_columns(
        (
            (
                pl.col("disagreement_pressure_bps")
                - pl.col("disagreement_pressure_bps").rolling_mean(window_size=zscore_window, min_samples=zscore_window)
            )
            / pl.col("disagreement_pressure_bps").rolling_std(window_size=zscore_window, min_samples=zscore_window)
        ).alias("disagreement_zscore_60")
    )
    working = working.with_columns(
        pl.when(pl.col("disagreement_zscore_60").is_infinite())
        .then(None)
        .otherwise(pl.col("disagreement_zscore_60"))
        .alias("disagreement_zscore_60")
    )

    conflict_series = [int(value) for value in working["_conflict_event"].to_list()]
    working = working.with_columns(
        pl.Series(
            "disagreement_entropy_30",
            rolling_shannon_entropy(conflict_series, entropy_window),
            dtype=pl.Float64,
        )
    )

    return working.drop(["_dual_source_gap", "_conflict_event", "_secondary_gap_ratio", "_conflict_ratio"])


def _resolve_time_col(df: pl.DataFrame, time_col: str | None) -> str:
    if time_col is not None:
        return time_col
    if "time_utc" in df.columns:
        return "time_utc"
    if "ts_utc" in df.columns:
        return "ts_utc"
    raise KeyError("disagreement features require a time column")


def _column_or_value(df: pl.DataFrame, *, preferred: list[str], default: float | None) -> pl.Expr:
    for column in preferred:
        if column in df.columns:
            return pl.col(column).cast(pl.Float64)
    return pl.lit(default, dtype=pl.Float64)
