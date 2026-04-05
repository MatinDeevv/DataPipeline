"""Machine-native event-shape features built from trailing bar activity."""

from __future__ import annotations

import polars as pl

from mt5pipe.features.internal.statistics import signed_run_lengths, switch_indicators


def add_event_shape_features(
    df: pl.DataFrame,
    *,
    time_col: str | None = None,
    bar_duration_seconds: int = 60,
    window: int = 20,
) -> pl.DataFrame:
    """Add PIT-safe event-shape features using only current and trailing rows."""
    resolved_time_col = _resolve_time_col(df, time_col)
    working = df.sort(resolved_time_col)

    tick_count = _column_or_value(working, preferred=["tick_count"], default=1.0).clip(lower_bound=1.0)
    mid_return = _column_or_value(working, preferred=["mid_return"], default=0.0)
    filled_flag = _column_or_value(working, preferred=["_filled"], default=0.0).clip(lower_bound=0.0, upper_bound=1.0)

    working = working.with_columns(
        (tick_count / float(bar_duration_seconds)).alias("tick_rate_hz"),
        ((float(bar_duration_seconds) * 1000.0) / tick_count).alias("interarrival_mean_ms"),
        ((tick_count <= 1.0) | (filled_flag > 0.0)).cast(pl.Float64).alias("_silence_event"),
    )
    working = working.with_columns(
        (
            (
                tick_count.rolling_std(window_size=window, min_samples=window)
                - tick_count.rolling_mean(window_size=window, min_samples=window)
            )
            / (
                tick_count.rolling_std(window_size=window, min_samples=window)
                + tick_count.rolling_mean(window_size=window, min_samples=window)
            )
        ).alias("burstiness_20"),
        pl.col("_silence_event").rolling_mean(window_size=window, min_samples=window).alias("silence_ratio_20"),
        (
            mid_return.abs().rolling_sum(window_size=window, min_samples=window)
        ).alias("_rolling_abs_path"),
        mid_return.rolling_sum(window_size=window, min_samples=window).abs().alias("_rolling_net_path"),
    )
    working = working.with_columns(
        pl.when(pl.col("_rolling_abs_path") > 0.0)
        .then(pl.col("_rolling_net_path") / pl.col("_rolling_abs_path"))
        .otherwise(0.0)
        .alias("path_efficiency_20")
    )
    working = working.with_columns(
        (1.0 - pl.col("path_efficiency_20")).clip(lower_bound=0.0, upper_bound=1.0).alias("tortuosity_20"),
    )
    working = working.with_columns(
        pl.when(pl.col("burstiness_20").is_infinite()).then(None).otherwise(pl.col("burstiness_20")).alias("burstiness_20")
    )

    mid_return_values = [float(value) if value is not None else None for value in working.select(mid_return.alias("mid_return"))["mid_return"].to_list()]
    switches = switch_indicators(mid_return_values)
    run_lengths = signed_run_lengths(mid_return_values)
    working = working.with_columns(
        pl.Series("_direction_switch_event", switches, dtype=pl.Int64),
        pl.Series("signed_run_length", run_lengths, dtype=pl.Int64),
    )
    working = working.with_columns(
        pl.col("_direction_switch_event")
        .cast(pl.Float64)
        .rolling_mean(window_size=window, min_samples=window)
        .alias("direction_switch_rate_20")
    )

    return working.drop(["_silence_event", "_rolling_abs_path", "_rolling_net_path", "_direction_switch_event"])


def _resolve_time_col(df: pl.DataFrame, time_col: str | None) -> str:
    if time_col is not None:
        return time_col
    if "time_utc" in df.columns:
        return "time_utc"
    if "ts_utc" in df.columns:
        return "ts_utc"
    raise KeyError("event-shape features require a time column")


def _column_or_value(df: pl.DataFrame, *, preferred: list[str], default: float) -> pl.Expr:
    for column in preferred:
        if column in df.columns:
            return pl.col(column).cast(pl.Float64)
    return pl.lit(default, dtype=pl.Float64)
