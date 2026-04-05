"""Machine-native entropy and complexity features."""

from __future__ import annotations

import polars as pl

from mt5pipe.features.internal.statistics import (
    rolling_approximate_entropy,
    rolling_permutation_entropy,
    rolling_sample_entropy,
    rolling_shannon_entropy,
)


def add_entropy_features(
    df: pl.DataFrame,
    *,
    time_col: str | None = None,
    window: int = 30,
) -> pl.DataFrame:
    """Add PIT-safe entropy metrics over trailing returns and volatility."""
    resolved_time_col = _resolve_time_col(df, time_col)
    working = df.sort(resolved_time_col)

    return_values = _series_from_columns(working, preferred=["mid_return"], default=0.0)
    sign_values = [_sign_bucket(value) for value in return_values]
    volatility_values = _series_from_columns(working, preferred=["realized_vol", "spread_mean"], default=0.0)

    working = working.with_columns(
        pl.Series(
            "return_sign_shannon_entropy_30",
            rolling_shannon_entropy(sign_values, window),
            dtype=pl.Float64,
        ),
        pl.Series(
            "return_permutation_entropy_30",
            rolling_permutation_entropy(return_values, window, order=3, delay=1),
            dtype=pl.Float64,
        ),
        pl.Series(
            "return_sample_entropy_30",
            rolling_sample_entropy(return_values, window, pattern_size=2, tolerance_scale=0.2),
            dtype=pl.Float64,
        ),
        pl.Series(
            "volatility_approx_entropy_30",
            rolling_approximate_entropy(volatility_values, window, pattern_size=2, tolerance_scale=0.2),
            dtype=pl.Float64,
        ),
    )
    return working


def _resolve_time_col(df: pl.DataFrame, time_col: str | None) -> str:
    if time_col is not None:
        return time_col
    if "time_utc" in df.columns:
        return "time_utc"
    if "ts_utc" in df.columns:
        return "ts_utc"
    raise KeyError("entropy features require a time column")


def _series_from_columns(df: pl.DataFrame, *, preferred: list[str], default: float) -> list[float | None]:
    for column in preferred:
        if column in df.columns:
            return [float(value) if value is not None else None for value in df[column].to_list()]
    return [default] * len(df)


def _sign_bucket(value: float | None) -> int:
    if value is None or value == 0.0:
        return 0
    return 1 if value > 0.0 else -1
