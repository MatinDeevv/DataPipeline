"""Pure state-frame and rolling-window builders."""

from __future__ import annotations

import datetime as dt
import math
from typing import Callable

import polars as pl

from mt5pipe.bars.builder import timeframe_to_seconds
from mt5pipe.contracts.state import parse_window_size
from mt5pipe.quality.gaps import _is_forex_closed
from mt5pipe.state.models import StateSnapshot, StateWindowRecord


def session_code(ts_utc: dt.datetime) -> str:
    """Map UTC timestamps to the repo's canonical session labels."""
    if _is_forex_closed(ts_utc):
        return "weekend_closed"
    hour = ts_utc.hour
    if 13 <= hour < 16:
        return "overlap"
    if 13 <= hour < 22:
        return "ny"
    if 7 <= hour < 16:
        return "london"
    if 0 <= hour < 8:
        return "asia"
    return "other"


def state_resolution_ms(clock: str, *, tick_resolution_ms: int = 1_000) -> int:
    """Return the expected observation resolution for a state clock."""
    if clock.lower() == "tick":
        return tick_resolution_ms
    return timeframe_to_seconds(clock) * 1_000


def canonical_ticks_to_state_rows(
    canonical_df: pl.DataFrame,
    *,
    symbol: str,
    state_version_ref: str,
    provenance_resolver: Callable[[dt.date], list[str]],
) -> pl.DataFrame:
    """Convert canonical ticks into state snapshots with disagreement/staleness substrate."""
    if canonical_df.is_empty():
        return pl.DataFrame()

    working = canonical_df.sort("ts_msc")
    required = {"ts_utc", "ts_msc", "bid", "ask", "symbol"}
    missing = sorted(required - set(working.columns))
    if missing:
        raise KeyError(f"Canonical ticks are missing required columns: {missing}")

    prev_ts_msc: int | None = None
    rows: list[dict[str, object]] = []
    for row in working.iter_rows(named=True):
        ts_utc = row["ts_utc"]
        if not isinstance(ts_utc, dt.datetime):
            raise TypeError("Canonical tick ts_utc must be a timezone-aware datetime")
        bid = float(row["bid"])
        ask = float(row["ask"])
        broker_a_mid = _mid_from_pair(row.get("broker_a_bid"), row.get("broker_a_ask"))
        broker_b_mid = _mid_from_pair(row.get("broker_b_bid"), row.get("broker_b_ask"))
        broker_a_spread = _spread_from_pair(row.get("broker_a_bid"), row.get("broker_a_ask"))
        broker_b_spread = _spread_from_pair(row.get("broker_b_bid"), row.get("broker_b_ask"))
        disagreement_bps = _disagreement_bps(broker_a_mid, broker_b_mid)
        spread_disagreement_bps = _disagreement_bps(broker_a_spread, broker_b_spread)
        source_secondary = _normalize_source_secondary(row.get("source_secondary"))
        source_count = 1 + int(bool(source_secondary))
        ts_msc = int(row["ts_msc"])
        primary_staleness_ms = 0 if prev_ts_msc is None else max(ts_msc - prev_ts_msc, 0)
        prev_ts_msc = ts_msc

        snapshot = StateSnapshot(
            state_version=state_version_ref,
            snapshot_id=f"state:{symbol}:tick:{ts_utc.isoformat()}",
            symbol=symbol,
            ts_utc=ts_utc,
            ts_msc=ts_msc,
            clock="tick",
            window_start_utc=ts_utc,
            window_end_utc=ts_utc,
            bid=bid,
            ask=ask,
            mid=(bid + ask) / 2.0,
            spread=ask - bid,
            source_primary=str(row.get("source_primary") or "canonical"),
            source_secondary=source_secondary or None,
            source_count=source_count,
            merge_mode=str(row.get("merge_mode") or ("best" if source_count >= 2 else "single")),
            conflict_flag=bool(row.get("conflict_flag", False)),
            disagreement_bps=disagreement_bps,
            spread_disagreement_bps=spread_disagreement_bps,
            broker_a_mid=broker_a_mid,
            broker_b_mid=broker_b_mid,
            broker_a_spread=broker_a_spread,
            broker_b_spread=broker_b_spread,
            primary_staleness_ms=primary_staleness_ms,
            secondary_staleness_ms=None,
            source_offset_ms=None,
            quality_score=float(row.get("quality_score", 0.0) or 0.0),
            source_quality_hint=float(row.get("quality_score", 0.0) or 0.0),
            expected_observations=1,
            observed_observations=1,
            missing_observations=0,
            window_completeness=1.0,
            session_code=session_code(ts_utc),
            trust_flags=["conflict"] if bool(row.get("conflict_flag", False)) else [],
            provenance_refs=provenance_resolver(ts_utc.date()),
        )
        rows.append(snapshot.model_dump())

    return pl.DataFrame(rows).sort("ts_utc")


def build_state_windows(
    state_df: pl.DataFrame,
    *,
    symbol: str,
    clock: str,
    state_version: str,
    window_size: str,
    base_provenance_refs: list[str],
    include_partial_windows: bool = False,
) -> pl.DataFrame:
    """Build PIT-safe rolling windows with machine-native list payloads."""
    if state_df.is_empty():
        return pl.DataFrame()

    if "ts_utc" not in state_df.columns:
        raise KeyError("State DataFrame must contain ts_utc")

    window_delta = parse_window_size(window_size)
    window_ms = int(window_delta.total_seconds() * 1000)
    resolution_ms = state_resolution_ms(clock)
    expected_bins = max(1, math.ceil(window_ms / resolution_ms))
    working = state_df.sort("ts_utc")
    rows: list[dict[str, object]] = []

    source_ts = working["ts_utc"].to_list()
    if not source_ts:
        return pl.DataFrame()

    for anchor in working.iter_rows(named=True):
        anchor_ts = anchor["ts_utc"]
        if not isinstance(anchor_ts, dt.datetime):
            raise TypeError("State row ts_utc must be a timezone-aware datetime")
        lower_bound = anchor_ts - window_delta
        window_df = working.filter((pl.col("ts_utc") > lower_bound) & (pl.col("ts_utc") <= anchor_ts))
        if window_df.is_empty():
            continue

        observed_bins = _observed_bins(window_df, resolution_ms)
        missing_bins = max(expected_bins - observed_bins, 0)
        completeness = (expected_bins - missing_bins) / expected_bins
        if not include_partial_windows and completeness < 1.0:
            continue

        mids = _series_to_list(window_df, "mid", default=0.0)
        spreads = _series_to_list(window_df, "spread", default=0.0)
        source_counts = _series_to_list(window_df, "source_count", default=1)
        quality_scores = _series_to_list(window_df, "quality_score", default=0.0)
        disagreements = _series_to_optional_list(window_df, "disagreement_bps")
        staleness = _series_to_optional_list(window_df, "primary_staleness_ms")
        conflicts = [bool(v) for v in _series_to_list(window_df, "conflict_flag", default=False)]
        source_offsets = _series_to_optional_list(window_df, "source_offset_ms")
        mid_returns = _mid_return_bps(mids)

        record = StateWindowRecord(
            state_version=state_version,
            window_id=f"state-window:{symbol}:{clock}:{window_size}:{anchor_ts.isoformat()}",
            symbol=symbol,
            clock=clock,
            anchor_ts_utc=anchor_ts,
            anchor_ts_msc=int(anchor["ts_msc"]) if anchor.get("ts_msc") is not None else int(anchor_ts.timestamp() * 1000),
            window_size=window_size,
            window_start_utc=lower_bound,
            window_end_utc=anchor_ts,
            row_count=window_df.height,
            expected_row_count=expected_bins,
            missing_row_count=missing_bins,
            completeness=completeness,
            source_count_mean=float(sum(source_counts) / len(source_counts)) if source_counts else 0.0,
            dual_source_ratio_window=float(sum(1 for value in source_counts if value >= 2) / len(source_counts)) if source_counts else 0.0,
            quality_score_mean=float(sum(quality_scores) / len(quality_scores)) if quality_scores else 0.0,
            conflict_count_window=sum(1 for value in conflicts if value),
            conflict_ratio=float(sum(1 for value in conflicts if value) / len(conflicts)) if conflicts else 0.0,
            disagreement_bps_mean=_mean_optional(disagreements),
            staleness_ms_max=_max_optional(staleness),
            mid_values=mids,
            spread_values=spreads,
            mid_return_bps_values=mid_returns,
            source_count_values=source_counts,
            quality_score_values=quality_scores,
            disagreement_bps_values=disagreements,
            staleness_ms_values=staleness,
            conflict_flags=conflicts,
            source_offset_ms_values=source_offsets,
            provenance_refs=list(base_provenance_refs),
        )
        rows.append(record.model_dump())

    return pl.DataFrame(rows).sort("anchor_ts_utc") if rows else pl.DataFrame()


def _normalize_source_secondary(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value)


def _mid_from_pair(bid: object, ask: object) -> float | None:
    if bid is None or ask is None:
        return None
    bid_f = float(bid)
    ask_f = float(ask)
    if bid_f <= 0 or ask_f <= 0:
        return None
    return (bid_f + ask_f) / 2.0


def _spread_from_pair(bid: object, ask: object) -> float | None:
    if bid is None or ask is None:
        return None
    bid_f = float(bid)
    ask_f = float(ask)
    if bid_f <= 0 or ask_f <= 0:
        return None
    return max(ask_f - bid_f, 0.0)


def _disagreement_bps(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    avg = (left + right) / 2.0
    if avg <= 0:
        return None
    return abs(left - right) / avg * 10_000.0


def _series_to_list(df: pl.DataFrame, column: str, *, default: object) -> list:
    if column not in df.columns:
        return [default] * df.height
    return df[column].fill_null(default).to_list()


def _series_to_optional_list(df: pl.DataFrame, column: str) -> list:
    if column not in df.columns:
        return [None] * df.height
    return df[column].to_list()


def _observed_bins(window_df: pl.DataFrame, resolution_ms: int) -> int:
    ts_values = window_df["ts_msc"].to_list() if "ts_msc" in window_df.columns else [
        int(ts.timestamp() * 1000) for ts in window_df["ts_utc"].to_list()
    ]
    return len({ts // resolution_ms for ts in ts_values})


def _mean_optional(values: list[float | None]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return None
    return float(sum(filtered) / len(filtered))


def _max_optional(values: list[int | None]) -> int | None:
    filtered = [int(value) for value in values if value is not None]
    if not filtered:
        return None
    return max(filtered)


def _mid_return_bps(mids: list[float]) -> list[float]:
    if not mids:
        return []
    returns = [0.0]
    for prev, cur in zip(mids[:-1], mids[1:]):
        if prev <= 0:
            returns.append(0.0)
        else:
            returns.append(((cur - prev) / prev) * 10_000.0)
    return returns
