"""Coverage and source-quality summaries for state artifacts."""

from __future__ import annotations

import math

import polars as pl

from mt5pipe.state.internal.bar_support import timeframe_to_seconds
from mt5pipe.state.models import StateCoverageSummary, StateSourceQualitySummary


def state_resolution_ms(clock: str, *, tick_resolution_ms: int = 1_000) -> int:
    """Return the expected observation resolution for a state clock."""
    if clock.lower() == "tick":
        return tick_resolution_ms
    return timeframe_to_seconds(clock) * 1_000


def coverage_mode_for_clock(clock: str) -> str:
    return "activity_clock" if clock.lower() == "tick" else "regular_clock"


def snapshot_source_participation_score(
    *,
    source_count: int,
    conflict_flag: bool,
    disagreement_bps: float | None,
    gap_fill_flag: bool,
    quality_score: float,
) -> float:
    """Normalized source participation quality for a single state snapshot."""
    base = 0.15 if source_count <= 0 else 0.55 if source_count == 1 else 1.0
    conflict_penalty = 0.35 if conflict_flag else 0.0
    disagreement_penalty = min((disagreement_bps or 0.0) / 25.0, 0.25)
    fill_penalty = 0.45 if gap_fill_flag else 0.0
    quality_modifier = max(0.0, min(quality_score / 100.0, 1.0))
    return max(0.0, min(1.0, (base - conflict_penalty - disagreement_penalty - fill_penalty) * quality_modifier))


def snapshot_overlap_confidence_hint(
    *,
    source_count: int,
    source_participation_score: float,
    window_completeness: float,
    source_quality_hint: float | None,
) -> float:
    """Normalized overlap-confidence hint for a single state snapshot."""
    dual_bonus = 1.0 if source_count >= 2 else 0.55 if source_count == 1 else 0.20
    quality_modifier = max(0.0, min((source_quality_hint if source_quality_hint is not None else 50.0) / 100.0, 1.0))
    confidence = dual_bonus * source_participation_score * max(window_completeness, 0.0) * quality_modifier
    return max(0.0, min(1.0, confidence))


def build_state_coverage_summary(state_df: pl.DataFrame, *, clock: str) -> StateCoverageSummary:
    """Summarize state coverage and gap behavior for a state frame."""
    resolution_ms = state_resolution_ms(clock)
    coverage_mode = coverage_mode_for_clock(clock)
    if state_df.is_empty():
        return StateCoverageSummary(
            coverage_mode=coverage_mode,
            resolution_ms=resolution_ms,
            row_count=0,
            expected_rows=0,
            missing_rows=0,
            completeness_ratio=0.0,
            filled_row_count=0,
            filled_ratio=0.0,
            gap_count=0,
            max_gap_ms=0,
            observed_span_ms=0,
        )

    ts_values = _timestamp_values_ms(state_df)
    time_values = _time_values(state_df)
    observed_span_ms = max(ts_values[-1] - ts_values[0], 0) if len(ts_values) > 1 else 0
    filled_flags = _filled_flags(state_df)
    filled_row_count = _filled_row_count(state_df, filled_flags)
    filled_ratio = _filled_ratio(state_df, filled_row_count)

    if coverage_mode == "regular_clock":
        expected_rows = max(1, math.floor(observed_span_ms / resolution_ms) + 1)
        missing_rows = max(expected_rows - state_df.height, 0)
        gap_count = _gap_count(state_df, filled_flags)
        max_gap_ms = _max_gap_ms(state_df, filled_flags, resolution_ms)
    else:
        expected_rows = state_df.height
        missing_rows = 0
        staleness_values = _staleness_values(state_df)
        gap_events = [value for value in staleness_values if value > resolution_ms]
        gap_count = len(gap_events)
        max_gap_ms = max(gap_events, default=0)

    completeness_ratio = ((expected_rows - missing_rows) / expected_rows) if expected_rows > 0 else 0.0
    return StateCoverageSummary(
        coverage_mode=coverage_mode,
        resolution_ms=resolution_ms,
        row_count=state_df.height,
        expected_rows=expected_rows,
        missing_rows=missing_rows,
        completeness_ratio=completeness_ratio,
        filled_row_count=filled_row_count,
        filled_ratio=filled_ratio,
        gap_count=gap_count,
        max_gap_ms=max_gap_ms,
        observed_span_ms=observed_span_ms,
        time_range_start_utc=time_values[0],
        time_range_end_utc=time_values[-1],
    )


def build_state_source_quality_summary(state_df: pl.DataFrame) -> StateSourceQualitySummary:
    """Summarize source participation and quality hints for a state frame."""
    if state_df.is_empty():
        return StateSourceQualitySummary(
            mean_source_count=0.0,
            dual_source_ratio=0.0,
            conflict_ratio=0.0,
            mean_quality_score=0.0,
            min_quality_score=0.0,
            mean_source_quality_hint=0.0,
            mean_source_participation_score=0.0,
            mean_overlap_confidence=0.0,
            median_primary_staleness_ms=0.0,
            p95_primary_staleness_ms=0.0,
            max_primary_staleness_ms=0,
        )

    source_counts = _source_count_values(state_df)
    quality_scores = _quality_score_values(state_df)
    quality_hints = _source_quality_hint_values(state_df)
    participation_scores = _source_participation_values(state_df)
    overlap_confidences = _overlap_confidence_values(state_df)
    conflicts = _conflict_flags(state_df)
    staleness_values = _staleness_values(state_df)

    return StateSourceQualitySummary(
        mean_source_count=_mean(source_counts),
        dual_source_ratio=_dual_source_ratio(state_df, source_counts),
        conflict_ratio=_ratio(sum(1 for value in conflicts if value), len(conflicts)),
        mean_quality_score=_mean(quality_scores),
        min_quality_score=min(quality_scores) if quality_scores else 0.0,
        mean_source_quality_hint=_mean_optional(quality_hints),
        mean_source_participation_score=_mean_optional(participation_scores),
        mean_overlap_confidence=_mean_optional(overlap_confidences),
        median_primary_staleness_ms=_percentile(staleness_values, 0.5),
        p95_primary_staleness_ms=_percentile(staleness_values, 0.95),
        max_primary_staleness_ms=max(staleness_values, default=0),
    )


def _filled_flags(state_df: pl.DataFrame) -> list[bool]:
    if "filled_row_count" in state_df.columns:
        return [int(value or 0) > 0 for value in state_df["filled_row_count"].fill_null(0).to_list()]
    if "gap_fill_flag" in state_df.columns:
        return [bool(value) for value in state_df["gap_fill_flag"].to_list()]
    if "trust_flags" not in state_df.columns:
        return [False] * state_df.height
    flags = state_df["trust_flags"].to_list()
    return ["filled_gap" in (value or []) for value in flags]


def _staleness_values(state_df: pl.DataFrame) -> list[int]:
    if "observed_interval_ms" in state_df.columns:
        return [int(value) for value in state_df["observed_interval_ms"].fill_null(0).to_list()]
    if "primary_staleness_ms" in state_df.columns:
        return [int(value) for value in state_df["primary_staleness_ms"].fill_null(0).to_list()]
    if "staleness_ms_max" in state_df.columns:
        return [int(value) for value in state_df["staleness_ms_max"].fill_null(0).to_list()]
    return [0] * state_df.height


def _timestamp_values_ms(state_df: pl.DataFrame) -> list[int]:
    if "ts_msc" in state_df.columns:
        return [int(value) for value in state_df["ts_msc"].to_list()]
    if "anchor_ts_msc" in state_df.columns:
        return [int(value) for value in state_df["anchor_ts_msc"].to_list()]
    raise KeyError("State frame must contain ts_msc or anchor_ts_msc")


def _time_values(state_df: pl.DataFrame) -> list[object]:
    if "ts_utc" in state_df.columns:
        return state_df["ts_utc"].to_list()
    if "anchor_ts_utc" in state_df.columns:
        return state_df["anchor_ts_utc"].to_list()
    raise KeyError("State frame must contain ts_utc or anchor_ts_utc")


def _filled_row_count(state_df: pl.DataFrame, filled_flags: list[bool]) -> int:
    if "filled_row_count" in state_df.columns:
        return sum(int(value or 0) for value in state_df["filled_row_count"].fill_null(0).to_list())
    return sum(1 for flag in filled_flags if flag)


def _filled_ratio(state_df: pl.DataFrame, filled_row_count: int) -> float:
    if "row_count" in state_df.columns:
        total_rows = sum(int(value or 0) for value in state_df["row_count"].fill_null(0).to_list())
        return float(filled_row_count / total_rows) if total_rows > 0 else 0.0
    return float(filled_row_count / state_df.height) if state_df.height > 0 else 0.0


def _gap_count(state_df: pl.DataFrame, filled_flags: list[bool]) -> int:
    if "gap_count" in state_df.columns:
        return sum(int(value or 0) for value in state_df["gap_count"].fill_null(0).to_list())
    return _run_count(filled_flags)


def _max_gap_ms(state_df: pl.DataFrame, filled_flags: list[bool], resolution_ms: int) -> int:
    if "max_gap_ms" in state_df.columns:
        return max((int(value or 0) for value in state_df["max_gap_ms"].fill_null(0).to_list()), default=0)
    return _max_gap_run_ms(filled_flags, resolution_ms)


def _source_count_values(df: pl.DataFrame) -> list[float]:
    if "source_count" in df.columns:
        return _numeric_list(df, "source_count")
    if "source_count_mean" in df.columns:
        return _numeric_list(df, "source_count_mean")
    return []


def _quality_score_values(df: pl.DataFrame) -> list[float]:
    if "quality_score" in df.columns:
        return _numeric_list(df, "quality_score")
    if "quality_score_mean" in df.columns:
        return _numeric_list(df, "quality_score_mean")
    return []


def _source_quality_hint_values(df: pl.DataFrame) -> list[float | None]:
    if "source_quality_hint" in df.columns:
        return _optional_numeric_list(df, "source_quality_hint")
    if "source_quality_hint_mean" in df.columns:
        return _optional_numeric_list(df, "source_quality_hint_mean")
    return []


def _source_participation_values(df: pl.DataFrame) -> list[float | None]:
    if "source_participation_score" in df.columns:
        return _optional_numeric_list(df, "source_participation_score")
    if "source_participation_score_mean" in df.columns:
        return _optional_numeric_list(df, "source_participation_score_mean")
    return []


def _overlap_confidence_values(df: pl.DataFrame) -> list[float | None]:
    if "overlap_confidence_hint" in df.columns:
        return _optional_numeric_list(df, "overlap_confidence_hint")
    if "overlap_confidence_mean" in df.columns:
        return _optional_numeric_list(df, "overlap_confidence_mean")
    return []


def _conflict_flags(df: pl.DataFrame) -> list[bool]:
    if "conflict_flag" in df.columns:
        return _bool_list(df, "conflict_flag")
    if "conflict_count_window" in df.columns:
        return [int(value or 0) > 0 for value in df["conflict_count_window"].fill_null(0).to_list()]
    return [False] * df.height


def _dual_source_ratio(df: pl.DataFrame, source_counts: list[float]) -> float:
    if "dual_source_ratio_window" in df.columns:
        ratios = _numeric_list(df, "dual_source_ratio_window")
        return _mean(ratios)
    return _ratio(sum(1 for value in source_counts if value >= 2), len(source_counts))


def _run_count(flags: list[bool]) -> int:
    count = 0
    in_run = False
    for flag in flags:
        if flag and not in_run:
            count += 1
            in_run = True
        elif not flag:
            in_run = False
    return count


def _max_gap_run_ms(flags: list[bool], resolution_ms: int) -> int:
    best = 0
    current = 0
    for flag in flags:
        if flag:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best * resolution_ms


def _numeric_list(df: pl.DataFrame, column: str) -> list[float]:
    if column not in df.columns:
        return []
    return [float(value) for value in df[column].fill_null(0).to_list()]


def _optional_numeric_list(df: pl.DataFrame, column: str) -> list[float | None]:
    if column not in df.columns:
        return []
    result: list[float | None] = []
    for value in df[column].to_list():
        result.append(None if value is None else float(value))
    return result


def _bool_list(df: pl.DataFrame, column: str) -> list[bool]:
    if column not in df.columns:
        return [False] * df.height
    return [bool(value) for value in df[column].fill_null(False).to_list()]


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _mean_optional(values: list[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return float(sum(filtered) / len(filtered))


def _ratio(true_count: int | object, total: int) -> float:
    count = int(true_count) if not isinstance(true_count, int) else true_count
    if total <= 0:
        return 0.0
    return float(count / total)


def _percentile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    series = pl.Series("values", values)
    quantile = series.quantile(q)
    return float(quantile) if quantile is not None else 0.0
