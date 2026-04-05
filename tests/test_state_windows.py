"""State-sector tests for machine-native state substrate behavior."""

from __future__ import annotations

import datetime as dt

import polars as pl

from mt5pipe.contracts.artifacts import ArtifactKind
from mt5pipe.contracts.state import StateWindowRequest, TickArtifactRef
from mt5pipe.state.public import (
    StateArtifactRef,
    StateService,
    StateSnapshot,
    StateWindowArtifactRef,
    StateWindowRecord,
    load_state_artifact,
    materialize_state_windows,
)


UTC = dt.timezone.utc


def _bars(start: dt.datetime, rows: int) -> pl.DataFrame:
    times = [start + dt.timedelta(minutes=i) for i in range(rows)]
    return pl.DataFrame(
        {
            "symbol": ["XAUUSD"] * rows,
            "timeframe": ["M1"] * rows,
            "time_utc": times,
            "open": [3000.0 + i for i in range(rows)],
            "high": [3000.4 + i for i in range(rows)],
            "low": [2999.8 + i for i in range(rows)],
            "close": [3000.2 + i for i in range(rows)],
            "tick_count": [10] * rows,
            "spread_mean": [0.2] * rows,
            "bid_close": [3000.1 + i for i in range(rows)],
            "ask_close": [3000.3 + i for i in range(rows)],
            "source_count": [2, 2, 2, 1, 2, 2][:rows],
            "conflict_count": [0, 0, 1, 0, 0, 0][:rows],
            "dual_source_ratio": [0.8, 0.8, 0.8, 0.0, 0.8, 0.8][:rows],
        }
    )


def _canonical_ticks(start: dt.datetime) -> pl.DataFrame:
    times = [start + dt.timedelta(seconds=i * 10) for i in range(4)]
    return pl.DataFrame(
        {
            "ts_utc": times,
            "ts_msc": [int(ts.timestamp() * 1000) for ts in times],
            "symbol": ["XAUUSD"] * 4,
            "bid": [3000.0, 3000.1, 3000.2, 3000.3],
            "ask": [3000.2, 3000.3, 3000.4, 3000.5],
            "last": [0.0] * 4,
            "volume": [1.0] * 4,
            "source_primary": ["broker_a"] * 4,
            "source_secondary": ["broker_b"] * 4,
            "merge_mode": ["best", "best", "conflict", "best"],
            "quality_score": [95.0, 94.0, 70.0, 96.0],
            "conflict_flag": [False, False, True, False],
            "broker_a_bid": [3000.0, 3000.1, 3000.2, 3000.3],
            "broker_a_ask": [3000.2, 3000.3, 3000.4, 3000.5],
            "broker_b_bid": [3000.05, 3000.12, 3000.18, 3000.31],
            "broker_b_ask": [3000.24, 3000.34, 3000.44, 3000.55],
        }
    )


def test_tick_state_materialization_builds_disagreement_and_staleness(paths, store) -> None:
    service = StateService(paths, store)
    start = dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC)
    ticks = _canonical_ticks(start)
    store.write(ticks, paths.canonical_ticks_file("XAUUSD", start.date()))

    result = service.materialize_tick_state(symbol="XAUUSD", date_from=start.date(), date_to=start.date())

    assert result.ref.clock == "tick"
    assert result.state_df.height == 4
    assert result.state_df["primary_staleness_ms"].to_list() == [0, 10_000, 10_000, 10_000]
    assert result.state_df["disagreement_bps"].drop_nulls().len() == 4
    assert result.state_df["conflict_flag"].to_list() == [False, False, True, False]

    loaded = load_state_artifact(paths, store, result.ref)
    assert loaded.height == 4
    assert loaded["clock"].unique().to_list() == ["tick"]


def test_state_window_materialization_is_pit_safe_and_persisted(paths, store) -> None:
    service = StateService(paths, store)
    start = dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC)
    bars = _bars(start, 6)
    store.write(bars, paths.built_bars_file("XAUUSD", "M1", start.date()))

    state_result = service.materialize_state(
        symbol="XAUUSD",
        clock="M1",
        state_version_ref="state.default@1.0.0",
        date_from=start.date(),
        date_to=start.date(),
        build_id="state.test.build",
        dataset_spec_ref="dataset.test@1.0.0",
        code_version="workspace-local-no-git",
        merge_config_ref="merge.default@test",
    )

    windows = materialize_state_windows(
        paths,
        store,
        state_result.ref,
        request=StateWindowRequest(
            symbol="XAUUSD",
            clock="M1",
            state_version="state.default@1.0.0",
            date_from=start.date(),
            date_to=start.date(),
            window_sizes=["5m"],
            include_partial_windows=False,
        ),
    )

    window_result = windows["5m"]
    assert isinstance(window_result.ref, StateWindowArtifactRef)
    assert window_result.window_df.height == 2
    assert window_result.window_df["anchor_ts_utc"].to_list()[0] == start + dt.timedelta(minutes=4)

    second_window = window_result.window_df.row(1, named=True)
    assert second_window["row_count"] == 5
    assert second_window["mid_values"] == [3001.2, 3002.2, 3003.2, 3004.2, 3005.2]
    assert second_window["conflict_count_window"] == 1

    loaded = service.load_state_window_artifact(window_result.ref)
    assert loaded.height == 2
    assert loaded["window_size"].unique().to_list() == ["5m"]


def test_state_public_boundary_exports_are_available() -> None:
    assert StateSnapshot is not None
    assert StateArtifactRef is not None
    assert StateWindowArtifactRef is not None
    assert StateWindowRecord is not None
    assert StateService is not None

    tick_ref = TickArtifactRef(
        artifact_id="canonical_tick.XAUUSD.abc",
        kind=ArtifactKind.CANONICAL_TICK,
        logical_name="XAUUSD",
        version="1.0.0",
        symbol="XAUUSD",
        date_from=dt.date(2026, 4, 1),
        date_to=dt.date(2026, 4, 1),
    )
    assert tick_ref.kind == ArtifactKind.CANONICAL_TICK
