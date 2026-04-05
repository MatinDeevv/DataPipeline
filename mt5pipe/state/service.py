"""State services and public-facing loaders for state artifacts."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from mt5pipe.bars.builder import timeframe_to_seconds
from mt5pipe.contracts.artifacts import ArtifactKind
from mt5pipe.contracts.state import (
    StateArtifactRef,
    StateWindowArtifactRef,
    StateWindowRequest,
    TickArtifactRef,
)
from mt5pipe.quality.cleaning import validate_bars
from mt5pipe.quality.gaps import detect_gaps, fill_bar_gaps
from mt5pipe.state.internal.artifacts import (
    build_artifact_id,
    build_id_now,
    build_manifest_id,
    compute_content_hash,
    state_code_version,
    write_state_manifest,
)
from mt5pipe.state.internal.windows import build_state_windows, canonical_ticks_to_state_rows, session_code
from mt5pipe.state.models import StateArtifactManifest, StateSnapshot
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths


@dataclass
class StateMaterializationResult:
    ref: StateArtifactRef
    artifact_id: str
    manifest: StateArtifactManifest
    manifest_path: Path
    state_df: pl.DataFrame
    base_df: pl.DataFrame


@dataclass
class StateWindowMaterializationResult:
    ref: StateWindowArtifactRef
    manifest: StateArtifactManifest
    manifest_path: Path
    window_df: pl.DataFrame


class StateService:
    """Materialize and load state-side artifacts without compiler-sector imports."""

    def __init__(
        self,
        paths: StoragePaths,
        store: ParquetStore,
        catalog: Any | None = None,
    ) -> None:
        self._paths = paths
        self._store = store
        self._catalog = catalog

    def materialize_state(
        self,
        *,
        symbol: str,
        clock: str,
        state_version_ref: str,
        date_from: dt.date,
        date_to: dt.date,
        build_id: str,
        dataset_spec_ref: str,
        code_version: str,
        merge_config_ref: str,
    ) -> StateMaterializationResult:
        """Materialize bar-backed state snapshots. Kept compatible for compiler usage."""
        base_df = self._load_bars_range(symbol, clock, date_from, date_to)
        if base_df.is_empty():
            raise FileNotFoundError(
                f"No built bars found for state materialization: symbol={symbol} clock={clock} "
                f"range={date_from.isoformat()}..{date_to.isoformat()}"
            )

        normalized_base = self._normalize_base_bars(base_df, clock)
        state_df = self._build_bar_state_rows(symbol, clock, state_version_ref, normalized_base)
        input_partition_refs = self._collect_bar_input_partition_refs(symbol, clock, date_from, date_to)
        return self._persist_state_artifact(
            symbol=symbol,
            clock=clock,
            state_version_ref=state_version_ref,
            date_from=date_from,
            date_to=date_to,
            build_id=build_id,
            dataset_spec_ref=dataset_spec_ref,
            code_version=code_version,
            merge_config_ref=merge_config_ref,
            state_df=state_df,
            base_df=normalized_base,
            input_partition_refs=input_partition_refs,
        )

    def materialize_tick_state(
        self,
        *,
        symbol: str,
        date_from: dt.date,
        date_to: dt.date,
        state_version_ref: str = "state.tick@1.0.0",
        build_id: str | None = None,
        dataset_spec_ref: str | None = None,
        code_version: str | None = None,
        merge_config_ref: str | None = None,
    ) -> StateMaterializationResult:
        """Materialize tick-level state snapshots from canonical ticks."""
        canonical_df = self.load_tick_artifact(
            TickArtifactRef(
                artifact_id=f"canonical_tick.{symbol}.{date_from.isoformat()}.{date_to.isoformat()}",
                kind=ArtifactKind.CANONICAL_TICK,
                logical_name=symbol,
                version="1.0.0",
                symbol=symbol,
                date_from=date_from,
                date_to=date_to,
            )
        )
        if canonical_df.is_empty():
            raise FileNotFoundError(
                f"No canonical ticks found for tick-state materialization: symbol={symbol} "
                f"range={date_from.isoformat()}..{date_to.isoformat()}"
            )

        state_df = canonical_ticks_to_state_rows(
            canonical_df,
            symbol=symbol,
            state_version_ref=state_version_ref,
            provenance_resolver=lambda day: [str(self._paths.canonical_ticks_dir(symbol, day))],
        )
        input_partition_refs = self._collect_tick_input_partition_refs(symbol, date_from, date_to)
        return self._persist_state_artifact(
            symbol=symbol,
            clock="tick",
            state_version_ref=state_version_ref,
            date_from=date_from,
            date_to=date_to,
            build_id=build_id or build_id_now("state"),
            dataset_spec_ref=dataset_spec_ref,
            code_version=code_version or state_code_version(),
            merge_config_ref=merge_config_ref,
            state_df=state_df,
            base_df=canonical_df.sort("ts_utc"),
            input_partition_refs=input_partition_refs,
        )

    def load_tick_artifact(self, ref: TickArtifactRef) -> pl.DataFrame:
        """Load canonical ticks for a typed artifact reference."""
        frames: list[pl.DataFrame] = []
        current = ref.date_from
        while current <= ref.date_to:
            day = self._store.read_dir(self._paths.canonical_ticks_dir(ref.symbol, current))
            if not day.is_empty():
                frames.append(day)
            current += dt.timedelta(days=1)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed").sort("ts_msc")

    def load_state_artifact(self, ref: StateArtifactRef) -> pl.DataFrame:
        """Load persisted state snapshots for a typed artifact reference."""
        frames: list[pl.DataFrame] = []
        current = ref.date_from
        while current <= ref.date_to:
            day = self._store.read_dir(self._paths.state_dir(ref.symbol, ref.clock, current, ref.state_version))
            if not day.is_empty():
                frames.append(day)
            current += dt.timedelta(days=1)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed").sort("ts_utc")

    def materialize_state_windows(
        self,
        source_ref: StateArtifactRef | TickArtifactRef,
        *,
        request: StateWindowRequest,
        build_id: str | None = None,
        code_version: str | None = None,
    ) -> dict[str, StateWindowMaterializationResult]:
        """Materialize PIT-safe rolling state windows from a state or canonical tick source."""
        if isinstance(source_ref, TickArtifactRef):
            if request.anchor_on != "canonical_tick":
                raise ValueError("TickArtifactRef requires request.anchor_on='canonical_tick'")
            if request.symbol != source_ref.symbol:
                raise ValueError("StateWindowRequest.symbol must match the TickArtifactRef symbol")
            canonical_df = self.load_tick_artifact(source_ref)
            if canonical_df.is_empty():
                raise FileNotFoundError(f"No canonical ticks found for {source_ref}")
            state_df = canonical_ticks_to_state_rows(
                canonical_df,
                symbol=request.symbol,
                state_version_ref=request.state_version,
                provenance_resolver=lambda day: [str(self._paths.canonical_ticks_dir(request.symbol, day))],
            )
            source_artifact_id = source_ref.artifact_id
            clock = "tick"
            input_partition_refs = self._collect_tick_input_partition_refs(request.symbol, request.date_from, request.date_to)
        else:
            if request.anchor_on != "state":
                raise ValueError("StateArtifactRef requires request.anchor_on='state'")
            if request.symbol != source_ref.symbol or request.clock != source_ref.clock or request.state_version != source_ref.state_version:
                raise ValueError("StateWindowRequest must match the source StateArtifactRef symbol/clock/state_version")
            state_df = self.load_state_artifact(source_ref)
            if state_df.is_empty():
                raise FileNotFoundError(f"No state snapshots found for {source_ref}")
            source_artifact_id = source_ref.artifact_id
            clock = source_ref.clock
            input_partition_refs = self._collect_state_input_partition_refs(
                request.symbol,
                source_ref.clock,
                request.state_version,
                request.date_from,
                request.date_to,
            )

        results: dict[str, StateWindowMaterializationResult] = {}
        resolved_build_id = build_id or build_id_now("state-window")
        resolved_code_version = code_version or state_code_version()
        base_provenance = list(input_partition_refs)

        for window_size in request.window_sizes:
            window_df = build_state_windows(
                state_df,
                symbol=request.symbol,
                clock=clock,
                state_version=request.state_version,
                window_size=window_size,
                base_provenance_refs=base_provenance,
                include_partial_windows=request.include_partial_windows,
            )
            logical_name = f"{request.symbol}.{clock}.{window_size}"
            content_hash = compute_content_hash(
                {
                    "artifact_kind": "state_window",
                    "logical_name": logical_name,
                    "state_version": request.state_version,
                    "window_size": window_size,
                    "rows": len(window_df),
                    "columns": window_df.columns,
                    "anchor_start": str(window_df["anchor_ts_utc"].min()) if not window_df.is_empty() else "",
                    "anchor_end": str(window_df["anchor_ts_utc"].max()) if not window_df.is_empty() else "",
                    "source_artifact_id": source_artifact_id,
                    "input_partition_refs": input_partition_refs,
                }
            )
            artifact_id = build_artifact_id("state_window", logical_name, content_hash)
            ref = StateWindowArtifactRef(
                artifact_id=artifact_id,
                logical_name=logical_name,
                version=request.state_version,
                content_hash=content_hash,
                symbol=request.symbol,
                clock=clock,
                state_version=request.state_version,
                window_size=window_size,
                date_from=request.date_from,
                date_to=request.date_to,
                source_artifact_id=source_artifact_id,
            )
            manifest = StateArtifactManifest(
                manifest_id=build_manifest_id("state_window", logical_name, content_hash),
                artifact_id=artifact_id,
                artifact_kind="state_window",
                logical_name=logical_name,
                logical_version=request.state_version,
                artifact_uri=str(
                    self._paths.state_window_root(request.symbol, clock, request.state_version, window_size)
                ),
                content_hash=content_hash,
                build_id=resolved_build_id,
                created_at=dt.datetime.now(dt.timezone.utc),
                status="accepted",
                dataset_spec_ref=None,
                state_artifact_refs=[source_artifact_id] if isinstance(source_ref, StateArtifactRef) else [],
                code_version=resolved_code_version,
                input_partition_refs=input_partition_refs,
                parent_artifact_refs=[source_artifact_id],
                metadata={
                    "row_count": len(window_df),
                    "column_count": len(window_df.columns),
                    "window_size": window_size,
                    "anchor_start": str(window_df["anchor_ts_utc"].min()) if not window_df.is_empty() else "",
                    "anchor_end": str(window_df["anchor_ts_utc"].max()) if not window_df.is_empty() else "",
                    "source_artifact_id": source_artifact_id,
                },
            )
            self._write_state_window_partitions(request.symbol, clock, request.state_version, window_size, window_df)
            manifest_path = write_state_manifest(manifest, self._paths)
            self._register_manifest(manifest, manifest_path)
            results[window_size] = StateWindowMaterializationResult(
                ref=ref,
                manifest=manifest,
                manifest_path=manifest_path,
                window_df=window_df,
            )

        return results

    def load_state_window_artifact(self, ref: StateWindowArtifactRef) -> pl.DataFrame:
        """Load persisted rolling state windows for a typed artifact reference."""
        frames: list[pl.DataFrame] = []
        current = ref.date_from
        while current <= ref.date_to:
            day = self._store.read_dir(
                self._paths.state_window_dir(ref.symbol, ref.clock, current, ref.state_version, ref.window_size)
            )
            if not day.is_empty():
                frames.append(day)
            current += dt.timedelta(days=1)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed").sort("anchor_ts_utc")

    def _persist_state_artifact(
        self,
        *,
        symbol: str,
        clock: str,
        state_version_ref: str,
        date_from: dt.date,
        date_to: dt.date,
        build_id: str,
        dataset_spec_ref: str | None,
        code_version: str,
        merge_config_ref: str | None,
        state_df: pl.DataFrame,
        base_df: pl.DataFrame,
        input_partition_refs: list[str],
    ) -> StateMaterializationResult:
        created_at = dt.datetime.now(dt.timezone.utc)
        logical_name = f"{symbol}.{clock}"
        content_hash = compute_content_hash(
            {
                "artifact_kind": "state",
                "logical_name": logical_name,
                "logical_version": state_version_ref,
                "rows": len(state_df),
                "columns": state_df.columns,
                "time_range_start": str(state_df["ts_utc"].min()) if not state_df.is_empty() else "",
                "time_range_end": str(state_df["ts_utc"].max()) if not state_df.is_empty() else "",
                "input_partition_refs": input_partition_refs,
            }
        )
        artifact_id = build_artifact_id("state", logical_name, content_hash)
        ref = StateArtifactRef(
            artifact_id=artifact_id,
            logical_name=logical_name,
            version=state_version_ref,
            content_hash=content_hash,
            symbol=symbol,
            clock=clock,
            state_version=state_version_ref,
            date_from=date_from,
            date_to=date_to,
        )
        manifest = StateArtifactManifest(
            manifest_id=build_manifest_id("state", logical_name, content_hash),
            artifact_id=artifact_id,
            artifact_kind="state",
            logical_name=logical_name,
            logical_version=state_version_ref,
            artifact_uri=str(
                self._paths.state_root(symbol, clock, state_version_ref)
            ),
            content_hash=content_hash,
            build_id=build_id,
            created_at=created_at,
            status="accepted",
            dataset_spec_ref=dataset_spec_ref,
            code_version=code_version,
            merge_config_ref=merge_config_ref,
            input_partition_refs=input_partition_refs,
            metadata={
                "row_count": len(state_df),
                "column_count": len(state_df.columns),
                "time_range_start": str(state_df["ts_utc"].min()) if not state_df.is_empty() else "",
                "time_range_end": str(state_df["ts_utc"].max()) if not state_df.is_empty() else "",
                "clock": clock,
            },
        )

        self._write_state_partitions(symbol, clock, state_version_ref, state_df)
        manifest_path = write_state_manifest(manifest, self._paths)
        self._register_manifest(manifest, manifest_path)
        return StateMaterializationResult(
            ref=ref,
            artifact_id=artifact_id,
            manifest=manifest,
            manifest_path=manifest_path,
            state_df=state_df,
            base_df=base_df,
        )

    def _register_manifest(self, manifest: StateArtifactManifest, manifest_path: Path) -> None:
        if self._catalog is not None and hasattr(self._catalog, "register_artifact"):
            self._catalog.register_artifact(manifest, str(manifest_path))

    def _load_bars_range(
        self,
        symbol: str,
        clock: str,
        date_from: dt.date,
        date_to: dt.date,
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        current = date_from
        while current <= date_to:
            day = self._store.read_dir(self._paths.built_bars_dir(symbol, clock, current))
            if not day.is_empty():
                frames.append(day)
            current += dt.timedelta(days=1)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed").sort("time_utc")

    def _normalize_base_bars(self, base_df: pl.DataFrame, clock: str) -> pl.DataFrame:
        normalized = validate_bars(base_df)
        tf_seconds = timeframe_to_seconds(clock)
        gap_report = detect_gaps(normalized, clock, tf_seconds)
        if gap_report.missing_bars > 0:
            normalized = fill_bar_gaps(normalized, clock, tf_seconds)
        elif "_filled" not in normalized.columns:
            normalized = normalized.with_columns(pl.lit(False).alias("_filled"))
        return normalized.sort("time_utc")

    def _build_bar_state_rows(
        self,
        symbol: str,
        clock: str,
        state_version_ref: str,
        base_df: pl.DataFrame,
    ) -> pl.DataFrame:
        tf_seconds = timeframe_to_seconds(clock)
        prev_ts_msc: int | None = None
        rows: list[dict[str, object]] = []

        for row in base_df.iter_rows(named=True):
            ts_utc = row["time_utc"]
            if not isinstance(ts_utc, dt.datetime):
                raise TypeError("Base bar time_utc must be a timezone-aware datetime")

            spread = self._resolve_spread(row)
            bid = self._resolve_bid(row, spread)
            ask = self._resolve_ask(row, spread, bid)
            source_count = int(row.get("source_count", 1) or 1)
            conflict_count = int(row.get("conflict_count", 0) or 0)
            dual_ratio = row.get("dual_source_ratio")
            dual_ratio_f = float(dual_ratio) if dual_ratio is not None else None
            merge_mode = "conflict" if conflict_count > 0 else "best" if source_count >= 2 else "single"
            quality_score = self._quality_score(
                source_count=source_count,
                conflict_count=conflict_count,
                dual_source_ratio=dual_ratio_f,
                filled=bool(row.get("_filled", False)),
            )
            ts_msc = int(ts_utc.timestamp() * 1000)
            primary_staleness_ms = 0 if prev_ts_msc is None else max(ts_msc - prev_ts_msc, 0)
            prev_ts_msc = ts_msc

            snapshot = StateSnapshot(
                state_version=state_version_ref,
                snapshot_id=f"state:{symbol}:{clock}:{ts_utc.isoformat()}",
                symbol=symbol,
                ts_utc=ts_utc,
                ts_msc=ts_msc,
                clock=clock,
                window_start_utc=ts_utc,
                window_end_utc=ts_utc + dt.timedelta(seconds=tf_seconds) - dt.timedelta(milliseconds=1),
                bid=bid,
                ask=ask,
                mid=(bid + ask) / 2.0,
                spread=ask - bid,
                source_primary="canonical",
                source_secondary="multi" if source_count >= 2 else None,
                source_count=source_count,
                merge_mode=merge_mode,
                conflict_flag=conflict_count > 0,
                disagreement_bps=None,
                spread_disagreement_bps=None,
                broker_a_mid=None,
                broker_b_mid=None,
                broker_a_spread=None,
                broker_b_spread=None,
                primary_staleness_ms=primary_staleness_ms,
                secondary_staleness_ms=None,
                source_offset_ms=None,
                quality_score=quality_score,
                source_quality_hint=quality_score,
                expected_observations=1,
                observed_observations=1,
                missing_observations=0,
                window_completeness=1.0,
                session_code=session_code(ts_utc),
                trust_flags=["filled_gap"] if bool(row.get("_filled", False)) else [],
                provenance_refs=self._bar_provenance_refs(symbol, clock, ts_utc.date()),
            )
            rows.append(snapshot.model_dump())

        return pl.DataFrame(rows).sort("ts_utc")

    def _collect_bar_input_partition_refs(
        self,
        symbol: str,
        clock: str,
        date_from: dt.date,
        date_to: dt.date,
    ) -> list[str]:
        refs: list[str] = []
        current = date_from
        while current <= date_to:
            bar_dir = self._paths.built_bars_dir(symbol, clock, current)
            if bar_dir.exists():
                refs.append(str(bar_dir))
            merge_qa_dir = self._paths.merge_qa_dir(symbol, current)
            if merge_qa_dir.exists():
                refs.append(str(merge_qa_dir))
            current += dt.timedelta(days=1)
        return sorted(set(refs))

    def _collect_tick_input_partition_refs(
        self,
        symbol: str,
        date_from: dt.date,
        date_to: dt.date,
    ) -> list[str]:
        refs: list[str] = []
        current = date_from
        while current <= date_to:
            tick_dir = self._paths.canonical_ticks_dir(symbol, current)
            if tick_dir.exists():
                refs.append(str(tick_dir))
            current += dt.timedelta(days=1)
        return sorted(set(refs))

    def _collect_state_input_partition_refs(
        self,
        symbol: str,
        clock: str,
        state_version: str,
        date_from: dt.date,
        date_to: dt.date,
    ) -> list[str]:
        refs: list[str] = []
        current = date_from
        while current <= date_to:
            state_dir = self._paths.state_dir(symbol, clock, current, state_version)
            if state_dir.exists():
                refs.append(str(state_dir))
            current += dt.timedelta(days=1)
        return sorted(set(refs))

    def _bar_provenance_refs(self, symbol: str, clock: str, day: dt.date) -> list[str]:
        refs = [str(self._paths.built_bars_dir(symbol, clock, day))]
        merge_qa_dir = self._paths.merge_qa_dir(symbol, day)
        if merge_qa_dir.exists():
            refs.append(str(merge_qa_dir))
        return refs

    def _write_state_partitions(self, symbol: str, clock: str, state_version_ref: str, state_df: pl.DataFrame) -> None:
        dated = state_df.with_columns(pl.col("ts_utc").dt.date().alias("_date"))
        for date_val in dated["_date"].unique().sort().to_list():
            day_df = dated.filter(pl.col("_date") == date_val).drop("_date")
            self._store.write(day_df, self._paths.state_file(symbol, clock, date_val, state_version_ref))

    def _write_state_window_partitions(
        self,
        symbol: str,
        clock: str,
        state_version: str,
        window_size: str,
        window_df: pl.DataFrame,
    ) -> None:
        if window_df.is_empty():
            return
        dated = window_df.with_columns(pl.col("anchor_ts_utc").dt.date().alias("_date"))
        for date_val in dated["_date"].unique().sort().to_list():
            day_df = dated.filter(pl.col("_date") == date_val).drop("_date")
            self._store.write(
                day_df,
                self._paths.state_window_file(symbol, clock, date_val, state_version, window_size),
            )

    @staticmethod
    def _resolve_spread(row: dict[str, object]) -> float:
        spread = float(row.get("spread_mean", 0.0) or 0.0)
        if spread > 0:
            return spread
        bid = row.get("bid_close")
        ask = row.get("ask_close")
        if bid is not None and ask is not None:
            return max(float(ask) - float(bid), 1e-9)
        return 1e-6

    @staticmethod
    def _resolve_bid(row: dict[str, object], spread: float) -> float:
        bid = row.get("bid_close")
        if bid is not None:
            return float(bid)
        close = float(row.get("close", 0.0) or 0.0)
        return max(close - (spread / 2.0), 1e-9)

    @staticmethod
    def _resolve_ask(row: dict[str, object], spread: float, bid: float) -> float:
        ask = row.get("ask_close")
        if ask is not None:
            return float(ask)
        close = float(row.get("close", 0.0) or 0.0)
        return max(close + (spread / 2.0), bid)

    @staticmethod
    def _quality_score(
        *,
        source_count: int,
        conflict_count: int,
        dual_source_ratio: float | None,
        filled: bool,
    ) -> float:
        dual_component = min(max(dual_source_ratio or 0.0, 0.0) * 25.0, 25.0)
        source_component = 10.0 if source_count >= 2 else 0.0
        conflict_penalty = 20.0 if conflict_count > 0 else 0.0
        filled_penalty = 30.0 if filled else 0.0
        return max(0.0, min(100.0, 65.0 + dual_component + source_component - conflict_penalty - filled_penalty))


def load_state_artifact(
    paths: StoragePaths,
    store: ParquetStore,
    ref: StateArtifactRef,
) -> pl.DataFrame:
    """Module-level public loader for persisted state snapshots."""
    return StateService(paths, store).load_state_artifact(ref)


def materialize_state_windows(
    paths: StoragePaths,
    store: ParquetStore,
    source_ref: StateArtifactRef | TickArtifactRef,
    *,
    request: StateWindowRequest,
    catalog: Any | None = None,
) -> dict[str, StateWindowMaterializationResult]:
    """Module-level public helper for rolling state window materialization."""
    return StateService(paths, store, catalog=catalog).materialize_state_windows(source_ref, request=request)
