"""State engine services."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from mt5pipe.bars.builder import timeframe_to_seconds
from mt5pipe.catalog.sqlite import CatalogDB
from mt5pipe.compiler.manifest import (
    build_stage_artifact_id,
    build_stage_manifest_id,
    compute_content_hash,
    write_manifest_sidecar,
)
from mt5pipe.compiler.models import LineageManifest
from mt5pipe.quality.cleaning import validate_bars
from mt5pipe.quality.gaps import _is_forex_closed, detect_gaps, fill_bar_gaps
from mt5pipe.state.models import StateSnapshot
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths


@dataclass
class StateMaterializationResult:
    artifact_id: str
    manifest: LineageManifest
    manifest_path: Path
    state_df: pl.DataFrame
    base_df: pl.DataFrame


class StateService:
    """Materialize compiler-era state artifacts from built bars."""

    def __init__(
        self,
        paths: StoragePaths,
        store: ParquetStore,
        catalog: CatalogDB,
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
        base_df = self._load_bars_range(symbol, clock, date_from, date_to)
        if base_df.is_empty():
            raise FileNotFoundError(
                f"No built bars found for state materialization: symbol={symbol} clock={clock} "
                f"range={date_from.isoformat()}..{date_to.isoformat()}"
            )

        normalized_base = self._normalize_base_bars(base_df, clock)
        state_df = self._build_state_rows(symbol, clock, state_version_ref, normalized_base)
        input_partition_refs = self._collect_input_partition_refs(symbol, clock, date_from, date_to)

        created_at = dt.datetime.now(dt.timezone.utc)
        logical_name = f"{symbol}.{clock}"
        logical_version = state_version_ref
        content_hash = compute_content_hash({
            "artifact_kind": "state",
            "logical_name": logical_name,
            "logical_version": logical_version,
            "rows": len(state_df),
            "columns": state_df.columns,
            "time_range_start": str(state_df["ts_utc"].min()) if not state_df.is_empty() else "",
            "time_range_end": str(state_df["ts_utc"].max()) if not state_df.is_empty() else "",
            "input_partition_refs": input_partition_refs,
        })
        artifact_id = build_stage_artifact_id("state", logical_name, created_at, content_hash)
        manifest = LineageManifest(
            manifest_id=build_stage_manifest_id("state", logical_name, created_at, content_hash),
            artifact_id=artifact_id,
            artifact_kind="state",
            logical_name=logical_name,
            logical_version=logical_version,
            artifact_uri=str(
                self._paths.root
                / "state"
                / f"symbol={symbol}"
                / f"clock={clock}"
                / f"state_version={state_version_ref}"
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
            },
        )

        self._write_state_partitions(symbol, clock, state_version_ref, state_df)
        manifest_path = write_manifest_sidecar(manifest, self._paths)
        self._catalog.register_artifact(manifest, str(manifest_path))
        return StateMaterializationResult(
            artifact_id=artifact_id,
            manifest=manifest,
            manifest_path=manifest_path,
            state_df=state_df,
            base_df=normalized_base,
        )

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

    def _build_state_rows(
        self,
        symbol: str,
        clock: str,
        state_version_ref: str,
        base_df: pl.DataFrame,
    ) -> pl.DataFrame:
        tf_seconds = timeframe_to_seconds(clock)
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

            snapshot = StateSnapshot(
                state_version=state_version_ref,
                snapshot_id=f"state:{symbol}:{clock}:{ts_utc.isoformat()}",
                symbol=symbol,
                ts_utc=ts_utc,
                ts_msc=int(ts_utc.timestamp() * 1000),
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
                quality_score=quality_score,
                session_code=self._session_code(ts_utc),
                trust_flags=["filled_gap"] if bool(row.get("_filled", False)) else [],
                provenance_refs=self._provenance_refs(symbol, clock, ts_utc.date()),
            )
            rows.append(snapshot.model_dump())

        return pl.DataFrame(rows).sort("ts_utc")

    def _collect_input_partition_refs(
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

    def _provenance_refs(self, symbol: str, clock: str, day: dt.date) -> list[str]:
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

    @staticmethod
    def _session_code(ts_utc: dt.datetime) -> str:
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
