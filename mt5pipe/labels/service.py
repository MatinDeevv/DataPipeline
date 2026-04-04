"""Label view materialization services."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from mt5pipe.catalog.sqlite import CatalogDB
from mt5pipe.compiler.manifest import (
    build_stage_artifact_id,
    build_stage_manifest_id,
    compute_content_hash,
    write_manifest_sidecar,
)
from mt5pipe.compiler.models import LineageManifest
from mt5pipe.features.labels import (
    add_direction_labels,
    add_future_returns,
    add_triple_barrier_labels,
)
from mt5pipe.labels.registry.models import LabelPack
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths


LABEL_JOIN_KEYS = ["symbol", "timeframe", "time_utc"]


@dataclass
class LabelMaterializationResult:
    artifact_id: str
    manifest: LineageManifest
    manifest_path: Path
    label_df: pl.DataFrame


class LabelService:
    """Materialize registered label packs as first-class compiler artifacts."""

    def __init__(
        self,
        paths: StoragePaths,
        store: ParquetStore,
        catalog: CatalogDB,
    ) -> None:
        self._paths = paths
        self._store = store
        self._catalog = catalog

    def materialize_labels(
        self,
        *,
        symbol: str,
        base_clock: str,
        date_from: dt.date,
        date_to: dt.date,
        label_pack: LabelPack,
        base_df: pl.DataFrame,
        state_artifact_id: str,
        build_id: str,
        dataset_spec_ref: str,
        code_version: str,
    ) -> LabelMaterializationResult:
        label_df = self._build_label_view(base_df, label_pack)
        input_partition_refs = self._collect_input_partition_refs(symbol, base_clock, date_from, date_to)
        created_at = dt.datetime.now(dt.timezone.utc)
        content_hash = compute_content_hash({
            "artifact_kind": "label_view",
            "label_pack": label_pack.model_dump(mode="json"),
            "rows": len(label_df),
            "columns": label_df.columns,
            "time_range_start": str(label_df["time_utc"].min()) if not label_df.is_empty() else "",
            "time_range_end": str(label_df["time_utc"].max()) if not label_df.is_empty() else "",
            "state_artifact_id": state_artifact_id,
            "input_partition_refs": input_partition_refs,
        })
        artifact_id = build_stage_artifact_id("label_view", label_pack.key, created_at, content_hash)
        manifest = LineageManifest(
            manifest_id=build_stage_manifest_id("label_view", label_pack.key, created_at, content_hash),
            artifact_id=artifact_id,
            artifact_kind="label_view",
            logical_name=label_pack.key,
            logical_version=label_pack.version,
            artifact_uri=str(
                self._paths.root
                / "label_views"
                / f"label_pack={label_pack.key}"
                / f"clock={label_pack.base_clock}"
            ),
            content_hash=content_hash,
            build_id=build_id,
            created_at=created_at,
            status="accepted",
            dataset_spec_ref=dataset_spec_ref,
            state_artifact_refs=[state_artifact_id],
            label_pack_ref=label_pack.key,
            code_version=code_version,
            input_partition_refs=input_partition_refs,
            parent_artifact_refs=[state_artifact_id],
            metadata={
                "row_count": len(label_df),
                "column_count": len(label_df.columns),
                "output_columns": label_pack.output_columns,
                "purge_rows": label_pack.purge_rows,
                "time_range_start": str(label_df["time_utc"].min()) if not label_df.is_empty() else "",
                "time_range_end": str(label_df["time_utc"].max()) if not label_df.is_empty() else "",
            },
        )

        self._write_label_partitions(label_pack, label_df)
        manifest_path = write_manifest_sidecar(manifest, self._paths)
        self._catalog.register_artifact(manifest, str(manifest_path))
        return LabelMaterializationResult(
            artifact_id=artifact_id,
            manifest=manifest,
            manifest_path=manifest_path,
            label_df=label_df,
        )

    def _build_label_view(self, base_df: pl.DataFrame, label_pack: LabelPack) -> pl.DataFrame:
        working = base_df.clone().sort("time_utc")
        working = add_future_returns(working, label_pack.horizons_minutes)
        working = add_direction_labels(working, label_pack.horizons_minutes)
        working = add_triple_barrier_labels(
            working,
            label_pack.horizons_minutes,
            tp_bps=float(label_pack.parameters.get("tp_bps", 50.0)),
            sl_bps=float(label_pack.parameters.get("sl_bps", 50.0)),
            vol_scale_window=int(label_pack.parameters.get("vol_scale_window", 0)),
            vol_multiplier=float(label_pack.parameters.get("vol_multiplier", 2.0)),
        )
        return working.select([*LABEL_JOIN_KEYS, *label_pack.output_columns]).sort("time_utc")

    def _collect_input_partition_refs(
        self,
        symbol: str,
        base_clock: str,
        date_from: dt.date,
        date_to: dt.date,
    ) -> list[str]:
        refs: list[str] = []
        current = date_from
        while current <= date_to:
            base_dir = self._paths.built_bars_dir(symbol, base_clock, current)
            if base_dir.exists():
                refs.append(str(base_dir))
            current += dt.timedelta(days=1)
        return sorted(set(refs))

    def _write_label_partitions(self, label_pack: LabelPack, label_df: pl.DataFrame) -> None:
        dated = label_df.with_columns(pl.col("time_utc").dt.date().alias("_date"))
        for date_val in dated["_date"].unique().sort().to_list():
            day_df = dated.filter(pl.col("_date") == date_val).drop("_date")
            self._store.write(day_df, self._paths.label_view_file(label_pack.key, label_pack.base_clock, date_val))
