"""Phase 1 dataset compiler services."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from mt5pipe.catalog.sqlite import CatalogDB
from mt5pipe.compiler.manifest import (
    build_artifact_id,
    build_id_now,
    build_manifest_id,
    code_version,
    compute_content_hash,
    load_dataset_spec,
    merge_config_ref,
    read_manifest_sidecar,
    write_manifest_sidecar,
)
from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.config.models import PipelineConfig
from mt5pipe.features.dataset import walk_forward_splits
from mt5pipe.features.registry.defaults import get_default_feature_specs, resolve_feature_selectors
from mt5pipe.features.registry.models import FeatureSpec
from mt5pipe.features.service import FeatureService
from mt5pipe.labels.registry.defaults import get_default_label_packs, resolve_label_pack
from mt5pipe.labels.registry.models import LabelPack
from mt5pipe.labels.service import LabelService
from mt5pipe.quality.cleaning import clean_dataset
from mt5pipe.state.service import StateService
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths
from mt5pipe.truth.models import TrustReport
from mt5pipe.truth.service import TruthService


MANDATORY_DATASET_COLUMNS = [
    "symbol",
    "timeframe",
    "time_utc",
    "open",
    "high",
    "low",
    "close",
    "tick_count",
    "spread_mean",
    "mid_return",
    "realized_vol",
    "source_count",
    "conflict_count",
]

DATASET_JOIN_KEYS = ["symbol", "timeframe", "time_utc"]


@dataclass
class CompileDatasetResult:
    spec: DatasetSpec
    artifact_id: str
    manifest: LineageManifest
    manifest_path: Path
    trust_report: TrustReport
    truth_report_path: Path
    split_row_counts: dict[str, int]


@dataclass
class InspectDatasetResult:
    manifest: LineageManifest
    trust_report: TrustReport | None


@dataclass
class DiffDatasetResult:
    left: InspectDatasetResult
    right: InspectDatasetResult
    diff: dict[str, object]


class DatasetCompiler:
    """Compiler-first implementation for explicit dataset artifacts."""

    def __init__(
        self,
        cfg: PipelineConfig,
        paths: StoragePaths,
        store: ParquetStore,
        catalog: CatalogDB,
    ) -> None:
        self._cfg = cfg
        self._paths = paths
        self._store = store
        self._catalog = catalog
        self._truth = TruthService()
        self._state = StateService(paths, store, catalog)
        self._features = FeatureService(paths, store, catalog, cfg.dataset)
        self._labels = LabelService(paths, store, catalog)
        self._catalog.register_feature_specs(get_default_feature_specs())
        self._catalog.register_label_packs(get_default_label_packs())

    def compile_dataset(self, spec_path: Path) -> CompileDatasetResult:
        spec = load_dataset_spec(spec_path)
        self._catalog.register_dataset_spec(spec)

        if len(spec.symbols) != 1:
            raise NotImplementedError("Phase 1 compiler supports exactly one symbol per DatasetSpec")

        symbol = spec.symbols[0]
        feature_specs = resolve_feature_selectors(spec.feature_selectors)
        label_pack = resolve_label_pack(spec.label_pack_ref)
        resolved_code_version = code_version()
        resolved_merge_config_ref = merge_config_ref(self._cfg.merge)

        build_id = build_id_now()
        self._catalog.start_build(spec.key, resolved_code_version, build_id)

        try:
            state_result = self._state.materialize_state(
                symbol=symbol,
                clock=spec.base_clock,
                state_version_ref=spec.state_version_ref,
                date_from=spec.date_from,
                date_to=spec.date_to,
                build_id=build_id,
                dataset_spec_ref=spec.key,
                code_version=resolved_code_version,
                merge_config_ref=resolved_merge_config_ref,
            )
            feature_result = self._features.materialize_features(
                symbol=symbol,
                base_clock=spec.base_clock,
                date_from=spec.date_from,
                date_to=spec.date_to,
                feature_specs=feature_specs,
                base_df=state_result.base_df,
                state_artifact_id=state_result.artifact_id,
                build_id=build_id,
                dataset_spec_ref=spec.key,
                code_version=resolved_code_version,
            )
            label_result = self._labels.materialize_labels(
                symbol=symbol,
                base_clock=spec.base_clock,
                date_from=spec.date_from,
                date_to=spec.date_to,
                label_pack=label_pack,
                base_df=state_result.base_df,
                state_artifact_id=state_result.artifact_id,
                build_id=build_id,
                dataset_spec_ref=spec.key,
                code_version=resolved_code_version,
            )

            compiled_df = self._compile_dataset_frame(
                state_result.base_df,
                feature_result.combined_df,
                label_result.label_df,
                spec,
                label_pack,
            )
            selected_df = self._select_dataset_columns(compiled_df, feature_specs, label_pack)
            split_frames = self._split_dataset(selected_df, spec)

            created_at = dt.datetime.now(dt.timezone.utc)
            content_hash = compute_content_hash({
                "dataset_spec": spec.model_dump(mode="json"),
                "columns": selected_df.columns,
                "rows": len(selected_df),
                "split_rows": {name: len(frame) for name, frame in split_frames.items()},
                "time_range_start": str(selected_df["time_utc"].min()) if not selected_df.is_empty() else "",
                "time_range_end": str(selected_df["time_utc"].max()) if not selected_df.is_empty() else "",
                "state_artifact_id": state_result.artifact_id,
                "feature_artifact_ids": [artifact.artifact_id for artifact in feature_result.artifacts],
                "label_artifact_id": label_result.artifact_id,
            })
            artifact_id = build_artifact_id(spec.dataset_name, created_at, content_hash)
            artifact_root = self._paths.root / "datasets" / f"name={spec.dataset_name}" / f"artifact={artifact_id}"

            self._write_compiled_artifact(spec, artifact_id, split_frames)

            parent_artifact_refs = [
                state_result.artifact_id,
                *[artifact.artifact_id for artifact in feature_result.artifacts],
                label_result.artifact_id,
            ]
            input_partition_refs = sorted(set(
                state_result.manifest.input_partition_refs
                + [ref for artifact in feature_result.artifacts for ref in artifact.manifest.input_partition_refs]
                + label_result.manifest.input_partition_refs
            ))
            manifest = LineageManifest(
                manifest_id=build_manifest_id(spec.dataset_name, created_at, content_hash),
                artifact_id=artifact_id,
                artifact_kind="dataset",
                logical_name=spec.dataset_name,
                logical_version=spec.version,
                artifact_uri=str(artifact_root),
                content_hash=content_hash,
                build_id=build_id,
                created_at=created_at,
                status="building",
                dataset_spec_ref=spec.key,
                state_artifact_refs=[state_result.artifact_id],
                feature_spec_refs=[feature.key for feature in feature_specs],
                label_pack_ref=label_pack.key,
                code_version=resolved_code_version,
                merge_config_ref=resolved_merge_config_ref,
                input_partition_refs=input_partition_refs,
                parent_artifact_refs=parent_artifact_refs,
                metadata={
                    "schema_columns": selected_df.columns,
                    "split_row_counts": {name: len(frame) for name, frame in split_frames.items()},
                    "time_range_start": str(selected_df["time_utc"].min()) if not selected_df.is_empty() else "",
                    "time_range_end": str(selected_df["time_utc"].max()) if not selected_df.is_empty() else "",
                    "upstream_artifacts": parent_artifact_refs,
                },
            )

            trust_report = self._truth.evaluate_dataset(
                artifact_id=artifact_id,
                dataset_df=selected_df,
                split_frames=split_frames,
                spec=spec,
                feature_specs=feature_specs,
                label_pack=label_pack,
                manifest=manifest,
                paths=self._paths,
                store=self._store,
            )

            artifact_status = "published" if trust_report.accepted_for_publication and spec.publish_on_accept else trust_report.status
            manifest = manifest.model_copy(update={
                "status": artifact_status,
                "truth_report_ref": trust_report.report_id,
            })

            truth_path = self._write_truth_report(trust_report)
            manifest_path = write_manifest_sidecar(manifest, self._paths)
            self._catalog.register_artifact(manifest, str(manifest_path))
            self._catalog.register_trust_report(trust_report)

            if artifact_status == "published":
                self._catalog.upsert_alias(f"dataset://{spec.dataset_name}@{spec.version}", artifact_id)
                self._catalog.upsert_alias(f"dataset://{spec.dataset_name}:latest", artifact_id)

            self._catalog.finish_build(build_id, artifact_status, artifact_id=artifact_id)
            return CompileDatasetResult(
                spec=spec,
                artifact_id=artifact_id,
                manifest=manifest,
                manifest_path=manifest_path,
                trust_report=trust_report,
                truth_report_path=truth_path,
                split_row_counts={name: len(frame) for name, frame in split_frames.items()},
            )
        except Exception as exc:
            self._catalog.finish_build(build_id, "failed", error_message=str(exc))
            raise

    def inspect_dataset(self, ref: str) -> InspectDatasetResult:
        manifest = self._resolve_manifest(ref)
        trust_report = self._resolve_trust_report(manifest.artifact_id)
        return InspectDatasetResult(manifest=manifest, trust_report=trust_report)

    def diff_datasets(self, left_ref: str, right_ref: str) -> DiffDatasetResult:
        left = self.inspect_dataset(left_ref)
        right = self.inspect_dataset(right_ref)
        diff = {
            "artifact_id_changed": left.manifest.artifact_id != right.manifest.artifact_id,
            "logical_name_changed": left.manifest.logical_name != right.manifest.logical_name,
            "logical_version_changed": left.manifest.logical_version != right.manifest.logical_version,
            "feature_spec_refs_added": sorted(set(right.manifest.feature_spec_refs) - set(left.manifest.feature_spec_refs)),
            "feature_spec_refs_removed": sorted(set(left.manifest.feature_spec_refs) - set(right.manifest.feature_spec_refs)),
            "label_pack_changed": left.manifest.label_pack_ref != right.manifest.label_pack_ref,
            "schema_columns_added": sorted(set(right.manifest.metadata.get("schema_columns", [])) - set(left.manifest.metadata.get("schema_columns", []))),
            "schema_columns_removed": sorted(set(left.manifest.metadata.get("schema_columns", [])) - set(right.manifest.metadata.get("schema_columns", []))),
            "split_row_counts_left": left.manifest.metadata.get("split_row_counts", {}),
            "split_row_counts_right": right.manifest.metadata.get("split_row_counts", {}),
            "state_artifact_refs_left": left.manifest.state_artifact_refs,
            "state_artifact_refs_right": right.manifest.state_artifact_refs,
            "trust_score_left": left.trust_report.trust_score_total if left.trust_report else None,
            "trust_score_right": right.trust_report.trust_score_total if right.trust_report else None,
        }
        return DiffDatasetResult(left=left, right=right, diff=diff)

    def _compile_dataset_frame(
        self,
        base_df: pl.DataFrame,
        feature_df: pl.DataFrame,
        label_df: pl.DataFrame,
        spec: DatasetSpec,
        label_pack: LabelPack,
    ) -> pl.DataFrame:
        df = base_df.sort("time_utc")

        feature_cols = [col for col in feature_df.columns if col not in DATASET_JOIN_KEYS]
        if feature_cols:
            df = df.join(feature_df.select([*DATASET_JOIN_KEYS, *feature_cols]), on=DATASET_JOIN_KEYS, how="left")

        label_cols = [col for col in label_df.columns if col not in DATASET_JOIN_KEYS]
        if label_cols:
            df = df.join(label_df.select([*DATASET_JOIN_KEYS, *label_cols]), on=DATASET_JOIN_KEYS, how="left")

        if len(df) <= label_pack.purge_rows:
            return pl.DataFrame()

        df = df.head(len(df) - label_pack.purge_rows)
        df = self._apply_dataset_filters(df, spec.filters)

        if "source_count" in df.columns:
            df = df.filter(pl.col("source_count") > 0)

        cleaned, _ = clean_dataset(df)
        return cleaned.sort("time_utc")

    def _apply_dataset_filters(self, dataset_df: pl.DataFrame, filters: list[str]) -> pl.DataFrame:
        df = dataset_df
        for raw_filter in filters:
            current = raw_filter.strip().lower()
            if not current:
                continue
            if current == "exclude:filled_rows" and "_filled" in df.columns:
                df = df.filter(~pl.col("_filled"))
            elif current == "exclude:zero_source_rows" and "source_count" in df.columns:
                df = df.filter(pl.col("source_count") > 0)
            else:
                raise ValueError(f"Unsupported DatasetSpec filter '{raw_filter}'")
        return df

    def _select_dataset_columns(self, dataset_df: pl.DataFrame, feature_specs: list[FeatureSpec], label_pack: LabelPack) -> pl.DataFrame:
        df = dataset_df
        for feature in feature_specs:
            for column in feature.output_columns:
                if column not in df.columns:
                    if feature.missingness_policy == "allow":
                        df = df.with_columns(pl.lit(None).alias(column))
                    elif feature.missingness_policy == "impute_zero":
                        df = df.with_columns(pl.lit(0.0).alias(column))

        selected_columns: list[str] = []
        for col in MANDATORY_DATASET_COLUMNS:
            if col in df.columns and col not in selected_columns:
                selected_columns.append(col)
        for feature in feature_specs:
            for col in feature.output_columns:
                if col in df.columns and col not in selected_columns:
                    selected_columns.append(col)
        for col in label_pack.output_columns:
            if col in df.columns and col not in selected_columns:
                selected_columns.append(col)

        return df.select(selected_columns)

    def _split_dataset(self, dataset_df: pl.DataFrame, spec: DatasetSpec) -> dict[str, pl.DataFrame]:
        if spec.split_policy == "temporal_holdout":
            n = len(dataset_df)
            train_end = int(n * spec.train_ratio)
            val_start = train_end + spec.embargo_rows
            val_end = int(n * (spec.train_ratio + spec.val_ratio))
            test_start = val_end + spec.embargo_rows

            return {
                "train": dataset_df.slice(0, train_end),
                "val": dataset_df.slice(val_start, max(0, val_end - val_start)) if val_start < n else pl.DataFrame(),
                "test": dataset_df.slice(test_start, max(0, n - test_start)) if test_start < n else pl.DataFrame(),
            }

        splits = walk_forward_splits(
            dataset_df,
            n_splits=spec.n_walk_forward_splits or 5,
            train_pct=spec.train_ratio,
            embargo_rows=spec.embargo_rows,
        )
        split_frames: dict[str, pl.DataFrame] = {}
        for idx, (train_df, test_df) in enumerate(splits, start=1):
            split_frames[f"train_fold_{idx}"] = train_df
            split_frames[f"test_fold_{idx}"] = test_df
        return split_frames

    def _write_compiled_artifact(self, spec: DatasetSpec, artifact_id: str, split_frames: dict[str, pl.DataFrame]) -> None:
        for split_name, split_df in split_frames.items():
            if split_df.is_empty():
                continue
            path = self._paths.compiler_dataset_file(spec.dataset_name, artifact_id, split_name)
            self._store.write(split_df, path)

    def _write_truth_report(self, report: TrustReport) -> Path:
        path = self._paths.truth_report_file(report.artifact_id, report.report_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True, default=str), encoding="utf-8")
        return path

    def _resolve_manifest(self, ref: str) -> LineageManifest:
        maybe_path = Path(ref)
        if maybe_path.exists():
            return read_manifest_sidecar(maybe_path)

        artifact = self._catalog.resolve_artifact(ref)
        if artifact is None:
            raise KeyError(f"Artifact '{ref}' was not found in the compiler catalog")
        return read_manifest_sidecar(Path(artifact.manifest_uri))

    def _resolve_trust_report(self, artifact_id: str) -> TrustReport | None:
        raw = self._catalog.get_trust_report_json(artifact_id)
        if raw is None:
            return None
        return TrustReport.model_validate_json(raw)

    @staticmethod
    def format_inspect_result(result: InspectDatasetResult) -> str:
        manifest = result.manifest
        trust = result.trust_report
        lines = [
            f"artifact_id: {manifest.artifact_id}",
            f"logical: {manifest.logical_name}@{manifest.logical_version}",
            f"status: {manifest.status}",
            f"artifact_uri: {manifest.artifact_uri}",
            f"state_artifacts: {', '.join(manifest.state_artifact_refs)}",
            f"feature_specs: {', '.join(manifest.feature_spec_refs)}",
            f"label_pack: {manifest.label_pack_ref or '-'}",
            f"split_rows: {json.dumps(manifest.metadata.get('split_row_counts', {}), sort_keys=True)}",
        ]
        if trust is not None:
            lines.append(f"trust_score_total: {trust.trust_score_total:.2f}")
            lines.append(f"trust_status: {trust.status}")
        return "\n".join(lines)

    @staticmethod
    def format_diff_result(result: DiffDatasetResult) -> str:
        lines = ["Dataset diff"]
        for key, value in result.diff.items():
            lines.append(f"{key}: {value}")
        return "\n".join(lines)
