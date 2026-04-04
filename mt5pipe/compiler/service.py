"""Core compiler services for build, inspection, and artifact diffing."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from mt5pipe.catalog.models import AliasRecord, ArtifactInputRecord, ArtifactRecord, ArtifactStatusEventRecord
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
from mt5pipe.config.loader import load_config
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
DEFAULT_CONFIG_RELATIVE_PATH = Path("config/pipeline.yaml")


@dataclass
class BuildResult:
    spec: DatasetSpec
    build_id: str
    artifact_id: str
    manifest: LineageManifest
    manifest_path: Path
    trust_report: TrustReport
    truth_report_path: Path
    split_row_counts: dict[str, int]
    published_aliases: list[str] = field(default_factory=list)
    status_history: list[str] = field(default_factory=list)


@dataclass
class ArtifactInspection:
    ref: str
    artifact: ArtifactRecord
    manifest: LineageManifest
    manifest_path: Path
    trust_report: TrustReport | None
    dataset_spec: DatasetSpec | None
    feature_specs: list[FeatureSpec]
    label_pack: LabelPack | None
    aliases: list[AliasRecord]
    artifact_inputs: list[ArtifactInputRecord]
    status_history: list[ArtifactStatusEventRecord]
    feature_families: list[str]
    time_range: dict[str, str]
    split_row_counts: dict[str, int]
    schema_columns: list[str]
    trust_score_breakdown: dict[str, float | None]
    lineage_refs: dict[str, list[str]]


@dataclass
class ArtifactDiff:
    left: ArtifactInspection
    right: ArtifactInspection
    diff: dict[str, object]


CompileDatasetResult = BuildResult
InspectDatasetResult = ArtifactInspection
DiffDatasetResult = ArtifactDiff


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

    @classmethod
    def from_config_path(cls, config_path: str | Path | None = None) -> tuple["DatasetCompiler", CatalogDB]:
        resolved_config_path = _resolve_pipeline_config_path(config_path)
        cfg = load_config(resolved_config_path)
        paths = StoragePaths(cfg.storage.root)
        store = ParquetStore(cfg.storage.compression, cfg.storage.parquet_row_group_size)
        catalog = CatalogDB(paths.catalog_db_path())
        return cls(cfg, paths, store, catalog), catalog

    def compile_dataset(self, spec_path: Path, *, publish: bool | None = None) -> BuildResult:
        spec = load_dataset_spec(spec_path)
        self._catalog.register_dataset_spec(spec)

        if len(spec.symbols) != 1:
            raise NotImplementedError("Phase 2 compiler core supports exactly one symbol per DatasetSpec")
        if spec.base_clock != "M1":
            raise NotImplementedError("Phase 2 compiler core currently supports base_clock='M1' only")

        symbol = spec.symbols[0]
        feature_specs = resolve_feature_selectors(spec.feature_selectors)
        label_pack = resolve_label_pack(spec.label_pack_ref)
        resolved_code_version = code_version()
        resolved_merge_config_ref = merge_config_ref(self._cfg.merge)
        publish_requested = spec.publish_on_accept if publish is None else bool(publish)

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
            content_hash = self._dataset_content_hash(
                spec=spec,
                dataset_df=selected_df,
                split_frames=split_frames,
                state_artifact_id=state_result.artifact_id,
                feature_artifact_ids=[artifact.artifact_id for artifact in feature_result.artifacts],
                label_artifact_id=label_result.artifact_id,
            )
            artifact_id = build_artifact_id(spec.dataset_name, created_at, content_hash)
            artifact_root = self._paths.root / "datasets" / f"name={spec.dataset_name}" / f"artifact={artifact_id}"

            self._write_compiled_artifact(spec, artifact_id, split_frames)

            parent_artifact_refs = [
                state_result.artifact_id,
                *[artifact.artifact_id for artifact in feature_result.artifacts],
                label_result.artifact_id,
            ]
            input_partition_refs = sorted(
                set(
                    state_result.manifest.input_partition_refs
                    + [ref for artifact in feature_result.artifacts for ref in artifact.manifest.input_partition_refs]
                    + label_result.manifest.input_partition_refs
                )
            )
            feature_families = sorted({feature.family for feature in feature_specs})
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
                    "feature_families": feature_families,
                    "upstream_artifacts": parent_artifact_refs,
                },
            )

            manifest_path = self._persist_manifest(manifest, detail="compiler candidate created")
            self._catalog.update_build_status(build_id, "building", artifact_id=artifact_id)

            manifest = manifest.model_copy(update={"status": "truth_pending"})
            manifest_path = self._persist_manifest(manifest, detail="awaiting truth gate")
            persisted_truth_pending = read_manifest_sidecar(manifest_path)
            self._catalog.update_build_status(build_id, "truth_pending", artifact_id=artifact_id)

            trust_report = self._truth.evaluate_dataset(
                artifact_id=artifact_id,
                dataset_df=selected_df,
                split_frames=split_frames,
                spec=spec,
                feature_specs=feature_specs,
                label_pack=label_pack,
                manifest=persisted_truth_pending,
                paths=self._paths,
                store=self._store,
                expected_content_hash=content_hash,
                manifest_path=manifest_path,
            )
            truth_path = self._write_truth_report(trust_report)
            self._catalog.register_trust_report(trust_report)

            final_manifest = persisted_truth_pending.model_copy(
                update={"status": "rejected", "truth_report_ref": trust_report.report_id}
            )
            published_aliases: list[str] = []

            if trust_report.accepted_for_publication:
                accepted_manifest = final_manifest.model_copy(update={"status": "accepted"})
                manifest_path = self._persist_manifest(accepted_manifest, detail="truth gate accepted artifact")
                self._catalog.update_build_status(build_id, "accepted", artifact_id=artifact_id)
                final_manifest = accepted_manifest

                if publish_requested:
                    try:
                        self._assert_publishable(final_manifest, manifest_path, trust_report, content_hash)
                    except Exception as exc:
                        final_manifest = final_manifest.model_copy(update={"status": "rejected"})
                        manifest_path = self._persist_manifest(
                            final_manifest,
                            detail=f"publication rejected: {exc}",
                        )
                        self._catalog.finish_build(
                            build_id,
                            "rejected",
                            artifact_id=artifact_id,
                            error_message=str(exc),
                        )
                    else:
                        final_manifest = final_manifest.model_copy(update={"status": "published"})
                        manifest_path = self._persist_manifest(final_manifest, detail="artifact published")
                        published_aliases = [
                            f"dataset://{spec.dataset_name}@{spec.version}",
                            f"dataset://{spec.dataset_name}:latest",
                        ]
                        for alias in published_aliases:
                            self._catalog.upsert_alias(alias, artifact_id)
                        self._catalog.finish_build(build_id, "published", artifact_id=artifact_id)
                else:
                    self._catalog.finish_build(build_id, "accepted", artifact_id=artifact_id)
            else:
                manifest_path = self._persist_manifest(final_manifest, detail="truth gate rejected artifact")
                self._catalog.finish_build(
                    build_id,
                    "rejected",
                    artifact_id=artifact_id,
                    error_message="truth gate rejected candidate dataset",
                )

            status_history = [event.status for event in self._catalog.get_artifact_status_history(artifact_id)]
            return BuildResult(
                spec=spec,
                build_id=build_id,
                artifact_id=artifact_id,
                manifest=final_manifest,
                manifest_path=manifest_path,
                trust_report=trust_report,
                truth_report_path=truth_path,
                split_row_counts={name: len(frame) for name, frame in split_frames.items()},
                published_aliases=published_aliases,
                status_history=status_history,
            )
        except Exception as exc:
            self._catalog.finish_build(build_id, "failed", error_message=str(exc))
            raise

    def inspect_dataset(self, ref: str) -> ArtifactInspection:
        manifest, manifest_path = self._resolve_manifest_with_path(ref)
        artifact = self._catalog.get_artifact(manifest.artifact_id)
        if artifact is None:
            raise KeyError(f"Artifact '{manifest.artifact_id}' is missing from the compiler catalog")

        trust_report = self._catalog.get_trust_report(manifest.artifact_id)
        dataset_spec = self._catalog.get_dataset_spec(manifest.dataset_spec_ref) if manifest.dataset_spec_ref else None
        feature_specs = [
            spec
            for spec in (self._catalog.get_feature_spec(key) for key in manifest.feature_spec_refs)
            if spec is not None
        ]
        label_pack = self._catalog.get_label_pack(manifest.label_pack_ref) if manifest.label_pack_ref else None
        aliases = self._catalog.list_aliases(manifest.artifact_id)
        artifact_inputs = self._catalog.list_artifact_inputs(manifest.artifact_id)
        status_history = self._catalog.get_artifact_status_history(manifest.artifact_id)
        feature_families = sorted({spec.family for spec in feature_specs})
        time_range = {
            "start": str(manifest.metadata.get("time_range_start", "")),
            "end": str(manifest.metadata.get("time_range_end", "")),
        }
        split_row_counts = dict(manifest.metadata.get("split_row_counts", {}))
        schema_columns = list(manifest.metadata.get("schema_columns", []))
        trust_score_breakdown = {
            "total": trust_report.trust_score_total if trust_report else None,
            "coverage": trust_report.coverage_score if trust_report else None,
            "leakage": trust_report.leakage_score if trust_report else None,
            "feature_quality": trust_report.feature_quality_score if trust_report else None,
            "label_quality": trust_report.label_quality_score if trust_report else None,
            "source_quality": trust_report.source_quality_score if trust_report else None,
            "lineage": trust_report.lineage_score if trust_report else None,
        }
        lineage_refs = {
            "input_partition_refs": list(manifest.input_partition_refs),
            "state_artifact_refs": list(manifest.state_artifact_refs),
            "parent_artifact_refs": list(manifest.parent_artifact_refs),
            "artifact_inputs": [record.input_ref for record in artifact_inputs],
        }

        return ArtifactInspection(
            ref=ref,
            artifact=artifact,
            manifest=manifest,
            manifest_path=manifest_path,
            trust_report=trust_report,
            dataset_spec=dataset_spec,
            feature_specs=feature_specs,
            label_pack=label_pack,
            aliases=aliases,
            artifact_inputs=artifact_inputs,
            status_history=status_history,
            feature_families=feature_families,
            time_range=time_range,
            split_row_counts=split_row_counts,
            schema_columns=schema_columns,
            trust_score_breakdown=trust_score_breakdown,
            lineage_refs=lineage_refs,
        )

    def diff_datasets(self, left_ref: str, right_ref: str) -> ArtifactDiff:
        left = self.inspect_dataset(left_ref)
        right = self.inspect_dataset(right_ref)

        left_spec_dump = left.dataset_spec.model_dump(mode="json") if left.dataset_spec is not None else {}
        right_spec_dump = right.dataset_spec.model_dump(mode="json") if right.dataset_spec is not None else {}
        spec_diff = {
            key: {"left": left_spec_dump.get(key), "right": right_spec_dump.get(key)}
            for key in sorted(set(left_spec_dump) | set(right_spec_dump))
            if left_spec_dump.get(key) != right_spec_dump.get(key)
        }

        left_feature_keys = [spec.key for spec in left.feature_specs]
        right_feature_keys = [spec.key for spec in right.feature_specs]
        schema_left = set(left.schema_columns)
        schema_right = set(right.schema_columns)
        lineage_left = set(left.lineage_refs["artifact_inputs"])
        lineage_right = set(right.lineage_refs["artifact_inputs"])

        diff = {
            "artifact_id_changed": left.artifact.artifact_id != right.artifact.artifact_id,
            "logical_name_changed": left.artifact.logical_name != right.artifact.logical_name,
            "logical_version_changed": left.artifact.logical_version != right.artifact.logical_version,
            "dataset_spec_ref_changed": left.manifest.dataset_spec_ref != right.manifest.dataset_spec_ref,
            "dataset_spec_differences": spec_diff,
            "feature_spec_refs_added": sorted(set(right_feature_keys) - set(left_feature_keys)),
            "feature_spec_refs_removed": sorted(set(left_feature_keys) - set(right_feature_keys)),
            "feature_families_added": sorted(set(right.feature_families) - set(left.feature_families)),
            "feature_families_removed": sorted(set(left.feature_families) - set(right.feature_families)),
            "label_pack_changed": (left.label_pack.key if left.label_pack else None) != (right.label_pack.key if right.label_pack else None),
            "label_pack_left": left.label_pack.key if left.label_pack else None,
            "label_pack_right": right.label_pack.key if right.label_pack else None,
            "schema_columns_added": sorted(schema_right - schema_left),
            "schema_columns_removed": sorted(schema_left - schema_right),
            "split_row_counts_left": left.split_row_counts,
            "split_row_counts_right": right.split_row_counts,
            "trust_score_left": left.trust_score_breakdown,
            "trust_score_right": right.trust_score_breakdown,
            "lineage_inputs_added": sorted(lineage_right - lineage_left),
            "lineage_inputs_removed": sorted(lineage_left - lineage_right),
            "state_artifact_refs_left": left.manifest.state_artifact_refs,
            "state_artifact_refs_right": right.manifest.state_artifact_refs,
        }
        return ArtifactDiff(left=left, right=right, diff=diff)

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

    def _persist_manifest(self, manifest: LineageManifest, *, detail: str = "") -> Path:
        manifest_path = write_manifest_sidecar(manifest, self._paths)
        self._catalog.register_artifact(manifest, str(manifest_path), detail=detail)
        return manifest_path

    def _resolve_manifest_with_path(self, ref: str) -> tuple[LineageManifest, Path]:
        maybe_path = Path(ref)
        if maybe_path.exists():
            return read_manifest_sidecar(maybe_path), maybe_path

        artifact = self._catalog.resolve_artifact(ref)
        if artifact is None:
            raise KeyError(f"Artifact '{ref}' was not found in the compiler catalog")
        manifest_path = Path(artifact.manifest_uri)
        return read_manifest_sidecar(manifest_path), manifest_path

    @staticmethod
    def _dataset_content_hash(
        *,
        spec: DatasetSpec,
        dataset_df: pl.DataFrame,
        split_frames: dict[str, pl.DataFrame],
        state_artifact_id: str,
        feature_artifact_ids: list[str],
        label_artifact_id: str,
    ) -> str:
        return compute_content_hash(
            {
                "dataset_spec": spec.model_dump(mode="json"),
                "columns": dataset_df.columns,
                "rows": len(dataset_df),
                "split_rows": {name: len(frame) for name, frame in split_frames.items()},
                "time_range_start": str(dataset_df["time_utc"].min()) if not dataset_df.is_empty() else "",
                "time_range_end": str(dataset_df["time_utc"].max()) if not dataset_df.is_empty() else "",
                "state_artifact_id": state_artifact_id,
                "feature_artifact_ids": feature_artifact_ids,
                "label_artifact_id": label_artifact_id,
            }
        )

    def _assert_publishable(
        self,
        manifest: LineageManifest,
        manifest_path: Path,
        trust_report: TrustReport,
        expected_content_hash: str,
    ) -> None:
        if not trust_report.accepted_for_publication:
            raise RuntimeError("Artifact cannot be published because the trust report is not accepted")
        if not manifest.truth_report_ref:
            raise RuntimeError("Artifact cannot be published without a linked TrustReport reference")
        if manifest.content_hash != expected_content_hash:
            raise RuntimeError("Artifact cannot be published because the manifest content hash does not match the compiler payload hash")

        manifest_from_disk = read_manifest_sidecar(manifest_path)
        if manifest_from_disk.content_hash != expected_content_hash:
            raise RuntimeError("Artifact cannot be published because the persisted manifest content hash is inconsistent")

        artifact = self._catalog.get_artifact(manifest.artifact_id)
        if artifact is None:
            raise RuntimeError("Artifact cannot be published because the compiler catalog entry is missing")
        if artifact.content_hash != expected_content_hash:
            raise RuntimeError("Artifact cannot be published because the catalog content hash is inconsistent")

    @staticmethod
    def format_inspect_result(result: ArtifactInspection) -> str:
        trust = result.trust_report
        lines = [
            f"artifact_id: {result.artifact.artifact_id}",
            f"logical: {result.artifact.logical_name}@{result.artifact.logical_version}",
            f"status: {result.artifact.status}",
            f"manifest_path: {result.manifest_path}",
            f"artifact_uri: {result.artifact.artifact_uri}",
            f"time_range: {result.time_range['start']} -> {result.time_range['end']}",
            f"split_rows: {json.dumps(result.split_row_counts, sort_keys=True)}",
            f"feature_families: {', '.join(result.feature_families) if result.feature_families else '-'}",
            f"label_pack: {result.label_pack.key if result.label_pack else '-'}",
            f"dataset_spec_ref: {result.manifest.dataset_spec_ref or '-'}",
            f"lineage_inputs: {len(result.lineage_refs['artifact_inputs'])}",
        ]
        if trust is not None:
            lines.append(
                "trust_scores: "
                + json.dumps(
                    {
                        "total": trust.trust_score_total,
                        "coverage": trust.coverage_score,
                        "leakage": trust.leakage_score,
                        "feature_quality": trust.feature_quality_score,
                        "label_quality": trust.label_quality_score,
                        "source_quality": trust.source_quality_score,
                        "lineage": trust.lineage_score,
                    },
                    sort_keys=True,
                )
            )
        return "\n".join(lines)

    @staticmethod
    def format_diff_result(result: ArtifactDiff) -> str:
        lines = ["Artifact diff"]
        for key, value in result.diff.items():
            lines.append(f"{key}: {value}")
        return "\n".join(lines)


def compile_dataset_spec(spec_path: str | Path, *, publish: bool = True) -> BuildResult:
    compiler, catalog = DatasetCompiler.from_config_path(_resolve_pipeline_config_path_from_spec(spec_path))
    try:
        return compiler.compile_dataset(Path(spec_path), publish=publish)
    finally:
        catalog.close()


def inspect_artifact(ref: str) -> ArtifactInspection:
    compiler, catalog = _build_compiler_for_ref(ref)
    try:
        return compiler.inspect_dataset(ref)
    finally:
        catalog.close()


def diff_artifacts(left_ref: str, right_ref: str) -> ArtifactDiff:
    compiler, catalog = _build_compiler_for_ref(left_ref)
    try:
        return compiler.diff_datasets(left_ref, right_ref)
    finally:
        catalog.close()


def _build_compiler_for_ref(ref: str) -> tuple[DatasetCompiler, CatalogDB]:
    config_path = _resolve_pipeline_config_path_from_ref(ref)
    return DatasetCompiler.from_config_path(config_path)


def _resolve_pipeline_config_path_from_spec(spec_path: str | Path) -> Path:
    spec = Path(spec_path)
    if spec.exists():
        candidate = _find_repo_config(spec)
        if candidate is not None:
            return candidate
    return _resolve_pipeline_config_path()


def _resolve_pipeline_config_path_from_ref(ref: str) -> Path:
    maybe_path = Path(ref)
    if maybe_path.exists():
        inferred_root = _infer_storage_root_from_manifest_path(maybe_path)
        if inferred_root is not None:
            candidate = _resolve_pipeline_config_path()
            if candidate.exists():
                return candidate
    return _resolve_pipeline_config_path()


def _resolve_pipeline_config_path(config_path: str | Path | None = None) -> Path:
    if config_path is not None:
        resolved = Path(config_path)
        if resolved.exists():
            return resolved.resolve()
        raise FileNotFoundError(f"Config file not found: {resolved}")

    candidate = _find_repo_config(Path.cwd())
    if candidate is not None:
        return candidate
    raise FileNotFoundError(f"Config file not found: {DEFAULT_CONFIG_RELATIVE_PATH}")


def _find_repo_config(anchor: Path) -> Path | None:
    current = anchor.resolve()
    if current.is_file():
        current = current.parent
    for parent in [current, *current.parents]:
        candidate = parent / DEFAULT_CONFIG_RELATIVE_PATH
        if candidate.exists():
            return candidate
    return None


def _infer_storage_root_from_manifest_path(path: Path) -> Path | None:
    resolved = path.resolve()
    for parent in [resolved.parent, *resolved.parents]:
        if parent.name == "manifests":
            return parent.parent
    return None
