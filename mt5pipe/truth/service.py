"""Truth gate scaffold for compiler-era dataset artifacts."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.features.registry.models import FeatureSpec
from mt5pipe.labels.registry.models import LabelPack
from mt5pipe.quality.report import dataset_quality_report
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths
from mt5pipe.truth.models import QaCheckResult, TrustReport


class TruthService:
    """Evaluate a candidate dataset artifact and gate publication."""

    MIN_TOTAL_SCORE = 80.0
    MIN_COVERAGE_SCORE = 85.0

    def evaluate_dataset(
        self,
        *,
        artifact_id: str,
        dataset_df: pl.DataFrame,
        split_frames: dict[str, pl.DataFrame],
        spec: DatasetSpec,
        feature_specs: list[FeatureSpec],
        label_pack: LabelPack,
        manifest: LineageManifest,
        paths: StoragePaths,
        store: ParquetStore,
        expected_content_hash: str | None = None,
        manifest_path: Path | None = None,
    ) -> TrustReport:
        quality = dataset_quality_report(dataset_df)
        checks: list[QaCheckResult] = []
        hard_failures: list[str] = []
        warnings: list[str] = []

        coverage_pct = 100.0 if not dataset_df.is_empty() else 0.0
        coverage_score = coverage_pct
        if dataset_df.is_empty():
            hard_failures.append("dataset_empty")
        for split_name, frame in split_frames.items():
            if frame.is_empty():
                hard_failures.append(f"empty_split:{split_name}")
        checks.append(QaCheckResult(
            check_name="coverage",
            status="failed" if dataset_df.is_empty() or any(frame.is_empty() for frame in split_frames.values()) else "passed",
            score=coverage_score,
            metrics={"rows": len(dataset_df), "split_rows": {k: len(v) for k, v in split_frames.items()}},
            thresholds={"min_rows": 1, "min_coverage_score": self.MIN_COVERAGE_SCORE},
            failure_reason=(
                "dataset artifact has no rows"
                if dataset_df.is_empty()
                else "one or more required splits is empty"
                if any(frame.is_empty() for frame in split_frames.values())
                else ""
            ),
        ))

        duplicate_rows = self._duplicate_primary_clock_rows(dataset_df)
        leakage_failed = duplicate_rows > 0 or any(not feature.point_in_time_safe for feature in feature_specs)
        if leakage_failed:
            hard_failures.append("leakage_or_duplicate_timestamp_failure")
        leakage_score = 0.0 if leakage_failed else 100.0
        checks.append(QaCheckResult(
            check_name="leakage",
            status="failed" if leakage_failed else "passed",
            score=leakage_score,
            metrics={
                "duplicate_primary_clock_rows": duplicate_rows,
                "all_features_pit_safe": all(fs.point_in_time_safe for fs in feature_specs),
            },
            thresholds={"duplicate_primary_clock_rows": 0, "all_features_pit_safe": True},
            failure_reason="duplicate primary clock rows or non-PIT feature detected" if leakage_failed else "",
        ))

        null_columns = quality.get("null_columns", {})
        total_nulls = int(quality.get("total_nulls", 0))
        feature_quality_score = max(0.0, float(quality.get("quality_score", 0.0)))
        if total_nulls > 0:
            warnings.append("dataset_contains_nulls")
        checks.append(QaCheckResult(
            check_name="feature_quality",
            status="warning" if total_nulls > 0 else "passed",
            score=feature_quality_score,
            metrics={"quality_score": feature_quality_score, "null_columns": list(null_columns.keys())},
            thresholds={"null_columns": 0},
            failure_reason="",
        ))

        missing_required_feature_columns = [
            col
            for feature in feature_specs
            if feature.missingness_policy == "fail"
            for col in feature.output_columns
            if col not in dataset_df.columns
        ]
        if missing_required_feature_columns:
            hard_failures.append("missing_required_feature_columns")
        checks.append(QaCheckResult(
            check_name="feature_contracts",
            status="failed" if missing_required_feature_columns else "passed",
            score=0.0 if missing_required_feature_columns else 100.0,
            metrics={"missing_required_feature_columns": missing_required_feature_columns},
            thresholds={"missing_required_feature_columns": []},
            failure_reason="required feature outputs are missing from the dataset artifact" if missing_required_feature_columns else "",
        ))

        label_cols = [col for col in label_pack.output_columns if col in dataset_df.columns]
        missing_label_columns = [col for col in label_pack.output_columns if col not in dataset_df.columns]
        all_null_label_columns = [
            col for col in label_cols
            if int(dataset_df[col].null_count()) == len(dataset_df)
        ]
        label_nulls = sum(int(dataset_df[col].null_count()) for col in label_cols)
        label_quality_score = (
            100.0
            if not missing_label_columns and not all_null_label_columns and label_nulls == 0
            else 80.0 if label_cols and not missing_label_columns and not all_null_label_columns
            else 0.0
        )
        if missing_label_columns or all_null_label_columns:
            hard_failures.append("label_columns_missing")
        checks.append(QaCheckResult(
            check_name="label_quality",
            status="failed" if missing_label_columns or all_null_label_columns else "warning" if label_nulls > 0 else "passed",
            score=label_quality_score,
            metrics={
                "label_columns_present": len(label_cols),
                "label_nulls": label_nulls,
                "missing_label_columns": missing_label_columns,
                "all_null_label_columns": all_null_label_columns,
            },
            thresholds={"expected_label_columns": len(label_pack.output_columns), "label_nulls": 0},
            failure_reason=(
                "not all label columns were materialized"
                if missing_label_columns
                else "one or more label columns is entirely null"
                if all_null_label_columns
                else ""
            ),
        ))

        source_quality_score = self._source_quality_score(spec.symbols[0], spec.date_from, spec.date_to, paths, store)
        if source_quality_score < 70.0:
            warnings.append("source_quality_below_70")
        checks.append(QaCheckResult(
            check_name="source_quality",
            status="warning" if source_quality_score < 70.0 else "passed",
            score=source_quality_score,
            metrics=self._source_quality_metrics(spec.symbols[0], spec.date_from, spec.date_to, paths, store),
            thresholds={"min_source_quality_score": 70.0},
            failure_reason="",
        ))

        lineage_complete = bool(
            manifest.input_partition_refs
            and manifest.feature_spec_refs
            and manifest.label_pack_ref
            and manifest.state_artifact_refs
            and manifest.parent_artifact_refs
        )
        lineage_score = 100.0 if lineage_complete else 0.0
        if not lineage_complete:
            hard_failures.append("lineage_incomplete")
        checks.append(QaCheckResult(
            check_name="lineage",
            status="failed" if not lineage_complete else "passed",
            score=lineage_score,
            metrics={
                "input_partition_refs": len(manifest.input_partition_refs),
                "state_artifact_refs": len(manifest.state_artifact_refs),
                "feature_spec_refs": len(manifest.feature_spec_refs),
                "label_pack_ref_present": bool(manifest.label_pack_ref),
                "parent_artifact_refs": len(manifest.parent_artifact_refs),
            },
            thresholds={
                "min_input_partition_refs": 1,
                "min_state_artifact_refs": 1,
                "label_pack_ref_present": True,
                "min_parent_artifact_refs": 1,
            },
            failure_reason="lineage manifest is incomplete" if not lineage_complete else "",
        ))

        manifest_hash_matches = expected_content_hash is None or manifest.content_hash == expected_content_hash
        if not manifest_hash_matches:
            hard_failures.append("manifest_hash_mismatch")
        checks.append(QaCheckResult(
            check_name="manifest_integrity",
            status="failed" if not manifest_hash_matches else "passed",
            score=100.0 if manifest_hash_matches else 0.0,
            metrics={
                "manifest_content_hash": manifest.content_hash,
                "expected_content_hash": expected_content_hash or manifest.content_hash,
                "manifest_path": str(manifest_path) if manifest_path is not None else "",
            },
            thresholds={"manifest_content_hash_matches": True},
            failure_reason="manifest content hash does not match the compiler payload hash" if not manifest_hash_matches else "",
        ))

        trust_score_total = (
            coverage_score * 0.25
            + leakage_score * 0.25
            + feature_quality_score * 0.15
            + label_quality_score * 0.15
            + source_quality_score * 0.10
            + lineage_score * 0.10
        )

        accepted = (
            not hard_failures
            and trust_score_total >= self.MIN_TOTAL_SCORE
            and coverage_score >= self.MIN_COVERAGE_SCORE
            and leakage_score == 100.0
            and lineage_score == 100.0
        )
        status = "accepted" if accepted else "rejected"

        return TrustReport(
            report_id=f"trust.{artifact_id}",
            artifact_id=artifact_id,
            artifact_kind="dataset",
            truth_policy_version=spec.truth_policy_ref,
            status=status,
            accepted_for_publication=accepted,
            trust_score_total=round(trust_score_total, 4),
            coverage_score=round(coverage_score, 4),
            leakage_score=round(leakage_score, 4),
            feature_quality_score=round(feature_quality_score, 4),
            label_quality_score=round(label_quality_score, 4),
            source_quality_score=round(source_quality_score, 4),
            lineage_score=round(lineage_score, 4),
            hard_failures=hard_failures,
            warnings=warnings,
            metrics={
                "rows": len(dataset_df),
                "columns": len(dataset_df.columns),
                "duplicate_primary_clock_rows": duplicate_rows,
                "quality_score": quality.get("quality_score", 0.0),
            },
            thresholds={
                "min_total_score": self.MIN_TOTAL_SCORE,
                "min_coverage_score": self.MIN_COVERAGE_SCORE,
                "leakage_score_required": 100.0,
                "lineage_score_required": 100.0,
            },
            generated_at=dt.datetime.now(dt.timezone.utc),
            checks=checks,
        )

    @staticmethod
    def _duplicate_primary_clock_rows(dataset_df: pl.DataFrame) -> int:
        required = {"symbol", "timeframe", "time_utc"}
        if not required.issubset(set(dataset_df.columns)):
            return 0
        unique_rows = dataset_df.select(["symbol", "timeframe", "time_utc"]).unique().height
        return max(0, len(dataset_df) - unique_rows)

    def _source_quality_metrics(
        self,
        symbol: str,
        date_from: dt.date,
        date_to: dt.date,
        paths: StoragePaths,
        store: ParquetStore,
    ) -> dict[str, float]:
        frames: list[pl.DataFrame] = []
        current = date_from
        while current <= date_to:
            day = store.read_dir(paths.merge_qa_dir(symbol, current))
            if not day.is_empty():
                frames.append(day)
            current += dt.timedelta(days=1)

        if not frames:
            return {"merge_qa_days": 0.0, "dual_source_ratio_mean": 0.0, "conflict_mean": 0.0}

        df = pl.concat(frames, how="diagonal_relaxed")
        return {
            "merge_qa_days": float(df.height),
            "dual_source_ratio_mean": float(df["dual_source_ratio"].mean()) if "dual_source_ratio" in df.columns else 0.0,
            "conflict_mean": float(df["conflicts"].mean()) if "conflicts" in df.columns else 0.0,
        }

    def _source_quality_score(
        self,
        symbol: str,
        date_from: dt.date,
        date_to: dt.date,
        paths: StoragePaths,
        store: ParquetStore,
    ) -> float:
        metrics = self._source_quality_metrics(symbol, date_from, date_to, paths, store)
        if metrics["merge_qa_days"] <= 0:
            return 70.0

        dual_component = min(metrics["dual_source_ratio_mean"] * 400.0, 100.0)
        conflict_penalty = min(metrics["conflict_mean"] * 20.0, 30.0)
        return max(0.0, min(100.0, 60.0 + dual_component - conflict_penalty))
