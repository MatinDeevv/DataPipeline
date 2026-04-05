"""Truth gate for compiler-era dataset artifacts."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.features.public import FeatureSpec
from mt5pipe.labels.public import LabelPack
from mt5pipe.quality.report import dataset_quality_report
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths
from mt5pipe.truth.models import QaCheckResult, TrustReport


class TruthService:
    """Evaluate a candidate dataset artifact and gate publication."""

    MIN_TOTAL_SCORE = 80.0
    MIN_COVERAGE_SCORE = 85.0
    MIN_FEATURE_QUALITY_SCORE = 80.0
    MIN_LABEL_QUALITY_SCORE = 80.0
    MIN_SOURCE_QUALITY_SCORE = 60.0

    FAMILY_MISSINGNESS_THRESHOLDS = {
        "time": 0.0,
        "session": 0.0,
        "quality": 0.0,
        "htf_context": 100.0,
        "disagreement": 15.0,
        "event_shape": 15.0,
        "entropy": 20.0,
    }

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
        state_df: pl.DataFrame | None = None,
        build_row_stats: dict[str, int] | None = None,
        expected_content_hash: str | None = None,
        manifest_path: Path | None = None,
    ) -> TrustReport:
        quality = dataset_quality_report(dataset_df)
        checks: list[QaCheckResult] = []
        hard_failures: list[str] = []
        warnings: list[str] = []
        build_row_stats = build_row_stats or {}

        coverage_check = self._coverage_check(dataset_df, split_frames)
        checks.append(coverage_check)
        if coverage_check.status == "failed":
            hard_failures.append("dataset_coverage_failure")
        coverage_score = coverage_check.score

        split_integrity_check = self._split_integrity_check(dataset_df, split_frames, spec)
        checks.append(split_integrity_check)
        if split_integrity_check.status == "failed":
            hard_failures.append("split_integrity_failure")

        leakage_check = self._leakage_check(dataset_df, feature_specs, split_integrity_check)
        checks.append(leakage_check)
        if leakage_check.status == "failed":
            hard_failures.append("leakage_or_duplicate_timestamp_failure")
        leakage_score = leakage_check.score

        feature_contracts_check = self._feature_contracts_check(dataset_df, feature_specs)
        checks.append(feature_contracts_check)
        if feature_contracts_check.status == "failed":
            hard_failures.append("missing_required_feature_columns")

        family_missingness_check = self._feature_family_missingness_check(dataset_df, feature_specs)
        checks.append(family_missingness_check)
        if family_missingness_check.status == "failed":
            hard_failures.append("feature_family_missingness_threshold_exceeded")
        elif family_missingness_check.status == "warning":
            warnings.append("feature_family_missingness_warning")

        warmup_check = self._warmup_and_row_loss_check(build_row_stats, feature_specs, label_pack)
        checks.append(warmup_check)
        if warmup_check.status == "failed":
            hard_failures.append("warmup_or_drop_row_sanity_failure")

        feature_quality_score = round(
            (
                feature_contracts_check.score
                + family_missingness_check.score
                + warmup_check.score
            ) / 3.0,
            4,
        )

        if quality.get("total_nulls", 0) > 0:
            warnings.append("dataset_contains_nulls")
        if quality.get("constant_columns"):
            warnings.append("dataset_contains_constant_columns")

        label_check = self._label_quality_check(dataset_df, label_pack)
        checks.append(label_check)
        if label_check.status == "failed":
            hard_failures.append("label_columns_missing")
        elif label_check.status == "warning":
            warnings.append("label_quality_warning")
        label_quality_score = label_check.score

        resolved_state_df = state_df if state_df is not None else pl.DataFrame()
        source_check = self._source_quality_check(
            symbol=spec.symbols[0],
            date_from=spec.date_from,
            date_to=spec.date_to,
            paths=paths,
            store=store,
            state_df=resolved_state_df,
        )
        checks.append(source_check)
        if source_check.status == "failed":
            hard_failures.append("source_quality_below_threshold")
        elif source_check.status == "warning":
            warnings.append("source_quality_warning")
        source_quality_score = source_check.score

        lineage_check = self._lineage_check(manifest)
        checks.append(lineage_check)
        if lineage_check.status == "failed":
            hard_failures.append("lineage_incomplete")
        lineage_score = lineage_check.score

        manifest_integrity_check = self._manifest_integrity_check(
            manifest=manifest,
            expected_content_hash=expected_content_hash,
            manifest_path=manifest_path,
        )
        checks.append(manifest_integrity_check)
        if manifest_integrity_check.status == "failed":
            hard_failures.append("manifest_hash_mismatch")

        trust_score_total = round(
            coverage_score * 0.20
            + leakage_score * 0.20
            + feature_quality_score * 0.20
            + label_quality_score * 0.15
            + source_quality_score * 0.10
            + lineage_score * 0.15,
            4,
        )

        accepted = (
            not hard_failures
            and trust_score_total >= self.MIN_TOTAL_SCORE
            and coverage_score >= self.MIN_COVERAGE_SCORE
            and leakage_score == 100.0
            and feature_quality_score >= self.MIN_FEATURE_QUALITY_SCORE
            and label_quality_score >= self.MIN_LABEL_QUALITY_SCORE
            and source_quality_score >= self.MIN_SOURCE_QUALITY_SCORE
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
            trust_score_total=trust_score_total,
            coverage_score=round(coverage_score, 4),
            leakage_score=round(leakage_score, 4),
            feature_quality_score=round(feature_quality_score, 4),
            label_quality_score=round(label_quality_score, 4),
            source_quality_score=round(source_quality_score, 4),
            lineage_score=round(lineage_score, 4),
            hard_failures=sorted(set(hard_failures)),
            warnings=sorted(set(warnings)),
            metrics={
                "rows": len(dataset_df),
                "columns": len(dataset_df.columns),
                "duplicate_primary_clock_rows": int(leakage_check.metrics.get("duplicate_primary_clock_rows", 0)),
                "quality_score": float(quality.get("quality_score", 0.0)),
                "build_row_stats": build_row_stats,
            },
            thresholds={
                "min_total_score": self.MIN_TOTAL_SCORE,
                "min_coverage_score": self.MIN_COVERAGE_SCORE,
                "min_feature_quality_score": self.MIN_FEATURE_QUALITY_SCORE,
                "min_label_quality_score": self.MIN_LABEL_QUALITY_SCORE,
                "min_source_quality_score": self.MIN_SOURCE_QUALITY_SCORE,
                "family_missingness_thresholds": self.FAMILY_MISSINGNESS_THRESHOLDS,
            },
            generated_at=dt.datetime.now(dt.timezone.utc),
            checks=checks,
        )

    def _coverage_check(self, dataset_df: pl.DataFrame, split_frames: dict[str, pl.DataFrame]) -> QaCheckResult:
        dataset_empty = dataset_df.is_empty()
        empty_splits = [name for name, frame in split_frames.items() if frame.is_empty()]
        failed = dataset_empty or bool(empty_splits)
        return QaCheckResult(
            check_name="coverage",
            status="failed" if failed else "passed",
            score=0.0 if failed else 100.0,
            metrics={"rows": len(dataset_df), "split_rows": {k: len(v) for k, v in split_frames.items()}},
            thresholds={"min_rows": 1, "required_non_empty_splits": sorted(split_frames)},
            failure_reason=(
                "dataset artifact has no rows"
                if dataset_empty
                else f"one or more required splits is empty: {', '.join(empty_splits)}"
                if empty_splits
                else ""
            ),
        )

    def _split_integrity_check(
        self,
        dataset_df: pl.DataFrame,
        split_frames: dict[str, pl.DataFrame],
        spec: DatasetSpec,
    ) -> QaCheckResult:
        split_ranges: dict[str, tuple[str, str]] = {}
        overlap_detected = False
        ordering_failure = False

        previous_end: dt.datetime | None = None
        ordered_split_names = self._ordered_split_names(split_frames)
        for split_name in ordered_split_names:
            frame = split_frames[split_name]
            if frame.is_empty() or "time_utc" not in frame.columns:
                continue
            start = frame["time_utc"].min()
            end = frame["time_utc"].max()
            split_ranges[split_name] = (str(start), str(end))
            if previous_end is not None and start is not None and start <= previous_end:
                overlap_detected = True
            previous_end = end

        if not dataset_df.is_empty() and {"symbol", "timeframe", "time_utc"}.issubset(set(dataset_df.columns)):
            total_unique = dataset_df.select(["symbol", "timeframe", "time_utc"]).unique().height
            split_unique = 0
            for frame in split_frames.values():
                if frame.is_empty():
                    continue
                current = frame.select([col for col in ["symbol", "timeframe", "time_utc"] if col in frame.columns]).unique().height
                split_unique += current
            ordering_failure = split_unique > total_unique

        failed = overlap_detected or ordering_failure
        return QaCheckResult(
            check_name="split_integrity",
            status="failed" if failed else "passed",
            score=0.0 if failed else 100.0,
            metrics={
                "split_ranges": split_ranges,
                "split_policy": spec.split_policy,
                "overlap_detected": overlap_detected,
                "ordering_failure": ordering_failure,
            },
            thresholds={"strict_temporal_ordering": True, "no_overlap": True},
            failure_reason="splits overlap or violate strict temporal ordering" if failed else "",
        )

    def _leakage_check(
        self,
        dataset_df: pl.DataFrame,
        feature_specs: list[FeatureSpec],
        split_integrity_check: QaCheckResult,
    ) -> QaCheckResult:
        duplicate_rows = self._duplicate_primary_clock_rows(dataset_df)
        all_pit_safe = all(feature.point_in_time_safe for feature in feature_specs)
        failed = duplicate_rows > 0 or not all_pit_safe or split_integrity_check.status == "failed"
        return QaCheckResult(
            check_name="leakage",
            status="failed" if failed else "passed",
            score=0.0 if failed else 100.0,
            metrics={
                "duplicate_primary_clock_rows": duplicate_rows,
                "all_features_pit_safe": all_pit_safe,
                "split_integrity_status": split_integrity_check.status,
            },
            thresholds={"duplicate_primary_clock_rows": 0, "all_features_pit_safe": True, "split_integrity": "passed"},
            failure_reason="duplicate primary clock rows, non-PIT feature, or split leakage detected" if failed else "",
        )

    def _feature_contracts_check(self, dataset_df: pl.DataFrame, feature_specs: list[FeatureSpec]) -> QaCheckResult:
        missing_required_feature_columns = [
            col
            for feature in feature_specs
            for col in feature.output_columns
            if col not in dataset_df.columns
        ]
        failed = bool(missing_required_feature_columns)
        return QaCheckResult(
            check_name="feature_contracts",
            status="failed" if failed else "passed",
            score=0.0 if failed else 100.0,
            metrics={"missing_required_feature_columns": missing_required_feature_columns},
            thresholds={"missing_required_feature_columns": []},
            failure_reason="required feature outputs are missing from the dataset artifact" if failed else "",
        )

    def _feature_family_missingness_check(self, dataset_df: pl.DataFrame, feature_specs: list[FeatureSpec]) -> QaCheckResult:
        family_metrics: dict[str, dict[str, float | int]] = {}
        failed_families: list[str] = []
        warning_families: list[str] = []

        for family, family_specs in self._group_feature_specs_by_family(feature_specs).items():
            columns = sorted({column for feature in family_specs for column in feature.output_columns})
            present_columns = [column for column in columns if column in dataset_df.columns]
            threshold = self._family_missingness_threshold(family, family_specs)

            if not present_columns or dataset_df.is_empty():
                null_pct = 100.0 if columns else 0.0
            else:
                total_cells = len(dataset_df) * len(present_columns)
                null_cells = sum(int(dataset_df[column].null_count()) for column in present_columns)
                null_pct = round((null_cells / total_cells * 100.0) if total_cells else 0.0, 4)

            family_metrics[family] = {
                "expected_columns": len(columns),
                "present_columns": len(present_columns),
                "null_pct": null_pct,
                "threshold_pct": threshold,
            }

            if len(present_columns) != len(columns) or null_pct > threshold:
                failed_families.append(family)
            elif threshold > 0.0 and null_pct > threshold * 0.75:
                warning_families.append(family)

        score = 100.0
        for metrics in family_metrics.values():
            null_pct = float(metrics["null_pct"])
            threshold = float(metrics["threshold_pct"])
            if threshold <= 0.0:
                if null_pct > 0.0:
                    score = 0.0
                    break
            else:
                score -= min((null_pct / threshold) * 10.0, 20.0)
        score = max(0.0, round(score, 4))

        status = "failed" if failed_families else "warning" if warning_families else "passed"
        return QaCheckResult(
            check_name="feature_family_missingness",
            status=status,
            score=0.0 if failed_families else score,
            metrics={"families": family_metrics, "failed_families": failed_families, "warning_families": warning_families},
            thresholds={"family_missingness_thresholds": self.FAMILY_MISSINGNESS_THRESHOLDS},
            failure_reason=f"feature family missingness exceeded threshold for: {', '.join(sorted(failed_families))}" if failed_families else "",
        )

    def _warmup_and_row_loss_check(
        self,
        build_row_stats: dict[str, int],
        feature_specs: list[FeatureSpec],
        label_pack: LabelPack,
    ) -> QaCheckResult:
        drop_row_removed = int(build_row_stats.get("feature_drop_row_rows_removed", 0))
        label_purge_applied = int(build_row_stats.get("label_purge_rows_applied", 0))
        rows_after_join = int(build_row_stats.get("rows_after_join", 0))
        drop_row_budget = max(
            [feature.warmup_rows for feature in feature_specs if feature.missingness_policy == "drop_row"] or [0]
        )
        failed = False
        reasons: list[str] = []

        if drop_row_removed > max(drop_row_budget, 0):
            failed = True
            reasons.append("drop-row removal exceeded declared warmup budget")
        if rows_after_join > label_pack.purge_rows and label_purge_applied != label_pack.purge_rows:
            failed = True
            reasons.append("label purge rows applied does not match label pack contract")

        return QaCheckResult(
            check_name="warmup_and_row_loss",
            status="failed" if failed else "passed",
            score=0.0 if failed else 100.0,
            metrics={
                "feature_drop_row_rows_removed": drop_row_removed,
                "drop_row_budget": drop_row_budget,
                "label_purge_rows_applied": label_purge_applied,
                "expected_label_purge_rows": label_pack.purge_rows,
                "rows_after_join": rows_after_join,
            },
            thresholds={"max_drop_row_rows_removed": drop_row_budget, "expected_label_purge_rows": label_pack.purge_rows},
            failure_reason="; ".join(reasons) if reasons else "",
        )

    def _label_quality_check(self, dataset_df: pl.DataFrame, label_pack: LabelPack) -> QaCheckResult:
        label_cols = [col for col in label_pack.output_columns if col in dataset_df.columns]
        missing_label_columns = [col for col in label_pack.output_columns if col not in dataset_df.columns]
        all_null_label_columns = [
            col for col in label_cols
            if int(dataset_df[col].null_count()) == len(dataset_df) and not dataset_df.is_empty()
        ]
        label_nulls = sum(int(dataset_df[col].null_count()) for col in label_cols)

        if missing_label_columns or all_null_label_columns:
            score = 0.0
            status = "failed"
        elif label_nulls > 0:
            score = 80.0
            status = "warning"
        else:
            score = 100.0
            status = "passed"

        return QaCheckResult(
            check_name="label_quality",
            status=status,
            score=score,
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
        )

    def _source_quality_check(
        self,
        *,
        symbol: str,
        date_from: dt.date,
        date_to: dt.date,
        paths: StoragePaths,
        store: ParquetStore,
        state_df: pl.DataFrame,
    ) -> QaCheckResult:
        metrics = self._source_quality_metrics(symbol, date_from, date_to, paths, store, state_df)
        score = self._source_quality_score(metrics)
        failed = score < self.MIN_SOURCE_QUALITY_SCORE
        warning = not failed and score < 75.0
        return QaCheckResult(
            check_name="source_quality",
            status="failed" if failed else "warning" if warning else "passed",
            score=score,
            metrics=metrics,
            thresholds={"min_source_quality_score": self.MIN_SOURCE_QUALITY_SCORE},
            failure_reason="source/merge quality metrics are below publication threshold" if failed else "",
        )

    def _lineage_check(self, manifest: LineageManifest) -> QaCheckResult:
        lineage_complete = bool(
            manifest.input_partition_refs
            and manifest.feature_spec_refs
            and manifest.label_pack_ref
            and manifest.state_artifact_refs
            and manifest.parent_artifact_refs
            and manifest.dataset_spec_ref
        )
        return QaCheckResult(
            check_name="lineage",
            status="passed" if lineage_complete else "failed",
            score=100.0 if lineage_complete else 0.0,
            metrics={
                "input_partition_refs": len(manifest.input_partition_refs),
                "state_artifact_refs": len(manifest.state_artifact_refs),
                "feature_spec_refs": len(manifest.feature_spec_refs),
                "label_pack_ref_present": bool(manifest.label_pack_ref),
                "dataset_spec_ref_present": bool(manifest.dataset_spec_ref),
                "parent_artifact_refs": len(manifest.parent_artifact_refs),
            },
            thresholds={
                "min_input_partition_refs": 1,
                "min_state_artifact_refs": 1,
                "min_feature_spec_refs": 1,
                "label_pack_ref_present": True,
                "dataset_spec_ref_present": True,
                "min_parent_artifact_refs": 1,
            },
            failure_reason="lineage manifest is incomplete" if not lineage_complete else "",
        )

    def _manifest_integrity_check(
        self,
        *,
        manifest: LineageManifest,
        expected_content_hash: str | None,
        manifest_path: Path | None,
    ) -> QaCheckResult:
        manifest_hash_matches = expected_content_hash is None or manifest.content_hash == expected_content_hash
        manifest_path_exists = manifest_path is None or manifest_path.exists()
        failed = not manifest_hash_matches or not manifest_path_exists
        return QaCheckResult(
            check_name="manifest_integrity",
            status="failed" if failed else "passed",
            score=0.0 if failed else 100.0,
            metrics={
                "manifest_content_hash": manifest.content_hash,
                "expected_content_hash": expected_content_hash or manifest.content_hash,
                "manifest_path": str(manifest_path) if manifest_path is not None else "",
                "manifest_path_exists": manifest_path_exists,
            },
            thresholds={"manifest_content_hash_matches": True, "manifest_path_exists": True},
            failure_reason="manifest content hash or persisted manifest path is inconsistent" if failed else "",
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
        state_df: pl.DataFrame,
    ) -> dict[str, float]:
        frames: list[pl.DataFrame] = []
        current = date_from
        while current <= date_to:
            day = store.read_dir(paths.merge_qa_dir(symbol, current))
            if not day.is_empty():
                frames.append(day)
            current += dt.timedelta(days=1)

        merge_df = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
        state_rows = float(len(state_df))
        return {
            "merge_qa_days": float(merge_df.height),
            "dual_source_ratio_mean": float(merge_df["dual_source_ratio"].mean()) if "dual_source_ratio" in merge_df.columns else 0.0,
            "merge_conflict_mean": float(merge_df["conflicts"].mean()) if "conflicts" in merge_df.columns else 0.0,
            "state_rows": state_rows,
            "state_quality_mean": float(state_df["quality_score"].mean()) if "quality_score" in state_df.columns and not state_df.is_empty() else 0.0,
            "state_conflict_rate": float(state_df["conflict_flag"].cast(pl.Float64).mean()) if "conflict_flag" in state_df.columns and not state_df.is_empty() else 0.0,
            "state_filled_ratio": float(
                state_df["trust_flags"].map_elements(
                    lambda flags: 1.0 if isinstance(flags, list) and "filled_gap" in flags else 0.0,
                    return_dtype=pl.Float64,
                ).mean()
            ) if "trust_flags" in state_df.columns and not state_df.is_empty() else 0.0,
        }

    def _source_quality_score(self, metrics: dict[str, float]) -> float:
        state_quality_mean = metrics.get("state_quality_mean", 0.0)
        merge_qa_days = metrics.get("merge_qa_days", 0.0)
        dual_source_ratio_mean = metrics.get("dual_source_ratio_mean", 0.0)
        merge_conflict_mean = metrics.get("merge_conflict_mean", 0.0)
        state_conflict_rate = metrics.get("state_conflict_rate", 0.0)
        state_filled_ratio = metrics.get("state_filled_ratio", 0.0)

        base = state_quality_mean if state_quality_mean > 0 else 70.0
        dual_source_component = min(max(dual_source_ratio_mean, 0.0) * 100.0, 100.0)
        if merge_qa_days <= 0:
            score = base
        else:
            score = base * 0.85 + dual_source_component * 0.15
            score -= min(max(merge_conflict_mean, 0.0) * 10.0, 10.0)

        # quality_score already captures most state-level conflict behavior, so only
        # penalize conflict rate directly when the state artifact does not expose it.
        if state_quality_mean <= 0:
            score -= min(max(state_conflict_rate, 0.0) * 20.0, 10.0)

        score -= min(max(state_filled_ratio, 0.0) * 35.0, 15.0)
        return round(max(0.0, min(100.0, score)), 4)

    @staticmethod
    def _group_feature_specs_by_family(feature_specs: list[FeatureSpec]) -> dict[str, list[FeatureSpec]]:
        grouped: dict[str, list[FeatureSpec]] = {}
        for spec in feature_specs:
            grouped.setdefault(spec.family, []).append(spec)
        return grouped

    def _family_missingness_threshold(self, family: str, specs: list[FeatureSpec]) -> float:
        if family in self.FAMILY_MISSINGNESS_THRESHOLDS:
            return self.FAMILY_MISSINGNESS_THRESHOLDS[family]
        if any(spec.missingness_policy in {"allow"} for spec in specs):
            return 100.0
        return 0.0

    @staticmethod
    def _ordered_split_names(split_frames: dict[str, pl.DataFrame]) -> list[str]:
        if {"train", "val", "test"}.issubset(set(split_frames)):
            return ["train", "val", "test"]
        return sorted(split_frames)
