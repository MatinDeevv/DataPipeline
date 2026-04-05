"""Focused tests for truth-layer publish gate behavior."""

from __future__ import annotations

import datetime as dt

import polars as pl

from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.features.registry.defaults import get_default_feature_specs
from mt5pipe.labels.registry.defaults import get_default_label_packs
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths
from mt5pipe.truth.service import TruthService


UTC = dt.timezone.utc


def test_truth_rejects_manifest_hash_mismatch(tmp_path) -> None:
    dataset_df = pl.DataFrame(
        {
            "symbol": ["XAUUSD"] * 4,
            "timeframe": ["M1"] * 4,
            "time_utc": [dt.datetime(2026, 4, 1, 0, i, tzinfo=UTC) for i in range(4)],
            "open": [3000.0, 3000.1, 3000.2, 3000.3],
            "high": [3000.2, 3000.3, 3000.4, 3000.5],
            "low": [2999.9, 3000.0, 3000.1, 3000.2],
            "close": [3000.1, 3000.2, 3000.3, 3000.4],
            "tick_count": [10, 10, 10, 10],
            "spread_mean": [0.1, 0.1, 0.1, 0.1],
            "mid_return": [0.001, 0.001, 0.001, 0.001],
            "realized_vol": [0.001, 0.001, 0.001, 0.001],
            "source_count": [2, 2, 2, 2],
            "conflict_count": [0, 0, 0, 0],
            "hour": [0, 0, 0, 0],
            "minute": [0, 1, 2, 3],
            "weekday": [3, 3, 3, 3],
            "time_sin": [0.0, 0.1, 0.2, 0.3],
            "time_cos": [1.0, 0.9, 0.8, 0.7],
            "weekday_sin": [0.1, 0.1, 0.1, 0.1],
            "weekday_cos": [0.9, 0.9, 0.9, 0.9],
            "session_asia": [1, 1, 1, 1],
            "session_london": [0, 0, 0, 0],
            "session_ny": [0, 0, 0, 0],
            "session_overlap": [0, 0, 0, 0],
            "relative_spread": [0.00003, 0.00003, 0.00003, 0.00003],
            "conflict_ratio": [0.0, 0.0, 0.0, 0.0],
            "broker_diversity": [2, 2, 2, 2],
            "future_return_5m": [0.1, 0.1, 0.1, 0.1],
            "direction_5m": [1, 1, 1, 1],
            "triple_barrier_5m": [1, 1, 1, 1],
            "mae_5m": [0.01, 0.01, 0.01, 0.01],
            "mfe_5m": [0.02, 0.02, 0.02, 0.02],
            "future_return_15m": [0.1, 0.1, 0.1, 0.1],
            "direction_15m": [1, 1, 1, 1],
            "triple_barrier_15m": [1, 1, 1, 1],
            "mae_15m": [0.01, 0.01, 0.01, 0.01],
            "mfe_15m": [0.02, 0.02, 0.02, 0.02],
            "future_return_60m": [0.1, 0.1, 0.1, 0.1],
            "direction_60m": [1, 1, 1, 1],
            "triple_barrier_60m": [1, 1, 1, 1],
            "mae_60m": [0.01, 0.01, 0.01, 0.01],
            "mfe_60m": [0.02, 0.02, 0.02, 0.02],
            "future_return_240m": [0.1, 0.1, 0.1, 0.1],
            "direction_240m": [1, 1, 1, 1],
            "triple_barrier_240m": [1, 1, 1, 1],
            "mae_240m": [0.01, 0.01, 0.01, 0.01],
            "mfe_240m": [0.02, 0.02, 0.02, 0.02],
        }
    )
    split_frames = {"train": dataset_df.slice(0, 2), "val": dataset_df.slice(2, 1), "test": dataset_df.slice(3, 1)}
    spec = DatasetSpec(
        dataset_name="xau_core",
        version="1.0.0",
        symbols=["XAUUSD"],
        date_from=dt.date(2026, 4, 1),
        date_to=dt.date(2026, 4, 1),
        base_clock="M1",
        state_version_ref="state.default@1.0.0",
        feature_selectors=["time/*", "session/*", "quality/*"],
        label_pack_ref="core_tb_volscaled@1.0.0",
        split_policy="temporal_holdout",
        embargo_rows=1,
        truth_policy_ref="truth.default@1.0.0",
    )
    manifest = LineageManifest(
        manifest_id="manifest.dataset.xau_core.bad",
        artifact_id="dataset.xau_core.bad",
        artifact_kind="dataset",
        logical_name="xau_core",
        logical_version="1.0.0",
        artifact_uri="artifact://dataset.xau_core.bad",
        content_hash="bad-hash",
        build_id="build.test",
        created_at=dt.datetime.now(UTC),
        status="truth_pending",
        dataset_spec_ref=spec.key,
        state_artifact_refs=["state.XAUUSD.M1.123"],
        feature_spec_refs=[feature.key for feature in get_default_feature_specs()[:3]],
        label_pack_ref=get_default_label_packs()[0].key,
        truth_report_ref=None,
        code_version="workspace-local-no-git",
        input_partition_refs=["data/bars/symbol=XAUUSD/timeframe=M1/date=2026-04-01"],
        parent_artifact_refs=["state.XAUUSD.M1.123"],
    )

    paths = StoragePaths(tmp_path / "pipeline_data")
    store = ParquetStore(compression="snappy", row_group_size=1000)
    report = TruthService().evaluate_dataset(
        artifact_id=manifest.artifact_id,
        dataset_df=dataset_df,
        split_frames=split_frames,
        spec=spec,
        feature_specs=get_default_feature_specs()[:3],
        label_pack=get_default_label_packs()[0],
        manifest=manifest,
        paths=paths,
        store=store,
        expected_content_hash="good-hash",
        manifest_path=tmp_path / "manifest.json",
    )

    assert report.status == "rejected"
    assert report.accepted_for_publication is False
    assert "manifest_hash_mismatch" in report.hard_failures


def test_truth_rejects_feature_family_missingness_threshold_breach(tmp_path) -> None:
    dataset_df = pl.DataFrame(
        {
            "symbol": ["XAUUSD"] * 4,
            "timeframe": ["M1"] * 4,
            "time_utc": [dt.datetime(2026, 4, 1, 0, i, tzinfo=UTC) for i in range(4)],
            "open": [3000.0, 3000.1, 3000.2, 3000.3],
            "high": [3000.2, 3000.3, 3000.4, 3000.5],
            "low": [2999.9, 3000.0, 3000.1, 3000.2],
            "close": [3000.1, 3000.2, 3000.3, 3000.4],
            "tick_count": [10, 10, 10, 10],
            "spread_mean": [0.1, 0.1, 0.1, 0.1],
            "mid_return": [0.001, 0.001, 0.001, 0.001],
            "realized_vol": [0.001, 0.001, 0.001, 0.001],
            "source_count": [2, 2, 2, 2],
            "conflict_count": [0, 0, 0, 0],
            "hour": [0, 0, 0, 0],
            "minute": [0, 1, 2, 3],
            "weekday": [3, 3, 3, 3],
            "time_sin": [0.0, 0.1, 0.2, 0.3],
            "time_cos": [1.0, 0.9, 0.8, 0.7],
            "weekday_sin": [0.1, 0.1, 0.1, 0.1],
            "weekday_cos": [0.9, 0.9, 0.9, 0.9],
            "session_asia": [1, 1, 1, 1],
            "session_london": [0, 0, 0, 0],
            "session_ny": [0, 0, 0, 0],
            "session_overlap": [0, 0, 0, 0],
            "relative_spread": [0.00003, 0.00003, 0.00003, 0.00003],
            "conflict_ratio": [0.0, 0.0, 0.0, 0.0],
            "broker_diversity": [2, 2, 2, 2],
            "entropy_signal": [None, None, None, None],
            "future_return_5m": [0.1, 0.1, 0.1, 0.1],
            "direction_5m": [1, 1, 1, 1],
            "triple_barrier_5m": [1, 1, 1, 1],
            "mae_5m": [0.01, 0.01, 0.01, 0.01],
            "mfe_5m": [0.02, 0.02, 0.02, 0.02],
            "future_return_15m": [0.1, 0.1, 0.1, 0.1],
            "direction_15m": [1, 1, 1, 1],
            "triple_barrier_15m": [1, 1, 1, 1],
            "mae_15m": [0.01, 0.01, 0.01, 0.01],
            "mfe_15m": [0.02, 0.02, 0.02, 0.02],
            "future_return_60m": [0.1, 0.1, 0.1, 0.1],
            "direction_60m": [1, 1, 1, 1],
            "triple_barrier_60m": [1, 1, 1, 1],
            "mae_60m": [0.01, 0.01, 0.01, 0.01],
            "mfe_60m": [0.02, 0.02, 0.02, 0.02],
            "future_return_240m": [0.1, 0.1, 0.1, 0.1],
            "direction_240m": [1, 1, 1, 1],
            "triple_barrier_240m": [1, 1, 1, 1],
            "mae_240m": [0.01, 0.01, 0.01, 0.01],
            "mfe_240m": [0.02, 0.02, 0.02, 0.02],
        }
    )
    split_frames = {"train": dataset_df.slice(0, 2), "val": dataset_df.slice(2, 1), "test": dataset_df.slice(3, 1)}
    spec = DatasetSpec(
        dataset_name="xau_nonhuman",
        version="1.0.0",
        symbols=["XAUUSD"],
        date_from=dt.date(2026, 4, 1),
        date_to=dt.date(2026, 4, 1),
        base_clock="M1",
        state_version_ref="state.default@1.0.0",
        feature_selectors=["time/*", "session/*", "quality/*", "entropy/*"],
        label_pack_ref="core_tb_volscaled@1.0.0",
        split_policy="temporal_holdout",
        embargo_rows=1,
        truth_policy_ref="truth.default@1.0.0",
    )
    entropy_spec = get_default_feature_specs()[0].model_copy(
        update={
            "feature_name": "entropy_signal",
            "family": "entropy",
            "output_columns": ["entropy_signal"],
            "missingness_policy": "fail",
        }
    )
    manifest = LineageManifest(
        manifest_id="manifest.dataset.xau_nonhuman.bad",
        artifact_id="dataset.xau_nonhuman.bad",
        artifact_kind="dataset",
        logical_name="xau_nonhuman",
        logical_version="1.0.0",
        artifact_uri="artifact://dataset.xau_nonhuman.bad",
        content_hash="good-hash",
        build_id="build.test",
        created_at=dt.datetime.now(UTC),
        status="truth_pending",
        dataset_spec_ref=spec.key,
        state_artifact_refs=["state.XAUUSD.M1.123"],
        feature_spec_refs=["time.cyclical_time@1.0.0", "session.session_flags@1.0.0", "quality.spread_quality@1.0.0", entropy_spec.key],
        label_pack_ref=get_default_label_packs()[0].key,
        truth_report_ref=None,
        code_version="workspace-local-no-git",
        input_partition_refs=["data/bars/symbol=XAUUSD/timeframe=M1/date=2026-04-01"],
        parent_artifact_refs=["state.XAUUSD.M1.123"],
    )

    paths = StoragePaths(tmp_path / "pipeline_data")
    store = ParquetStore(compression="snappy", row_group_size=1000)
    report = TruthService().evaluate_dataset(
        artifact_id=manifest.artifact_id,
        dataset_df=dataset_df,
        split_frames=split_frames,
        spec=spec,
        feature_specs=[*get_default_feature_specs()[:3], entropy_spec],
        label_pack=get_default_label_packs()[0],
        manifest=manifest,
        paths=paths,
        store=store,
        expected_content_hash="good-hash",
        manifest_path=tmp_path / "manifest.json",
    )

    assert report.status == "rejected"
    assert "feature_family_missingness_threshold_exceeded" in report.hard_failures


def test_truth_reports_informative_warning_reasons_for_source_quality(tmp_path) -> None:
    dataset_df = pl.DataFrame(
        {
            "symbol": ["XAUUSD"] * 4,
            "timeframe": ["M1"] * 4,
            "time_utc": [dt.datetime(2026, 4, 1, 0, i, tzinfo=UTC) for i in range(4)],
            "open": [3000.0, 3000.1, 3000.2, 3000.3],
            "high": [3000.2, 3000.4, 3000.5, 3000.6],
            "low": [2999.9, 3000.0, 3000.1, 3000.2],
            "close": [3000.1, 3000.2, 3000.35, 3000.45],
            "tick_count": [10, 11, 12, 13],
            "spread_mean": [0.10, 0.11, 0.10, 0.12],
            "mid_return": [0.0010, 0.0011, -0.0005, 0.0008],
            "realized_vol": [0.0010, 0.0011, 0.0012, 0.0011],
            "source_count": [2, 1, 2, 1],
            "conflict_count": [0, 1, 0, 1],
            "dual_source_ticks": [8, 7, 9, 8],
            "secondary_present_ticks": [8, 7, 9, 8],
            "dual_source_ratio": [0.80, 0.70, 0.90, 0.75],
            "hour": [0, 0, 0, 0],
            "minute": [0, 1, 2, 3],
            "weekday": [3, 3, 3, 3],
            "time_sin": [0.0, 0.1, 0.2, 0.3],
            "time_cos": [1.0, 0.9, 0.8, 0.7],
            "weekday_sin": [0.1, 0.1, 0.1, 0.1],
            "weekday_cos": [0.9, 0.9, 0.9, 0.9],
            "session_asia": [1, 1, 1, 1],
            "session_london": [0, 0, 0, 0],
            "session_ny": [0, 0, 0, 0],
            "session_overlap": [0, 0, 0, 0],
            "relative_spread": [0.00003, 0.00004, 0.00003, 0.00004],
            "conflict_ratio": [0.00, 0.05, 0.00, 0.05],
            "broker_diversity": [2, 1, 2, 1],
            "future_return_5m": [0.1, -0.1, 0.2, -0.2],
            "direction_5m": [1, 0, 1, 0],
            "triple_barrier_5m": [1, -1, 1, -1],
            "mae_5m": [0.01, 0.02, 0.01, 0.03],
            "mfe_5m": [0.02, 0.01, 0.03, 0.02],
            "future_return_15m": [0.1, -0.1, 0.2, -0.2],
            "direction_15m": [1, 0, 1, 0],
            "triple_barrier_15m": [1, -1, 1, -1],
            "mae_15m": [0.01, 0.02, 0.01, 0.03],
            "mfe_15m": [0.02, 0.01, 0.03, 0.02],
            "future_return_60m": [0.1, -0.1, 0.2, -0.2],
            "direction_60m": [1, 0, 1, 0],
            "triple_barrier_60m": [1, -1, 1, -1],
            "mae_60m": [0.01, 0.02, 0.01, 0.03],
            "mfe_60m": [0.02, 0.01, 0.03, 0.02],
            "future_return_240m": [0.1, -0.1, 0.2, -0.2],
            "direction_240m": [1, 0, 1, 0],
            "triple_barrier_240m": [1, -1, 1, -1],
            "mae_240m": [0.01, 0.02, 0.01, 0.03],
            "mfe_240m": [0.02, 0.01, 0.03, 0.02],
        }
    )
    split_frames = {"train": dataset_df.slice(0, 2), "val": dataset_df.slice(2, 1), "test": dataset_df.slice(3, 1)}
    spec = DatasetSpec(
        dataset_name="xau_core",
        version="1.0.0",
        symbols=["XAUUSD"],
        date_from=dt.date(2026, 4, 1),
        date_to=dt.date(2026, 4, 1),
        base_clock="M1",
        state_version_ref="state.default@1.0.0",
        feature_selectors=["time/*", "session/*", "quality/*"],
        label_pack_ref="core_tb_volscaled@1.0.0",
        split_policy="temporal_holdout",
        embargo_rows=1,
        truth_policy_ref="truth.default@1.0.0",
    )
    manifest = LineageManifest(
        manifest_id="manifest.dataset.xau_core.warning",
        artifact_id="dataset.xau_core.warning",
        artifact_kind="dataset",
        logical_name="xau_core",
        logical_version="1.0.0",
        artifact_uri="artifact://dataset.xau_core.warning",
        content_hash="good-hash",
        build_id="build.test",
        created_at=dt.datetime.now(UTC),
        status="truth_pending",
        dataset_spec_ref=spec.key,
        state_artifact_refs=["state.XAUUSD.M1.123"],
        feature_spec_refs=[feature.key for feature in get_default_feature_specs()[:3]],
        label_pack_ref=get_default_label_packs()[0].key,
        truth_report_ref=None,
        code_version="workspace-local-no-git",
        input_partition_refs=["data/bars/symbol=XAUUSD/timeframe=M1/date=2026-04-01"],
        parent_artifact_refs=["state.XAUUSD.M1.123"],
    )

    state_df = pl.DataFrame(
        {
            "quality_score": [70.0, 70.0, 70.0, 70.0],
            "conflict_flag": [False, False, False, False],
            "trust_flags": [[], [], [], []],
        }
    )

    paths = StoragePaths(tmp_path / "pipeline_data")
    store = ParquetStore(compression="snappy", row_group_size=1000)
    report = TruthService().evaluate_dataset(
        artifact_id=manifest.artifact_id,
        dataset_df=dataset_df,
        split_frames=split_frames,
        spec=spec,
        feature_specs=get_default_feature_specs()[:3],
        label_pack=get_default_label_packs()[0],
        manifest=manifest,
        paths=paths,
        store=store,
        state_df=state_df,
        expected_content_hash="good-hash",
    )

    assert report.status == "accepted"
    assert report.accepted_for_publication is True
    assert report.check_status_counts["warning"] >= 1
    assert any(reason.startswith("source_quality: score 70.00 is below preferred 75.00") for reason in report.warning_reasons)
    assert "source quality is acceptable but below preferred research comfort" not in report.warning_reasons
