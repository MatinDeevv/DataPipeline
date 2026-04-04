"""Tests for compiler success, rejection, inspection, and diff behavior."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from mt5pipe.catalog.sqlite import CatalogDB
from mt5pipe.compiler.service import compile_dataset_spec, diff_artifacts, inspect_artifact
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths


UTC = dt.timezone.utc


def _make_time_index(start: dt.datetime, rows: int, step_minutes: int) -> list[dt.datetime]:
    return [start + dt.timedelta(minutes=step_minutes * i) for i in range(rows)]


def _make_bars(start: dt.datetime, rows: int, timeframe: str, step_minutes: int) -> pl.DataFrame:
    times = _make_time_index(start, rows, step_minutes)
    base_open = [3000.0 + i * 0.002 + ((i % 30) - 15) * 0.03 for i in range(rows)]
    base_close = [price + (0.05 if i % 2 == 0 else -0.05) for i, price in enumerate(base_open)]
    base_high = [max(base_open[i], base_close[i]) + 0.05 + (i % 5) * 0.005 for i in range(rows)]
    base_low = [min(base_open[i], base_close[i]) - 0.05 - (i % 5) * 0.005 for i in range(rows)]
    dual_ratio = [0.75 + (i % 4) * 0.02 for i in range(rows)]
    return pl.DataFrame(
        {
            "symbol": ["XAUUSD"] * rows,
            "timeframe": [timeframe] * rows,
            "time_utc": times,
            "open": base_open,
            "high": base_high,
            "low": base_low,
            "close": base_close,
            "tick_count": [12] * rows,
            "bid_open": [base_open[i] - 0.05 for i in range(rows)],
            "ask_open": [base_open[i] + 0.05 for i in range(rows)],
            "bid_close": [base_close[i] - 0.05 for i in range(rows)],
            "ask_close": [base_close[i] + 0.05 for i in range(rows)],
            "spread_mean": [0.1 + (i % 5) * 0.001 for i in range(rows)],
            "spread_max": [0.12 + (i % 5) * 0.001 for i in range(rows)],
            "spread_min": [0.08 + (i % 5) * 0.001 for i in range(rows)],
            "mid_return": [0.0001 + (i % 7) * 0.00001 for i in range(rows)],
            "realized_vol": [0.0005 + (i % 9) * 0.00001 for i in range(rows)],
            "volume_sum": [10.0] * rows,
            "source_count": [1 + (i % 2) for i in range(rows)],
            "conflict_count": [i % 3 for i in range(rows)],
            "dual_source_ticks": [9 + (i % 3) for i in range(rows)],
            "secondary_present_ticks": [9 + (i % 3) for i in range(rows)],
            "dual_source_ratio": dual_ratio,
        }
    )


def _write_bars_by_date(df: pl.DataFrame, paths: StoragePaths, store: ParquetStore, timeframe: str) -> None:
    dated = df.with_columns(pl.col("time_utc").dt.date().alias("_date"))
    for date_val in dated["_date"].unique().sort().to_list():
        day_df = dated.filter(pl.col("_date") == date_val).drop("_date")
        store.write(day_df, paths.built_bars_file("XAUUSD", timeframe, date_val))


def _write_merge_qa(date: dt.date, paths: StoragePaths, store: ParquetStore, dual_source_ratio: float = 0.20) -> None:
    df = pl.DataFrame(
        [
            {
                "time_utc": dt.datetime.combine(date, dt.time(0, 0), tzinfo=UTC),
                "date": date.isoformat(),
                "symbol": "XAUUSD",
                "broker_a_id": "broker_a",
                "broker_b_id": "broker_b",
                "dual_source_ratio": dual_source_ratio,
                "conflicts": 0,
            }
        ]
    )
    store.write(df, paths.merge_qa_file("XAUUSD", date))


def _write_project_config(project_root: Path, storage_root: Path) -> Path:
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "pipeline.yaml"
    config_path.write_text(
        "\n".join(
            [
                "brokers: {}",
                "storage:",
                f"  root: \"{storage_root.as_posix()}\"",
                "  checkpoint_db: \"checkpoints.db\"",
                "  parquet_row_group_size: 1000",
                "  compression: \"snappy\"",
                "dataset:",
                "  base_timeframe: \"M1\"",
                "  context_timeframes:",
                "    - \"M5\"",
                "    - \"M15\"",
                "    - \"H1\"",
                "    - \"H4\"",
                "    - \"D1\"",
                "logging:",
                "  level: \"INFO\"",
                "  json_output: false",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _write_spec(path: Path, *, version: str, selectors: list[str], embargo_rows: int = 240) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                'schema_version: "1.0.0"',
                'dataset_name: "xau_m1_core"',
                f'version: "{version}"',
                'description: "compiler test dataset"',
                'symbols:',
                '  - "XAUUSD"',
                'date_from: "2026-04-01"',
                'date_to: "2026-04-02"',
                'base_clock: "M1"',
                'state_version_ref: "state.default@1.0.0"',
                'feature_selectors:',
                *[f'  - "{selector}"' for selector in selectors],
                'label_pack_ref: "core_tb_volscaled@1.0.0"',
                'filters:',
                '  - "exclude:filled_rows"',
                'split_policy: "temporal_holdout"',
                'train_ratio: 0.70',
                'val_ratio: 0.15',
                'test_ratio: 0.15',
                f"embargo_rows: {embargo_rows}",
                'truth_policy_ref: "truth.default@1.0.0"',
                "publish_on_accept: true",
            ]
        ),
        encoding="utf-8",
    )


def _seed_project_data(storage_root: Path) -> StoragePaths:
    paths = StoragePaths(storage_root)
    store = ParquetStore(compression="snappy", row_group_size=1000)

    _write_bars_by_date(_make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 2000, "M1", 1), paths, store, "M1")
    _write_bars_by_date(_make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 500, "M5", 5), paths, store, "M5")
    _write_bars_by_date(_make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 200, "M15", 15), paths, store, "M15")
    _write_bars_by_date(_make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 48, "H1", 60), paths, store, "H1")
    _write_bars_by_date(_make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 12, "H4", 240), paths, store, "H4")
    _write_bars_by_date(_make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 2, "D1", 24 * 60), paths, store, "D1")
    _write_merge_qa(dt.date(2026, 4, 1), paths, store)
    _write_merge_qa(dt.date(2026, 4, 2), paths, store)
    return paths


def test_compile_dataset_spec_builds_real_artifact_and_supports_inspect_diff(tmp_path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    storage_root = project_root / "local_data" / "pipeline_data"
    storage_root.mkdir(parents=True, exist_ok=True)
    paths = _seed_project_data(storage_root)
    _write_project_config(project_root, storage_root)

    spec_one = project_root / "config" / "datasets" / "xau_m1_core_v1.yaml"
    spec_two = project_root / "config" / "datasets" / "xau_m1_core_v2.yaml"
    _write_spec(spec_one, version="1.0.0", selectors=["time/*", "session/*", "quality/*", "htf_context/*"])
    _write_spec(spec_two, version="1.0.1", selectors=["time/*", "session/*", "quality/*"])

    monkeypatch.chdir(project_root)

    result_one = compile_dataset_spec(spec_one)
    assert result_one.manifest.status == "published"
    assert result_one.trust_report.accepted_for_publication is True
    assert result_one.published_aliases == [
        "dataset://xau_m1_core@1.0.0",
        "dataset://xau_m1_core:latest",
    ]
    assert result_one.status_history == ["building", "truth_pending", "accepted", "published"]
    assert result_one.split_row_counts["train"] > 0
    assert result_one.split_row_counts["val"] > 0
    assert result_one.split_row_counts["test"] > 0
    assert paths.compiler_dataset_dir("xau_m1_core", result_one.artifact_id, "train").exists()

    second_result_same_spec = compile_dataset_spec(spec_one)
    assert second_result_same_spec.artifact_id == result_one.artifact_id

    catalog = CatalogDB(paths.catalog_db_path())
    try:
        artifact = catalog.get_artifact(result_one.artifact_id)
        assert artifact is not None
        assert artifact.status == "published"

        build = catalog.get_build_run(result_one.build_id)
        assert build is not None
        assert build.status == "published"

        stored_spec = catalog.get_dataset_spec(result_one.spec.key)
        assert stored_spec is not None
        assert stored_spec.dataset_name == "xau_m1_core"

        trust_report = catalog.get_trust_report(result_one.artifact_id)
        assert trust_report is not None
        assert trust_report.status == "accepted"

        inputs = catalog.list_artifact_inputs(result_one.artifact_id)
        assert any(record.input_kind == "dataset_spec" for record in inputs)
        assert any(record.input_kind == "feature_spec" for record in inputs)
        assert any(record.input_kind == "label_pack" for record in inputs)
        assert any(record.input_kind == "merge_config" for record in inputs)
    finally:
        catalog.close()

    inspected_alias = inspect_artifact("dataset://xau_m1_core@1.0.0")
    assert inspected_alias.artifact.artifact_id == result_one.artifact_id
    assert inspected_alias.feature_families == ["htf_context", "quality", "session", "time"]
    assert inspected_alias.label_pack is not None
    assert inspected_alias.label_pack.key == "core_tb_volscaled@1.0.0"
    assert inspected_alias.split_row_counts == result_one.split_row_counts
    assert inspected_alias.time_range["start"]
    assert inspected_alias.time_range["end"]

    inspected_by_manifest = inspect_artifact(str(result_one.manifest_path))
    assert inspected_by_manifest.manifest_path == result_one.manifest_path
    assert inspected_by_manifest.trust_score_breakdown["total"] == result_one.trust_report.trust_score_total

    result_two = compile_dataset_spec(spec_two)
    diff = diff_artifacts(result_one.artifact_id, result_two.artifact_id)
    assert diff.diff["logical_version_changed"] is True
    assert "htf_context.standard_context@1.0.0" in diff.diff["feature_spec_refs_removed"]
    assert "H1_open" in diff.diff["schema_columns_removed"]
    assert diff.diff["label_pack_changed"] is False


def test_compile_dataset_spec_rejects_publication_on_truth_failure(tmp_path, monkeypatch) -> None:
    project_root = tmp_path / "project_reject"
    storage_root = project_root / "local_data" / "pipeline_data"
    storage_root.mkdir(parents=True, exist_ok=True)
    paths = _seed_project_data(storage_root)
    _write_project_config(project_root, storage_root)

    rejecting_spec = project_root / "config" / "datasets" / "xau_m1_core_reject.yaml"
    _write_spec(
        rejecting_spec,
        version="2.0.0",
        selectors=["time/*", "session/*", "quality/*", "htf_context/*"],
        embargo_rows=1500,
    )

    monkeypatch.chdir(project_root)

    result = compile_dataset_spec(rejecting_spec)
    assert result.manifest.status == "rejected"
    assert result.trust_report.accepted_for_publication is False
    assert any(status == "rejected" for status in result.status_history)
    assert any("empty_split" in failure for failure in result.trust_report.hard_failures)

    catalog = CatalogDB(paths.catalog_db_path())
    try:
        artifact = catalog.get_artifact(result.artifact_id)
        assert artifact is not None
        assert artifact.status == "rejected"

        aliases = catalog.list_aliases(result.artifact_id)
        assert aliases == []

        build = catalog.get_build_run(result.build_id)
        assert build is not None
        assert build.status == "rejected"
    finally:
        catalog.close()
