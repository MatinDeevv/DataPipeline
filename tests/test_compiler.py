"""Tests for the Phase 1 dataset compiler scaffold."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from mt5pipe.catalog.sqlite import CatalogDB
from mt5pipe.compiler.service import DatasetCompiler
from mt5pipe.config.models import DatasetConfig, LoggingConfig, MergeConfig, PipelineConfig, StorageConfig
from mt5pipe.storage.parquet_store import ParquetStore
from mt5pipe.storage.paths import StoragePaths


UTC = dt.timezone.utc


def _make_bars(start: dt.datetime, rows: int) -> pl.DataFrame:
    times = [start + dt.timedelta(minutes=i) for i in range(rows)]
    base_open = [3000.0 + i * 0.002 + ((i % 30) - 15) * 0.03 for i in range(rows)]
    base_close = [price + (0.05 if i % 2 == 0 else -0.05) for i, price in enumerate(base_open)]
    base_high = [max(base_open[i], base_close[i]) + 0.05 + (i % 5) * 0.005 for i in range(rows)]
    base_low = [min(base_open[i], base_close[i]) - 0.05 - (i % 5) * 0.005 for i in range(rows)]
    return pl.DataFrame({
        "symbol": ["XAUUSD"] * rows,
        "timeframe": ["M1"] * rows,
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
        "dual_source_ratio": [0.75 + (i % 4) * 0.02 for i in range(rows)],
    })


def _write_bars_by_date(df: pl.DataFrame, paths: StoragePaths, store: ParquetStore) -> None:
    df = df.with_columns(pl.col("time_utc").dt.date().alias("_date"))
    for date_val in df["_date"].unique().sort().to_list():
        day_df = df.filter(pl.col("_date") == date_val).drop("_date")
        store.write(day_df, paths.built_bars_file("XAUUSD", "M1", date_val))


def _write_merge_qa(date: dt.date, paths: StoragePaths, store: ParquetStore) -> None:
    df = pl.DataFrame([{
        "time_utc": dt.datetime.combine(date, dt.time(0, 0), tzinfo=UTC),
        "date": date.isoformat(),
        "symbol": "XAUUSD",
        "broker_a_id": "broker_a",
        "broker_b_id": "broker_b",
        "dual_source_ratio": 0.20,
        "conflicts": 0,
    }])
    store.write(df, paths.merge_qa_file("XAUUSD", date))


def _make_cfg(tmp_data_dir: Path) -> PipelineConfig:
    return PipelineConfig(
        brokers={},
        storage=StorageConfig(root=tmp_data_dir),
        dataset=DatasetConfig(),
        merge=MergeConfig(),
        logging=LoggingConfig(level="INFO", json_output=False),
    )


def _write_spec(path: Path, *, version: str, selectors: list[str]) -> None:
    path.write_text(
        "\n".join([
            'schema_version: "1.0.0"',
            'dataset_name: "xau_core"',
            f'version: "{version}"',
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
            'embargo_rows: 240',
            'truth_policy_ref: "truth.default@1.0.0"',
            'publish_on_accept: true',
        ]),
        encoding="utf-8",
    )


def test_compiler_builds_published_artifact_and_supports_inspect_diff(tmp_data_dir, paths, store) -> None:
    bars = _make_bars(dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC), 2000)
    _write_bars_by_date(bars, paths, store)
    _write_merge_qa(dt.date(2026, 4, 1), paths, store)
    _write_merge_qa(dt.date(2026, 4, 2), paths, store)

    cfg = _make_cfg(tmp_data_dir)
    catalog = CatalogDB(paths.catalog_db_path())
    compiler = DatasetCompiler(cfg, paths, store, catalog)

    spec_one = tmp_data_dir / "spec_one.yaml"
    spec_two = tmp_data_dir / "spec_two.yaml"
    _write_spec(spec_one, version="1.0.0", selectors=["time/*", "session/*", "quality/*"])
    _write_spec(spec_two, version="1.0.1", selectors=["time/*", "session/*"])

    try:
        result_one = compiler.compile_dataset(spec_one)
        assert result_one.manifest.status == "published"
        assert result_one.trust_report.accepted_for_publication is True
        assert len(result_one.manifest.state_artifact_refs) == 1
        assert len(result_one.manifest.parent_artifact_refs) == 5
        assert result_one.split_row_counts["train"] > 0
        assert result_one.split_row_counts["val"] > 0
        assert result_one.split_row_counts["test"] > 0
        assert paths.compiler_dataset_dir("xau_core", result_one.artifact_id, "train").exists()
        assert paths.state_dir("XAUUSD", "M1", dt.date(2026, 4, 1), "state.default@1.0.0").exists()
        assert paths.feature_view_dir("time.cyclical_time@1.0.0", "M1", dt.date(2026, 4, 1)).exists()
        assert paths.feature_view_dir("session.session_flags@1.0.0", "M1", dt.date(2026, 4, 1)).exists()
        assert paths.feature_view_dir("quality.spread_quality@1.0.0", "M1", dt.date(2026, 4, 1)).exists()
        assert paths.label_view_dir("core_tb_volscaled@1.0.0", "M1", dt.date(2026, 4, 1)).exists()

        state_artifact = catalog.get_artifact(result_one.manifest.state_artifact_refs[0])
        assert state_artifact is not None
        assert state_artifact.artifact_kind == "state"

        inspected = compiler.inspect_dataset("dataset://xau_core@1.0.0")
        assert inspected.manifest.artifact_id == result_one.artifact_id
        assert inspected.trust_report is not None
        assert inspected.trust_report.status == "accepted"

        result_two = compiler.compile_dataset(spec_two)
        diff = compiler.diff_datasets(result_one.artifact_id, result_two.artifact_id)
        assert diff.diff["logical_version_changed"] is True
        assert any("quality.spread_quality@1.0.0" == ref for ref in diff.diff["feature_spec_refs_removed"])
        assert "relative_spread" in diff.diff["schema_columns_removed"]
    finally:
        catalog.close()
