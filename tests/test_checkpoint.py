"""Tests for checkpoint resume logic."""

from __future__ import annotations

import datetime as dt

import pytest

from mt5pipe.models.checkpoint import IngestionCheckpoint
from mt5pipe.storage.checkpoint_db import CheckpointDB


class TestCheckpointDB:
    def test_upsert_and_get(self, checkpoint_db: CheckpointDB) -> None:
        cp = IngestionCheckpoint(
            broker_id="broker_a",
            symbol="XAUUSD",
            data_type="ticks",
            last_timestamp_utc=dt.datetime(2024, 1, 15, tzinfo=dt.timezone.utc),
            rows_ingested=1000,
        )
        checkpoint_db.upsert_checkpoint(cp)

        result = checkpoint_db.get_checkpoint("broker_a", "XAUUSD", "ticks")
        assert result is not None
        assert result.broker_id == "broker_a"
        assert result.rows_ingested == 1000
        assert result.last_timestamp_utc.year == 2024

    def test_upsert_overwrites(self, checkpoint_db: CheckpointDB) -> None:
        cp1 = IngestionCheckpoint(
            broker_id="broker_a",
            symbol="XAUUSD",
            data_type="ticks",
            last_timestamp_utc=dt.datetime(2024, 1, 15, tzinfo=dt.timezone.utc),
            rows_ingested=1000,
        )
        checkpoint_db.upsert_checkpoint(cp1)

        cp2 = IngestionCheckpoint(
            broker_id="broker_a",
            symbol="XAUUSD",
            data_type="ticks",
            last_timestamp_utc=dt.datetime(2024, 6, 1, tzinfo=dt.timezone.utc),
            rows_ingested=5000,
        )
        checkpoint_db.upsert_checkpoint(cp2)

        result = checkpoint_db.get_checkpoint("broker_a", "XAUUSD", "ticks")
        assert result is not None
        assert result.rows_ingested == 5000
        assert result.last_timestamp_utc.month == 6

    def test_get_nonexistent_returns_none(self, checkpoint_db: CheckpointDB) -> None:
        result = checkpoint_db.get_checkpoint("nonexistent", "XAUUSD", "ticks")
        assert result is None

    def test_list_checkpoints(self, checkpoint_db: CheckpointDB) -> None:
        for broker in ["broker_a", "broker_b"]:
            cp = IngestionCheckpoint(
                broker_id=broker,
                symbol="XAUUSD",
                data_type="ticks",
                last_timestamp_utc=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                rows_ingested=100,
            )
            checkpoint_db.upsert_checkpoint(cp)

        all_cps = checkpoint_db.list_checkpoints()
        assert len(all_cps) == 2

        broker_a_cps = checkpoint_db.list_checkpoints("broker_a")
        assert len(broker_a_cps) == 1

    def test_different_timeframes(self, checkpoint_db: CheckpointDB) -> None:
        for tf in ["M1", "M5", "H1"]:
            cp = IngestionCheckpoint(
                broker_id="broker_a",
                symbol="XAUUSD",
                data_type="bars",
                timeframe=tf,
                last_timestamp_utc=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                rows_ingested=100,
            )
            checkpoint_db.upsert_checkpoint(cp)

        all_cps = checkpoint_db.list_checkpoints("broker_a")
        assert len(all_cps) == 3


class TestJobLog:
    def test_start_and_finish_job(self, checkpoint_db: CheckpointDB) -> None:
        job_id = checkpoint_db.start_job("backfill_ticks", "broker_a", "XAUUSD")
        assert job_id > 0

        checkpoint_db.finish_job(job_id, "completed", rows_processed=5000)
        # No exception = success
