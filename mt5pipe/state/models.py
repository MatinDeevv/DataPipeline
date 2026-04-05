"""Typed models for state artifacts and rolling state windows."""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from mt5pipe.contracts.state import parse_window_size


def _ensure_utc(ts: dt.datetime, field_name: str) -> None:
    if ts.tzinfo is None or ts.utcoffset() != dt.timedelta(0):
        raise ValueError(f"{field_name} must be UTC-aware")


class StateSnapshot(BaseModel):
    """Canonical machine-readable market state at a point in time."""

    schema_version: str = Field(default="2.0.0")
    state_version: str
    snapshot_id: str
    symbol: str
    ts_utc: dt.datetime
    ts_msc: int
    clock: str
    window_start_utc: dt.datetime
    window_end_utc: dt.datetime
    bid: float
    ask: float
    mid: float
    spread: float
    source_primary: str
    source_secondary: str | None = None
    source_count: int
    merge_mode: Literal["single", "best", "conflict"]
    conflict_flag: bool
    disagreement_bps: float | None = None
    spread_disagreement_bps: float | None = None
    broker_a_mid: float | None = None
    broker_b_mid: float | None = None
    broker_a_spread: float | None = None
    broker_b_spread: float | None = None
    primary_staleness_ms: int = 0
    secondary_staleness_ms: int | None = None
    source_offset_ms: int | None = None
    quality_score: float
    source_quality_hint: float | None = None
    expected_observations: int = 1
    observed_observations: int = 1
    missing_observations: int = 0
    window_completeness: float = 1.0
    session_code: Literal["asia", "london", "ny", "overlap", "weekend_closed", "other"]
    event_flags: list[str] = Field(default_factory=list)
    trust_flags: list[str] = Field(default_factory=list)
    provenance_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_snapshot(self) -> "StateSnapshot":
        _ensure_utc(self.ts_utc, "ts_utc")
        _ensure_utc(self.window_start_utc, "window_start_utc")
        _ensure_utc(self.window_end_utc, "window_end_utc")
        if self.bid <= 0 or self.ask <= 0:
            raise ValueError("bid and ask must be positive")
        if self.bid > self.ask:
            raise ValueError("bid must be <= ask")
        if self.source_count < 1:
            raise ValueError("source_count must be >= 1")
        if self.window_start_utc > self.window_end_utc:
            raise ValueError("window_start_utc must be <= window_end_utc")
        if not (self.window_start_utc <= self.ts_utc <= self.window_end_utc):
            raise ValueError("ts_utc must lie within the snapshot window")
        expected_mid = (self.bid + self.ask) / 2.0
        expected_spread = self.ask - self.bid
        if abs(self.mid - expected_mid) > 1e-9:
            raise ValueError("mid must equal (bid + ask) / 2")
        if abs(self.spread - expected_spread) > 1e-9:
            raise ValueError("spread must equal ask - bid")
        if not (0.0 <= self.quality_score <= 100.0):
            raise ValueError("quality_score must be within [0, 100]")
        if self.source_quality_hint is not None and not (0.0 <= self.source_quality_hint <= 100.0):
            raise ValueError("source_quality_hint must be within [0, 100]")
        if self.primary_staleness_ms < 0:
            raise ValueError("primary_staleness_ms must be >= 0")
        if self.secondary_staleness_ms is not None and self.secondary_staleness_ms < 0:
            raise ValueError("secondary_staleness_ms must be >= 0 when provided")
        if self.expected_observations < 1:
            raise ValueError("expected_observations must be >= 1")
        if self.observed_observations < 0:
            raise ValueError("observed_observations must be >= 0")
        if self.missing_observations < 0:
            raise ValueError("missing_observations must be >= 0")
        if self.observed_observations > self.expected_observations:
            raise ValueError("observed_observations must be <= expected_observations")
        if self.missing_observations != (self.expected_observations - self.observed_observations):
            raise ValueError("missing_observations must equal expected_observations - observed_observations")
        if not (0.0 <= self.window_completeness <= 1.0):
            raise ValueError("window_completeness must be within [0, 1]")
        return self


class StateArtifactManifest(BaseModel):
    """State-sector artifact manifest with compiler-compatible core fields."""

    schema_version: str = Field(default="1.0.0")
    manifest_id: str
    artifact_id: str
    artifact_kind: Literal["state", "state_window"]
    logical_name: str
    logical_version: str
    artifact_uri: str
    content_hash: str
    build_id: str
    created_at: dt.datetime
    status: Literal["building", "accepted", "rejected", "published"] = "accepted"
    dataset_spec_ref: str | None = None
    state_artifact_refs: list[str] = Field(default_factory=list)
    feature_spec_refs: list[str] = Field(default_factory=list)
    label_pack_ref: str | None = None
    truth_report_ref: str | None = None
    code_version: str
    merge_config_ref: str | None = None
    input_partition_refs: list[str] = Field(default_factory=list)
    parent_artifact_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_manifest(self) -> "StateArtifactManifest":
        _ensure_utc(self.created_at, "created_at")
        if not self.content_hash:
            raise ValueError("content_hash must not be empty")
        if not self.input_partition_refs:
            raise ValueError("input_partition_refs must not be empty")
        return self


class StateWindowRecord(BaseModel):
    """Machine-native rolling state window anchored at a PIT-safe timestamp."""

    schema_version: str = Field(default="1.0.0")
    state_version: str
    window_id: str
    symbol: str
    clock: str
    anchor_ts_utc: dt.datetime
    anchor_ts_msc: int
    window_size: str
    window_start_utc: dt.datetime
    window_end_utc: dt.datetime
    row_count: int
    expected_row_count: int
    missing_row_count: int
    completeness: float
    source_count_mean: float
    dual_source_ratio_window: float
    quality_score_mean: float
    conflict_count_window: int
    conflict_ratio: float
    disagreement_bps_mean: float | None = None
    staleness_ms_max: int | None = None
    mid_values: list[float] = Field(default_factory=list)
    spread_values: list[float] = Field(default_factory=list)
    mid_return_bps_values: list[float] = Field(default_factory=list)
    source_count_values: list[int] = Field(default_factory=list)
    quality_score_values: list[float] = Field(default_factory=list)
    disagreement_bps_values: list[float | None] = Field(default_factory=list)
    staleness_ms_values: list[int | None] = Field(default_factory=list)
    conflict_flags: list[bool] = Field(default_factory=list)
    source_offset_ms_values: list[int | None] = Field(default_factory=list)
    provenance_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_window(self) -> "StateWindowRecord":
        _ensure_utc(self.anchor_ts_utc, "anchor_ts_utc")
        _ensure_utc(self.window_start_utc, "window_start_utc")
        _ensure_utc(self.window_end_utc, "window_end_utc")
        parse_window_size(self.window_size)
        if self.row_count < 0:
            raise ValueError("row_count must be >= 0")
        if self.expected_row_count < 1:
            raise ValueError("expected_row_count must be >= 1")
        if self.missing_row_count < 0:
            raise ValueError("missing_row_count must be >= 0")
        if self.missing_row_count > self.expected_row_count:
            raise ValueError("missing_row_count must be <= expected_row_count")
        if not (0.0 <= self.completeness <= 1.0):
            raise ValueError("completeness must be within [0, 1]")
        if not (0.0 <= self.conflict_ratio <= 1.0):
            raise ValueError("conflict_ratio must be within [0, 1]")
        if not (0.0 <= self.dual_source_ratio_window <= 1.0):
            raise ValueError("dual_source_ratio_window must be within [0, 1]")
        if self.expected_row_count > 0:
            expected_completeness = (self.expected_row_count - self.missing_row_count) / self.expected_row_count
            if abs(self.completeness - expected_completeness) > 1e-9:
                raise ValueError("completeness must match expected_row_count and missing_row_count")

        series_lengths = {
            len(self.mid_values),
            len(self.spread_values),
            len(self.mid_return_bps_values),
            len(self.source_count_values),
            len(self.quality_score_values),
            len(self.disagreement_bps_values),
            len(self.staleness_ms_values),
            len(self.conflict_flags),
            len(self.source_offset_ms_values),
        }
        if series_lengths != {self.row_count}:
            raise ValueError("All machine-native state window series must align to row_count")
        return self
