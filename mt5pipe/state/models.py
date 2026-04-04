"""Typed contracts for state engine artifacts."""

from __future__ import annotations

import datetime as dt
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class StateSnapshot(BaseModel):
    """Canonical machine-readable market state at a point in time."""

    schema_version: str = Field(default="1.0.0")
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
    quality_score: float
    session_code: Literal["asia", "london", "ny", "overlap", "weekend_closed", "other"]
    event_flags: list[str] = Field(default_factory=list)
    trust_flags: list[str] = Field(default_factory=list)
    provenance_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_snapshot(self) -> "StateSnapshot":
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
        return self
