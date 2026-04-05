"""State-side artifact references shared across sectors."""

from __future__ import annotations

import datetime as dt
import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from mt5pipe.contracts.artifacts import ArtifactKind, ArtifactRef


_WINDOW_RE = re.compile(r"^(?P<count>\d+)(?P<unit>s|m|h|d)$")


def parse_window_size(value: str) -> dt.timedelta:
    """Parse compact rolling-window sizes such as ``30s`` or ``5m``."""
    match = _WINDOW_RE.fullmatch(value.strip().lower())
    if match is None:
        raise ValueError(f"Unsupported window size '{value}'. Expected formats like 30s, 60s, 5m, 1h.")

    count = int(match.group("count"))
    unit = match.group("unit")
    if count <= 0:
        raise ValueError("Window size count must be positive")
    if unit == "s":
        return dt.timedelta(seconds=count)
    if unit == "m":
        return dt.timedelta(minutes=count)
    if unit == "h":
        return dt.timedelta(hours=count)
    return dt.timedelta(days=count)


class TickArtifactRef(ArtifactRef):
    """Reference to a canonical tick artifact range."""

    kind: Literal[ArtifactKind.CANONICAL_TICK] = ArtifactKind.CANONICAL_TICK
    symbol: str = Field(..., min_length=1)
    date_from: dt.date
    date_to: dt.date
    ts_column: str = Field(default="ts_utc", min_length=1)

    @model_validator(mode="after")
    def validate_dates(self) -> "TickArtifactRef":
        if self.date_from > self.date_to:
            raise ValueError("date_from must be <= date_to")
        return self


class StateArtifactRef(ArtifactRef):
    """Reference to a persisted state artifact range."""

    kind: Literal[ArtifactKind.STATE] = ArtifactKind.STATE
    symbol: str = Field(..., min_length=1)
    clock: str = Field(..., min_length=1)
    state_version: str = Field(..., min_length=1)
    date_from: dt.date
    date_to: dt.date

    @model_validator(mode="after")
    def validate_dates(self) -> "StateArtifactRef":
        if self.date_from > self.date_to:
            raise ValueError("date_from must be <= date_to")
        return self


class StateWindowArtifactRef(ArtifactRef):
    """Reference to a persisted rolling-window state artifact."""

    kind: Literal[ArtifactKind.STATE_WINDOW] = ArtifactKind.STATE_WINDOW
    symbol: str = Field(..., min_length=1)
    clock: str = Field(..., min_length=1)
    state_version: str = Field(..., min_length=1)
    window_size: str = Field(..., min_length=2)
    date_from: dt.date
    date_to: dt.date
    source_artifact_id: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def validate_window(self) -> "StateWindowArtifactRef":
        if self.date_from > self.date_to:
            raise ValueError("date_from must be <= date_to")
        parse_window_size(self.window_size)
        return self


class StateWindowRequest(BaseModel):
    """Typed request for rolling state-window materialization."""

    symbol: str = Field(..., min_length=1)
    clock: str = Field(..., min_length=1)
    state_version: str = Field(..., min_length=1)
    date_from: dt.date
    date_to: dt.date
    window_sizes: list[str] = Field(..., min_length=1)
    anchor_on: Literal["state", "canonical_tick"] = "state"
    include_partial_windows: bool = False

    @model_validator(mode="after")
    def validate_request(self) -> "StateWindowRequest":
        if self.date_from > self.date_to:
            raise ValueError("date_from must be <= date_to")
        if not self.window_sizes:
            raise ValueError("window_sizes must not be empty")
        for size in self.window_sizes:
            parse_window_size(size)
        return self
