"""Typed models for compiler catalog records."""

from __future__ import annotations

import datetime as dt
from typing import Literal

from pydantic import BaseModel


class BuildRunRecord(BaseModel):
    build_id: str
    dataset_spec_key: str
    status: str
    code_version: str
    started_at: dt.datetime
    finished_at: dt.datetime | None = None
    error_message: str = ""
    artifact_id: str | None = None


class ArtifactRecord(BaseModel):
    artifact_id: str
    artifact_kind: Literal["state", "feature_view", "label_view", "dataset"]
    logical_name: str
    logical_version: str
    artifact_uri: str
    manifest_uri: str
    content_hash: str
    status: str
    build_id: str
    created_at: dt.datetime
