"""Typed contracts for the dataset compiler."""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class DatasetSpec(BaseModel):
    """Compiler input spec for a dataset artifact."""

    schema_version: str = Field(default="1.0.0")
    dataset_name: str
    version: str
    description: str | None = None
    symbols: list[str]
    date_from: dt.date
    date_to: dt.date
    base_clock: str
    state_version_ref: str
    feature_selectors: list[str]
    label_pack_ref: str
    filters: list[str] = Field(default_factory=list)
    split_policy: Literal["temporal_holdout", "walk_forward"]
    train_ratio: float = 0.70
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    embargo_rows: int
    n_walk_forward_splits: int | None = None
    truth_policy_ref: str
    publish_on_accept: bool = True
    tags: list[str] = Field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.dataset_name}@{self.version}"

    @model_validator(mode="after")
    def validate_spec(self) -> "DatasetSpec":
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        if self.date_from > self.date_to:
            raise ValueError("date_from must be <= date_to")
        if not self.feature_selectors:
            raise ValueError("feature_selectors must not be empty")
        if self.embargo_rows <= 0:
            raise ValueError("embargo_rows must be > 0")
        if self.split_policy == "temporal_holdout":
            total = self.train_ratio + self.val_ratio + self.test_ratio
            if abs(total - 1.0) > 1e-9:
                raise ValueError("train_ratio + val_ratio + test_ratio must equal 1.0")
        if self.split_policy == "walk_forward" and not self.n_walk_forward_splits:
            raise ValueError("n_walk_forward_splits is required for walk_forward specs")
        return self


class LineageManifest(BaseModel):
    """Immutable lineage record for a compiled artifact."""

    schema_version: str = Field(default="1.0.0")
    manifest_id: str
    artifact_id: str
    artifact_kind: Literal["state", "feature_view", "label_view", "dataset"]
    logical_name: str
    logical_version: str
    artifact_uri: str
    content_hash: str
    build_id: str
    created_at: dt.datetime
    status: Literal["building", "accepted", "rejected", "published", "superseded"]
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
    def validate_manifest(self) -> "LineageManifest":
        if not self.content_hash:
            raise ValueError("content_hash must not be empty")
        if not self.input_partition_refs:
            raise ValueError("input_partition_refs must not be empty")
        if self.status == "published" and not self.truth_report_ref:
            raise ValueError("published manifests must reference a truth report")
        return self
