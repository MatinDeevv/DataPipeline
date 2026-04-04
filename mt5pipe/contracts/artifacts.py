"""
Artifact reference contracts.

These types are the shared vocabulary for referencing artifacts across sectors.
State, features, and compiler all use ArtifactRef to point at artifacts
without depending on each other's internals.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ArtifactKind(str, Enum):
    """Artifact taxonomy — matches existing LineageManifest.artifact_kind values."""

    STATE = "state"
    FEATURE_VIEW = "feature_view"
    LABEL_VIEW = "label_view"
    DATASET = "dataset"


class ArtifactRef(BaseModel):
    """
    Lightweight, immutable reference to a pipeline artifact.

    Used in cross-sector communication so that sectors can refer to
    artifacts produced by other sectors without importing their internals.

    The ``artifact_id`` format follows the existing convention:
        ``{kind}.{logical_name}.{content_hash[:12]}``
    """

    artifact_id: str = Field(..., min_length=1, description="Unique artifact identifier")
    kind: ArtifactKind
    logical_name: str = Field(..., min_length=1)
    version: Optional[str] = None
    content_hash: Optional[str] = Field(
        None, description="SHA-256 prefix for integrity verification"
    )

    def __str__(self) -> str:
        return self.artifact_id
