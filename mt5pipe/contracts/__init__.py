"""
Shared contracts package — single source of truth for cross-sector types.

All cross-package imports between state/features/compiler sectors
MUST go through this package or through <sector>.public.

Do NOT import sector internals from here.
"""

from mt5pipe.contracts.artifacts import ArtifactRef, ArtifactKind
from mt5pipe.contracts.dataset import DatasetId, DatasetSplitKind
from mt5pipe.contracts.trust import TrustVerdict
from mt5pipe.contracts.lineage import LineageNode

__all__ = [
    "ArtifactRef",
    "ArtifactKind",
    "DatasetId",
    "DatasetSplitKind",
    "TrustVerdict",
    "LineageNode",
]
