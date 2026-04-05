"""
Labels sector — public boundary module.

This is the ONLY module other sectors may import from the labels package.
All cross-sector consumers should import from here, never from
``mt5pipe.labels.service`` or ``mt5pipe.labels.registry.models`` directly.

Re-exports
----------
LabelPack : pydantic model
    Declarative label family pack (from registry).
LabelService : class
    Materializes label packs as compiler artifacts.
"""

from mt5pipe.labels.registry.models import LabelPack
from mt5pipe.labels.service import LabelService

__all__ = [
    "LabelPack",
    "LabelService",
]
