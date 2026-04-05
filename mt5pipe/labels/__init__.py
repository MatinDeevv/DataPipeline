"""Labels sector public surface.

For cross-sector imports, prefer ``mt5pipe.labels.public``.
This file exposes models for intra-package convenience.

Note: LabelService is NOT exported here to avoid circular imports
(labels.service → catalog.sqlite → features.registry.models).
Import LabelService from ``mt5pipe.labels.public`` or directly.
"""

from mt5pipe.labels.registry.models import LabelPack

__all__ = ["LabelPack"]
