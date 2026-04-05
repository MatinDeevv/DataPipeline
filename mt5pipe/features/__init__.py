"""Features sector public surface.

For cross-sector imports, prefer ``mt5pipe.features.public``.
This file exposes models and builder functions for intra-package convenience.

Note: FeatureService is NOT exported here to avoid circular imports
(features.service → catalog.sqlite → features.registry.models).
Import FeatureService from ``mt5pipe.features.public`` or directly.
"""

from mt5pipe.features.builder import (
    add_lagged_bar_features,
    add_session_features,
    add_spread_quality_features,
    add_time_features,
)
from mt5pipe.features.registry.models import FeatureSpec

__all__ = [
    "FeatureSpec",
    "add_time_features",
    "add_session_features",
    "add_spread_quality_features",
    "add_lagged_bar_features",
]
