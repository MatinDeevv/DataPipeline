"""
Features sector — public boundary module.

This is the ONLY module other sectors may import from the features/labels
packages. All cross-sector consumers should import from here, never from
``mt5pipe.features.builder``, ``mt5pipe.features.service``,
``mt5pipe.labels.service``, etc. directly.

Labels are part of the features sector (Agent 2 owns both).
``mt5pipe.labels.public`` is the intra-sector boundary; this module is the
cross-sector boundary for the combined features+labels surface.

Re-exports
----------
FeatureSpec : pydantic model
    Declarative feature contract (from registry).
FeatureService : class
    Materializes registered feature views as compiler artifacts.
LabelPack : pydantic model
    Declarative label family pack (from labels registry).
LabelService : class
    Materializes label packs as compiler artifacts.
add_time_features, add_session_features, add_spread_quality_features,
add_lagged_bar_features : functions
    Facade feature builders.
"""

from mt5pipe.features.builder import (
    add_lagged_bar_features,
    add_session_features,
    add_spread_quality_features,
    add_time_features,
)
from mt5pipe.features.registry.models import FeatureSpec
from mt5pipe.features.service import FeatureService
from mt5pipe.labels.registry.models import LabelPack
from mt5pipe.labels.service import LabelService

__all__ = [
    # Feature models
    "FeatureSpec",
    # Feature services
    "FeatureService",
    # Feature builders
    "add_time_features",
    "add_session_features",
    "add_spread_quality_features",
    "add_lagged_bar_features",
    # Label models
    "LabelPack",
    # Label services
    "LabelService",
]
