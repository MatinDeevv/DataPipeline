"""
Features sector — public boundary module.

This is the ONLY module other sectors may import from the features package.
All cross-sector consumers should import from here, never from
``mt5pipe.features.builder``, ``mt5pipe.features.service``, etc. directly.

Re-exports
----------
FeatureSpec : pydantic model
    Declarative feature contract (from registry).
FeatureService : class
    Materializes registered feature views as compiler artifacts.
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

__all__ = [
    # Models
    "FeatureSpec",
    # Services
    "FeatureService",
    # Builders
    "add_time_features",
    "add_session_features",
    "add_spread_quality_features",
    "add_lagged_bar_features",
]
