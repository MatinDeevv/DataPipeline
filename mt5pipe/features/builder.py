"""Compatibility facade for feature family builders.

Prefer importing from:
- `mt5pipe.features.time`
- `mt5pipe.features.session`
- `mt5pipe.features.quality`
- `mt5pipe.features.context`
"""

from __future__ import annotations

from mt5pipe.features.context import add_lagged_bar_features
from mt5pipe.features.quality import add_spread_quality_features
from mt5pipe.features.session import add_session_features
from mt5pipe.features.time import add_time_features

__all__ = [
    "add_time_features",
    "add_session_features",
    "add_spread_quality_features",
    "add_lagged_bar_features",
]
