"""
State sector — public boundary module.

This is the ONLY module other sectors may import from the state package.
All cross-sector consumers should import from here, never from
``mt5pipe.state.models`` or ``mt5pipe.state.service`` directly.

Re-exports
----------
StateSnapshot : pydantic model
    Canonical machine-readable market state row.
StateService : class
    Materializes state artifacts from built bars.
"""

from mt5pipe.state.models import StateSnapshot
from mt5pipe.state.service import StateService

__all__ = [
    "StateSnapshot",
    "StateService",
]
