"""
Base classes for projections and event handlers.

Projections build read models from domain events. This module provides:
- Projection: Abstract base class for all projections
- SyncProjection: Synchronous base class for projections
- EventHandlerBase: Base class for event handlers
- CheckpointTrackingProjection: Adds checkpoint, retry, and DLQ support
- DeclarativeProjection: Adds @handles decorator support with tenant filtering
- TenantFilter: Type alias for tenant filter parameter
- UnregisteredEventHandling: Type alias for unregistered event handling mode

DatabaseProjection now lives in `eventsource.adapters.sql.projection`.

Projections are a core concept in event sourcing, responsible for
maintaining read models optimized for specific query patterns.
"""

from __future__ import annotations

from eventsource.application.projections.base_checkpoint import (
    CheckpointTrackingProjection,
)
from eventsource.application.projections.base_declarative import (
    DeclarativeProjection,
)
from eventsource.application.projections.base_protocols import (
    EventHandlerBase,
    Projection,
    SyncProjection,
    TenantFilter,
    UnregisteredEventHandling,
)

__all__ = [
    "CheckpointTrackingProjection",
    "DeclarativeProjection",
    "EventHandlerBase",
    "Projection",
    "SyncProjection",
    "TenantFilter",
    "UnregisteredEventHandling",
]
