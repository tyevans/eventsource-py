"""
Projection coordinator and registry facade.

This module re-exports:
- ProjectionRegistry: Manages multiple projections and event routing
- ProjectionCoordinator: Coordinates event distribution from event store to projections
- SubscriberRegistry: Registry for EventSubscriber instances
"""

from __future__ import annotations

from eventsource.application.projections.coordinator_projection import (
    ProjectionCoordinator,
)
from eventsource.application.projections.coordinator_registry import (
    ProjectionRegistry,
    SubscriberRegistry,
)

__all__ = [
    "ProjectionCoordinator",
    "ProjectionRegistry",
    "SubscriberRegistry",
]
