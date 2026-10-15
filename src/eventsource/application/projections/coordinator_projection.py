"""Projection coordinator for batch-shaped operations over a ProjectionRegistry."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from eventsource.application.projections.base import EventHandlerBase, Projection
from eventsource.application.projections.coordinator_registry import ProjectionRegistry
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_EVENT_COUNT

logger = logging.getLogger(__name__)


class ProjectionCoordinator:
    """
    Batch-shaped operations over a ``ProjectionRegistry``.

    Accepts either a ``ProjectionRegistry`` or a sequence of ``Projection`` /
    ``EventHandlerBase`` instances directly.
    """

    def __init__(
        self,
        registry: ProjectionRegistry | Sequence[Projection | EventHandlerBase] | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """Initialize the coordinator from a ProjectionRegistry or sequence of projections/handlers."""
        if registry is None:
            self.registry = ProjectionRegistry(tracer=tracer, enable_tracing=enable_tracing)
        elif isinstance(registry, ProjectionRegistry):
            self.registry = registry
        else:
            self.registry = ProjectionRegistry(
                tracer=tracer, enable_tracing=enable_tracing
            ).register(*registry)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

    async def dispatch_events(self, events: list[DomainEvent]) -> int:
        """Dispatch events to all registered projections and return count dispatched."""
        with self._tracer.span(
            "eventsource.projection.coordinate",
            {
                ATTR_EVENT_COUNT: len(events),
                "projection.count": self.registry.get_projection_count(),
            },
        ):
            await self.registry.dispatch_many(events)
            return len(events)

    async def rebuild_all(self, events: list[DomainEvent]) -> int:
        """Reset all registered projections and replay ``events`` in order."""
        logger.warning("Rebuilding all projections", extra={"event_count": len(events)})
        await self.registry.reset_all()
        await self.registry.dispatch_many(events)
        logger.info("Completed rebuilding projections", extra={"event_count": len(events)})
        return len(events)

    async def rebuild_projection(
        self,
        projection: Projection,
        events: list[DomainEvent],
    ) -> int:
        """Reset a single projection and replay ``events`` into it."""
        projection_name = projection.__class__.__name__
        logger.warning(
            "Rebuilding projection %s",
            projection_name,
            extra={"projection": projection_name, "event_count": len(events)},
        )
        await projection.reset()
        for event in events:
            await projection.handle(event)
        logger.info(
            "Completed rebuilding projection %s",
            projection_name,
            extra={"projection": projection_name, "event_count": len(events)},
        )
        return len(events)

    async def catchup(
        self,
        projection: Projection,
        events: list[DomainEvent],
    ) -> int:
        """Process ``events`` into ``projection`` without resetting it."""
        projection_name = projection.__class__.__name__
        logger.info(
            "Catching up projection %s with %d events",
            projection_name,
            len(events),
            extra={"projection": projection_name, "event_count": len(events)},
        )
        for event in events:
            await projection.handle(event)
        return len(events)

    def get_projection_info(self) -> list[dict[str, Any]]:
        """Get information about all registered projections."""
        return [
            {"name": p.__class__.__name__, "type": type(p).__name__}
            for p in self.registry.projections
        ]

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on the projection system."""
        return {
            "status": "healthy",
            "projection_count": self.registry.get_projection_count(),
            "handler_count": self.registry.get_handler_count(),
            "projections": [p.__class__.__name__ for p in self.registry.projections],
            "handlers": [h.__class__.__name__ for h in self.registry.handlers],
        }


__all__ = ["ProjectionCoordinator"]
