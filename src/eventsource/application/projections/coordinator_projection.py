"""
Projection coordinator for batch-shaped operations over a ProjectionRegistry.
"""

from __future__ import annotations

import logging
from typing import Any

from eventsource.application.projections.base import Projection
from eventsource.application.projections.coordinator_registry import ProjectionRegistry
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_EVENT_COUNT

logger = logging.getLogger(__name__)


class ProjectionCoordinator:
    """
    Batch-shaped operations over a ProjectionRegistry.

    The coordinator does not poll or subscribe to anything itself -- it has
    no event bus connection and no background task. It is driven by an
    external caller (a subscription runner, `replay()`, or application code)
    that already has events in hand and wants them dispatched, rebuilt, or
    caught up against the registered projections. Concretely, it adds:

    1. `dispatch_events` -- dispatch a batch of events to the registry
    2. `rebuild_all` / `rebuild_projection` -- rebuild by replaying events
    3. `catchup` -- process events since a checkpoint without resetting
    4. `health_check` / `get_projection_info` -- introspection
    5. Optional OpenTelemetry tracing support (disabled by default)

    Live polling and catch-up subscriptions live in
    `application/subscriptions/`; rebuilding a projection from the global
    feed on its own is `replay()` in `application/projections/replay.py`.

    Example:
        >>> coordinator = ProjectionCoordinator(registry=registry)
        >>> await coordinator.dispatch_events(events)
        >>> await coordinator.rebuild_projection(order_projection, events)
        >>>
        >>> # With tracing enabled
        >>> coordinator = ProjectionCoordinator(registry=registry, enable_tracing=True)
    """

    def __init__(
        self,
        registry: ProjectionRegistry,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the coordinator.

        Args:
            registry: Registry containing projections
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency operations).
                          Ignored if tracer is explicitly provided.
        """
        self.registry = registry
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

    async def dispatch_events(self, events: list[DomainEvent]) -> int:
        """
        Dispatch events to all registered projections.

        Args:
            events: Events to dispatch

        Returns:
            Number of events dispatched
        """
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
        """
        Rebuild all projections by replaying events.

        WARNING: This will clear all read model data and replay all events.
        Use with caution in production.

        Args:
            events: All events to replay (in chronological order)

        Returns:
            Number of events replayed
        """
        logger.warning(
            "Rebuilding all projections",
            extra={"event_count": len(events)},
        )

        # Reset all projections
        await self.registry.reset_all()

        # Replay events
        await self.registry.dispatch_many(events)

        logger.info(
            "Completed rebuilding projections",
            extra={"event_count": len(events)},
        )

        return len(events)

    async def rebuild_projection(
        self,
        projection: Projection,
        events: list[DomainEvent],
    ) -> int:
        """
        Rebuild a single projection by replaying events.

        Args:
            projection: The projection to rebuild
            events: Events to replay (should be filtered to only those
                   the projection handles)

        Returns:
            Number of events replayed
        """
        projection_name = projection.__class__.__name__
        logger.warning(
            "Rebuilding projection %s",
            projection_name,
            extra={
                "projection": projection_name,
                "event_count": len(events),
            },
        )

        # Reset just this projection
        await projection.reset()

        # Replay events to just this projection
        for event in events:
            await projection.handle(event)

        logger.info(
            "Completed rebuilding projection %s",
            projection_name,
            extra={
                "projection": projection_name,
                "event_count": len(events),
            },
        )

        return len(events)

    async def catchup(
        self,
        projection: Projection,
        events: list[DomainEvent],
    ) -> int:
        """
        Catch up a projection that fell behind.

        Unlike rebuild, this doesn't reset the projection - it just
        processes the missing events.

        Args:
            projection: The projection to catch up
            events: Events since the last checkpoint

        Returns:
            Number of events processed
        """
        projection_name = projection.__class__.__name__
        logger.info(
            "Catching up projection %s with %d events",
            projection_name,
            len(events),
            extra={
                "projection": projection_name,
                "event_count": len(events),
            },
        )

        for event in events:
            await projection.handle(event)

        return len(events)

    def get_projection_info(self) -> list[dict[str, Any]]:
        """
        Get information about all registered projections.

        Returns:
            List of projection info dictionaries
        """
        return [
            {
                "name": p.__class__.__name__,
                "type": type(p).__name__,
            }
            for p in self.registry.projections
        ]

    async def health_check(self) -> dict[str, Any]:
        """
        Perform health check on projection system.

        Returns:
            Dictionary with health status
        """
        projection_names = [p.__class__.__name__ for p in self.registry.projections]
        handler_names = [h.__class__.__name__ for h in self.registry.handlers]

        return {
            "status": "healthy",
            "projection_count": self.registry.get_projection_count(),
            "handler_count": self.registry.get_handler_count(),
            "projections": projection_names,
            "handlers": handler_names,
        }


__all__ = ["ProjectionCoordinator"]
