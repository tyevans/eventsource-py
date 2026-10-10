"""
Registry for managing multiple projections and event handlers.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from eventsource.application.projections.base import EventHandlerBase, Projection
from eventsource.application.projections.coordinator_subscriber_registry import (
    SubscriberRegistry,
)
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
)

logger = logging.getLogger(__name__)


class ProjectionRegistry:
    """
    Registry for managing multiple projections.

    Allows routing events to appropriate projections and handlers.
    Supports concurrent execution for better throughput.
    Optional OpenTelemetry tracing support (disabled by default).

    Example:
        >>> registry = ProjectionRegistry()
        >>> registry.register_projection(order_projection)
        >>> registry.register_projection(inventory_projection)
        >>> registry.register_handler(notification_handler)
        >>>
        >>> # Dispatch events to all projections concurrently
        >>> await registry.dispatch(order_created_event)
        >>>
        >>> # With tracing enabled
        >>> registry = ProjectionRegistry(enable_tracing=True)
        >>>
        >>> # Cap fan-out concurrency for a large registry
        >>> registry = ProjectionRegistry(max_concurrency=16)
    """

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
        max_concurrency: int | None = None,
    ) -> None:
        """
        Initialize the projection registry.

        Args:
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency operations).
                          Ignored if tracer is explicitly provided.
            max_concurrency: Maximum number of projections/handlers dispatched
                          concurrently for a single event. `None` (the
                          default) leaves fan-out uncapped. The bound applies
                          per `dispatch()` call, not across the registry's
                          lifetime, and is enforced with one semaphore owned
                          by this instance -- never construct a new one per
                          call, which multiplies the effective ceiling under
                          concurrent callers.
        """
        self._projections: list[Projection] = []
        self._handlers: list[EventHandlerBase] = []
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

    def register_projection(self, projection: Projection) -> None:
        """
        Register a projection.

        Args:
            projection: The projection to register
        """
        self._projections.append(projection)
        projection_name = projection.__class__.__name__
        logger.info(
            "Registered projection %s",
            projection_name,
            extra={"projection": projection_name},
        )

    def register_handler(self, handler: EventHandlerBase) -> None:
        """
        Register an event handler.

        Args:
            handler: The handler to register
        """
        self._handlers.append(handler)
        handler_name = handler.__class__.__name__
        logger.info(
            "Registered handler %s",
            handler_name,
            extra={"handler": handler_name},
        )

    def unregister_projection(self, projection: Projection) -> bool:
        """
        Unregister a projection.

        Args:
            projection: The projection to unregister

        Returns:
            True if projection was found and removed, False otherwise
        """
        try:
            self._projections.remove(projection)
            logger.info(
                "Unregistered projection %s",
                projection.__class__.__name__,
                extra={"projection": projection.__class__.__name__},
            )
            return True
        except ValueError:
            return False

    def unregister_handler(self, handler: EventHandlerBase) -> bool:
        """
        Unregister an event handler.

        Args:
            handler: The handler to unregister

        Returns:
            True if handler was found and removed, False otherwise
        """
        try:
            self._handlers.remove(handler)
            logger.info(
                "Unregistered handler %s",
                handler.__class__.__name__,
                extra={"handler": handler.__class__.__name__},
            )
            return True
        except ValueError:
            return False

    async def dispatch(self, event: DomainEvent) -> None:
        """
        Dispatch an event to all registered projections and handlers concurrently.

        Uses asyncio.gather() to execute independent projections in parallel,
        improving throughput by 3-5x. Errors in individual projections are
        logged but don't prevent other projections from executing.

        Args:
            event: The event to dispatch
        """
        with self._tracer.span(
            "eventsource.projection.registry.dispatch",
            {
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_EVENT_ID: str(event.event_id),
                "projection.count": len(self._projections),
                "handler.count": len(self._handlers),
            },
        ):
            await self._dispatch_internal(event)

    async def _bounded(self, coro: Any) -> Any:
        """Run `coro` under the fan-out semaphore, if one is configured."""
        if self._semaphore is None:
            return await coro
        async with self._semaphore:
            return await coro

    async def _dispatch_internal(self, event: DomainEvent) -> None:
        """Internal dispatch implementation without tracing context."""
        # Prepare projection tasks
        projection_tasks = []
        for projection in self._projections:
            projection_tasks.append(self._bounded(projection.handle(event)))

        # Prepare handler tasks
        handler_tasks = []
        for handler in self._handlers:
            if handler.can_handle(event):
                handler_tasks.append(self._bounded(handler.handle(event)))

        # Execute all projections and handlers concurrently, up to
        # max_concurrency at a time if configured.
        # return_exceptions=True prevents one failure from stopping others
        all_tasks = projection_tasks + handler_tasks
        if all_tasks:
            results = await asyncio.gather(*all_tasks, return_exceptions=True)

            # Log any errors that occurred
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_type = "projection" if i < len(projection_tasks) else "handler"
                    if i < len(projection_tasks):
                        task_name = self._projections[i].__class__.__name__
                    else:
                        task_name = self._handlers[i - len(projection_tasks)].__class__.__name__
                    logger.error(
                        "Error in %s %s while processing %s: %s",
                        task_type,
                        task_name,
                        type(event).__name__,
                        result,
                        exc_info=result,
                        extra={
                            "event_type": type(event).__name__,
                            "event_id": str(event.event_id),
                            "task_type": task_type,
                            "task_name": task_name,
                        },
                    )

    async def dispatch_many(self, events: list[DomainEvent]) -> None:
        """
        Dispatch multiple events in order.

        Events are dispatched sequentially to maintain ordering guarantees.
        Within each event dispatch, projections run concurrently.

        Args:
            events: List of events to dispatch
        """
        for event in events:
            await self.dispatch(event)

    async def reset_all(self) -> None:
        """
        Reset all registered projections.

        Clears all read model data and checkpoints. Use with caution.
        """
        logger.warning(
            "Resetting all projections",
            extra={"projection_count": len(self._projections)},
        )
        for projection in self._projections:
            await projection.reset()
            logger.info(
                "Reset projection %s",
                projection.__class__.__name__,
                extra={"projection": projection.__class__.__name__},
            )

    @property
    def projections(self) -> list[Projection]:
        """Get list of registered projections."""
        return list(self._projections)

    @property
    def handlers(self) -> list[EventHandlerBase]:
        """Get list of registered handlers."""
        return list(self._handlers)

    def get_projection_count(self) -> int:
        """Get number of registered projections."""
        return len(self._projections)

    def get_handler_count(self) -> int:
        """Get number of registered handlers."""
        return len(self._handlers)


__all__ = ["ProjectionRegistry", "SubscriberRegistry"]
