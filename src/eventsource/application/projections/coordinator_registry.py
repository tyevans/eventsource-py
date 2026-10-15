"""Registry for managing multiple projections and event handlers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any, Self

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
    """Registry for managing multiple projections and routing events concurrently."""

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
        max_concurrency: int | None = None,
        *,
        projections: Sequence[Projection] | None = None,
        handlers: Sequence[EventHandlerBase] | None = None,
    ) -> None:
        """Initialize the projection registry with optional tracing, concurrency cap, and initial items."""
        self._projections: list[Projection] = []
        self._handlers: list[EventHandlerBase] = []
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None
        if projections:
            for proj in projections:
                self.register_projection(proj)
        if handlers:
            for handler in handlers:
                self.register_handler(handler)

    def register(self, *items: Projection | EventHandlerBase) -> Self:
        """Register one or more projections or event handlers and return ``self``."""
        for item in items:
            if isinstance(item, Projection):
                self.register_projection(item)
            elif isinstance(item, EventHandlerBase):
                self.register_handler(item)
            else:
                raise TypeError(
                    f"Expected Projection or EventHandlerBase, got {type(item).__name__}"
                )
        return self

    def register_projection(self, projection: Projection) -> None:
        """Register a projection."""
        self._projections.append(projection)
        name = projection.__class__.__name__
        logger.info("Registered projection %s", name, extra={"projection": name})

    def register_handler(self, handler: EventHandlerBase) -> None:
        """Register an event handler."""
        self._handlers.append(handler)
        name = handler.__class__.__name__
        logger.info("Registered handler %s", name, extra={"handler": name})

    def unregister_projection(self, projection: Projection) -> bool:
        """Unregister a projection. Returns True if found and removed."""
        try:
            self._projections.remove(projection)
            name = projection.__class__.__name__
            logger.info("Unregistered projection %s", name, extra={"projection": name})
            return True
        except ValueError:
            return False

    def unregister_handler(self, handler: EventHandlerBase) -> bool:
        """Unregister an event handler. Returns True if found and removed."""
        try:
            self._handlers.remove(handler)
            name = handler.__class__.__name__
            logger.info("Unregistered handler %s", name, extra={"handler": name})
            return True
        except ValueError:
            return False

    async def dispatch(self, event: DomainEvent) -> None:
        """Dispatch an event to all registered projections and handlers concurrently."""
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
        """Run ``coro`` under the fan-out semaphore, if one is configured."""
        if self._semaphore is None:
            return await coro
        async with self._semaphore:
            return await coro

    async def _dispatch_internal(self, event: DomainEvent) -> None:
        """Internal dispatch implementation without tracing context."""
        projection_tasks = [self._bounded(p.handle(event)) for p in self._projections]
        handler_tasks = [
            self._bounded(h.handle(event)) for h in self._handlers if h.can_handle(event)
        ]
        all_tasks = projection_tasks + handler_tasks
        if not all_tasks:
            return
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                is_proj = i < len(projection_tasks)
                task_type = "projection" if is_proj else "handler"
                target = (
                    self._projections[i] if is_proj else self._handlers[i - len(projection_tasks)]
                )
                task_name = target.__class__.__name__
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
        """Dispatch multiple events sequentially to maintain ordering guarantees."""
        for event in events:
            await self.dispatch(event)

    async def reset_all(self) -> None:
        """Reset all registered projections."""
        logger.warning(
            "Resetting all projections",
            extra={"projection_count": len(self._projections)},
        )
        for projection in self._projections:
            await projection.reset()
            name = projection.__class__.__name__
            logger.info("Reset projection %s", name, extra={"projection": name})

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
