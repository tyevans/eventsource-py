"""In-memory event bus implementation.

This module provides an in-memory event bus for distributing domain events
to registered subscribers within the same process.

Suitable for development, testing, and single-instance deployments.
For distributed deployments, use RedisEventBus instead.
"""

import asyncio
import logging

from eventsource.adapters._bus.base import BaseEventBus
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
)

logger = logging.getLogger(__name__)


class InMemoryEventBus(BaseEventBus):
    """
    In-memory event bus for event distribution.

    This implementation distributes events to handlers within the same process.
    Supports both synchronous (blocking) and asynchronous (fire-and-forget) modes.

    Features:
    - Thread-safe subscription management
    - Support for sync and async handlers
    - Wildcard subscriptions (receive all events)
    - Error isolation (handler failures don't stop other handlers)
    - Optional OpenTelemetry tracing
    - Background task management with proper cleanup

    Example:
        >>> bus = InMemoryEventBus()
        >>> bus.subscribe(OrderCreated, my_handler)
        >>> await bus.publish([OrderCreated(...)])

    Thread Safety:
        - Subscription methods (subscribe, unsubscribe) are thread-safe
        - Publishing should only be called from async context
    """

    def __init__(
        self,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        event_registry: EventRegistry | None = None,
    ) -> None:
        """
        Initialize the event bus with empty subscriber registry.

        Args:
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Ignored if tracer is explicitly provided.
            event_registry: Optional registry for resolving event classes.
        """
        super().__init__(event_registry=event_registry)

        self._stats = {
            "events_published": 0,
            "handlers_invoked": 0,
            "handler_errors": 0,
            "background_tasks_created": 0,
            "background_tasks_completed": 0,
        }

        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """
        Publish events to all registered subscribers.

        Events are processed sequentially to maintain ordering guarantees.
        Handler failures are logged but don't prevent other handlers from running.

        Args:
            events: Events to publish
            background: If True, dispatch events in background without blocking

        Note:
            Background publishing improves API response times but introduces
            eventual consistency - read-after-write may see stale data briefly.
        """
        if not events:
            return

        if background:
            self._track_background(self._publish_all(events))
            self._stats["background_tasks_created"] += 1
            logger.debug(
                f"Scheduled background publishing of {len(events)} event(s)",
                extra={"event_count": len(events)},
            )
        else:
            # Synchronous - wait for all handlers
            await self._publish_all(events)

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        """Callback when a background task completes."""
        super()._on_background_task_done(task)
        self._stats["background_tasks_completed"] += 1

    async def _publish_all(self, events: list[DomainEvent]) -> None:
        """
        Publish all events sequentially.

        Args:
            events: Events to publish
        """
        for event in events:
            await self._dispatch_event(event)
            self._stats["events_published"] += 1

    async def _dispatch_event(self, event: DomainEvent) -> None:
        """
        Dispatch a single event to all matching handlers.

        Args:
            event: The event to dispatch
        """
        event_type = type(event)
        handlers = self._handlers_for(event_type)

        if not handlers:
            logger.debug(
                f"No handlers registered for event type: {event_type.__name__}",
                extra={"event_type": event_type.__name__},
            )
            return

        logger.debug(
            f"Dispatching {event_type.__name__} to {len(handlers)} handler(s)",
            extra={
                "event_type": event_type.__name__,
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "handler_count": len(handlers),
            },
        )

        # Trace event dispatch with dynamic attributes
        with self._tracer.span(
            "eventsource.event_bus.dispatch",
            {
                ATTR_EVENT_TYPE: event_type.__name__,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_HANDLER_COUNT: len(handlers),
            },
        ):
            await self._invoke_handlers(handlers, event)

    async def _invoke_handlers(
        self, handlers: tuple[HandlerAdapter, ...], event: DomainEvent
    ) -> None:
        """
        Invoke all handlers for an event concurrently.

        Args:
            handlers: Tuple of HandlerAdapter instances
            event: The event to handle
        """
        # Process handlers concurrently but wait for all to complete
        tasks = [self._safe_handle(adapter, event) for adapter in handlers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_handle(self, adapter: HandlerAdapter, event: DomainEvent) -> None:
        """
        Safely execute a handler, catching and logging exceptions.

        Args:
            adapter: The HandlerAdapter wrapping the handler
            event: The event to handle
        """
        # Trace handler execution with dynamic attributes and error recording
        with self._tracer.span(
            "eventsource.event_bus.handle",
            {
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_HANDLER_NAME: adapter.name,
            },
        ) as span:
            try:
                await adapter.handle(event)
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                self._stats["handlers_invoked"] += 1
                logger.debug(
                    f"Handler {adapter.name} processed {type(event).__name__}",
                    extra={
                        "handler": adapter.name,
                        "event_type": type(event).__name__,
                        "event_id": str(event.event_id),
                    },
                )
            except Exception as e:
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    span.record_exception(e)
                self._stats["handler_errors"] += 1
                logger.error(
                    f"Handler {adapter.name} failed processing {type(event).__name__}: {e}",
                    exc_info=True,
                    extra={
                        "handler": adapter.name,
                        "event_type": type(event).__name__,
                        "event_id": str(event.event_id),
                        "error": str(e),
                    },
                )

    def get_stats(self) -> dict[str, int]:
        """
        Get statistics about event bus operation.

        Returns:
            Dictionary with counts:
            - events_published: Total events published
            - handlers_invoked: Total successful handler invocations
            - handler_errors: Total handler errors
            - background_tasks_created: Background tasks started
            - background_tasks_completed: Background tasks finished
        """
        return dict(self._stats)

    async def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown the event bus and wait for background tasks to complete.

        Args:
            timeout: Maximum time to wait for tasks to complete in seconds

        Note:
            After shutdown, new publish calls with background=True will still
            create tasks, but those won't be waited for. Call this method
            during application shutdown to ensure all events are processed.
        """
        await self._drain_background(timeout)
        logger.info("Event bus shutdown complete")


__all__ = ["InMemoryEventBus"]
