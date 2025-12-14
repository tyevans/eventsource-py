"""In-memory event bus implementation.

This module provides an in-memory event bus for distributing domain events
to registered subscribers within the same process.

Suitable for development, testing, and single-instance deployments.
For distributed deployments, use RedisEventBus instead.
"""

import asyncio
import logging
import threading
from collections import defaultdict

from eventsource.bus.interface import (
    EventBus,
    EventHandlerFunc,
)
from eventsource.events.base import DomainEvent
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
from eventsource.protocols import (
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

logger = logging.getLogger(__name__)


class InMemoryEventBus(EventBus):
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
    ) -> None:
        """
        Initialize the event bus with empty subscriber registry.

        Args:
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Defaults to True for consistency with other components.
                          Ignored if tracer is explicitly provided.
        """
        # Map of event type -> list of HandlerAdapter instances
        self._subscribers: dict[type[DomainEvent], list[HandlerAdapter]] = defaultdict(list)
        # List of wildcard handlers
        self._all_event_handlers: list[HandlerAdapter] = []
        # Lock for thread-safe subscription management
        self._lock = threading.RLock()
        # Track background tasks to prevent orphaned coroutines
        self._background_tasks: set[asyncio.Task[None]] = set()
        # Statistics
        self._stats = {
            "events_published": 0,
            "handlers_invoked": 0,
            "handler_errors": 0,
            "background_tasks_created": 0,
            "background_tasks_completed": 0,
        }

        # Tracing configuration - using composition (replaces TracingMixin)
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
            # Fire and forget - create a background task
            task = asyncio.create_task(self._publish_all(events))
            task.add_done_callback(self._on_background_task_done)
            self._background_tasks.add(task)
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
        self._background_tasks.discard(task)
        self._stats["background_tasks_completed"] += 1

        # Log any unexpected exceptions
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error(
                    f"Background publishing task failed: {exc}",
                    exc_info=exc,
                )

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

        # Gather handlers while holding the lock
        with self._lock:
            specific_handlers = list(self._subscribers.get(event_type, []))
            wildcard_handlers = list(self._all_event_handlers)

        handlers = specific_handlers + wildcard_handlers

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

    async def _invoke_handlers(self, handlers: list[HandlerAdapter], event: DomainEvent) -> None:
        """
        Invoke all handlers for an event concurrently.

        Args:
            handlers: List of HandlerAdapter instances
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

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        Thread-safe: Can be called from any thread.

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable
        """
        adapter = HandlerAdapter(handler)

        with self._lock:
            self._subscribers[event_type].append(adapter)

        logger.info(
            f"Registered handler {adapter.name} for {event_type.__name__}",
            extra={
                "handler": adapter.name,
                "event_type": event_type.__name__,
            },
        )

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from a specific event type.

        Thread-safe: Can be called from any thread.

        Args:
            event_type: The event class to unsubscribe from
            handler: The handler to remove

        Returns:
            True if the handler was found and removed, False otherwise
        """
        # Create adapter for comparison (uses identity comparison on original)
        target_adapter = HandlerAdapter(handler)

        with self._lock:
            adapters = self._subscribers.get(event_type, [])
            for i, adapter in enumerate(adapters):
                if adapter == target_adapter:
                    adapters.pop(i)
                    logger.info(
                        f"Unsubscribed handler {adapter.name} from {event_type.__name__}",
                        extra={
                            "handler": adapter.name,
                            "event_type": event_type.__name__,
                        },
                    )
                    return True

        logger.debug(
            f"Handler {target_adapter.name} not found for {event_type.__name__}",
            extra={
                "handler": target_adapter.name,
                "event_type": event_type.__name__,
            },
        )
        return False

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        """
        Subscribe a FlexibleEventSubscriber to all its declared event types.

        Args:
            subscriber: The subscriber to register
        """
        event_types = subscriber.subscribed_to()
        for event_type in event_types:
            # FlexibleEventSubscriber has a handle method compatible with FlexibleEventHandler
            self.subscribe(event_type, subscriber)

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to all event types (wildcard subscription).

        Thread-safe: Can be called from any thread.

        Args:
            handler: Handler that will receive all events
        """
        adapter = HandlerAdapter(handler)

        with self._lock:
            self._all_event_handlers.append(adapter)

        logger.info(
            f"Registered wildcard handler {adapter.name}",
            extra={"handler": adapter.name},
        )

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from the wildcard subscription.

        Thread-safe: Can be called from any thread.

        Args:
            handler: The handler to remove from wildcard subscriptions

        Returns:
            True if the handler was found and removed, False otherwise
        """
        # Create adapter for comparison (uses identity comparison on original)
        target_adapter = HandlerAdapter(handler)

        with self._lock:
            for i, adapter in enumerate(self._all_event_handlers):
                if adapter == target_adapter:
                    self._all_event_handlers.pop(i)
                    logger.info(
                        f"Unsubscribed wildcard handler {adapter.name}",
                        extra={"handler": adapter.name},
                    )
                    return True

        logger.debug(
            f"Wildcard handler {target_adapter.name} not found",
            extra={"handler": target_adapter.name},
        )
        return False

    def clear_subscribers(self) -> None:
        """
        Clear all subscribers.

        Thread-safe: Can be called from any thread.
        Useful for testing or reinitializing the bus.
        """
        with self._lock:
            self._subscribers.clear()
            self._all_event_handlers.clear()

        logger.info("All event subscribers cleared")

    def get_subscriber_count(self, event_type: type[DomainEvent] | None = None) -> int:
        """
        Get the number of registered subscribers.

        Thread-safe: Can be called from any thread.

        Args:
            event_type: If provided, count subscribers for this event type only.
                       Does not include wildcard subscribers.

        Returns:
            Number of registered subscribers
        """
        with self._lock:
            if event_type is None:
                return sum(len(handlers) for handlers in self._subscribers.values())
            return len(self._subscribers.get(event_type, []))

    def get_wildcard_subscriber_count(self) -> int:
        """
        Get the number of wildcard subscribers.

        Thread-safe: Can be called from any thread.

        Returns:
            Number of wildcard subscribers
        """
        with self._lock:
            return len(self._all_event_handlers)

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

    def get_background_task_count(self) -> int:
        """
        Get the number of currently active background tasks.

        Returns:
            Number of active background tasks
        """
        return len(self._background_tasks)

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
        logger.info(
            f"Shutting down event bus, waiting for {len(self._background_tasks)} background task(s)"
        )

        if not self._background_tasks:
            return

        # Wait for all background tasks to complete
        pending = list(self._background_tasks)
        if pending:
            try:
                done, remaining = await asyncio.wait(
                    pending,
                    timeout=timeout,
                    return_when=asyncio.ALL_COMPLETED,
                )
                if remaining:
                    logger.warning(
                        f"Event bus shutdown: {len(remaining)} task(s) did not complete within timeout",
                        extra={"remaining_tasks": len(remaining)},
                    )
                    # Cancel remaining tasks
                    for task in remaining:
                        task.cancel()
            except Exception as e:
                logger.error(f"Error during event bus shutdown: {e}", exc_info=True)

        logger.info("Event bus shutdown complete")


__all__ = ["InMemoryEventBus"]
