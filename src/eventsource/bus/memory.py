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
from typing import Any

from eventsource.bus.interface import (
    EventBus,
    EventHandler,
    EventHandlerFunc,
    EventSubscriber,
)
from eventsource.events.base import DomainEvent

# Optional OpenTelemetry integration
try:
    from opentelemetry import trace

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None

logger = logging.getLogger(__name__)


# Type for normalized handler (always async callable)
HandlerWrapper = tuple[Any, Any]  # (original_handler, normalized_async_handler)


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

    def __init__(self) -> None:
        """Initialize the event bus with empty subscriber registry."""
        # Map of event type -> list of (original_handler, normalized_handler) tuples
        self._subscribers: dict[type[DomainEvent], list[HandlerWrapper]] = defaultdict(list)
        # List of wildcard handlers
        self._all_event_handlers: list[HandlerWrapper] = []
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

    def _normalize_handler(self, handler: EventHandler | EventHandlerFunc) -> HandlerWrapper:
        """
        Normalize a handler to an async callable.

        Args:
            handler: Object with handle() method or callable

        Returns:
            Tuple of (original_handler, async_handler_func)
        """
        # If it's an object with a handle method
        if hasattr(handler, "handle"):
            handle_method = handler.handle
            if asyncio.iscoroutinefunction(handle_method):
                # Already async
                return (handler, handle_method)
            else:
                # Wrap sync method
                async def async_wrapper(event: DomainEvent) -> None:
                    result = handle_method(event)
                    # Handle case where sync method returns a coroutine (shouldn't happen but be safe)
                    if asyncio.iscoroutine(result):
                        await result

                return (handler, async_wrapper)
        # It's a callable (function or lambda)
        elif callable(handler):
            if asyncio.iscoroutinefunction(handler):
                return (handler, handler)
            else:
                # Wrap sync callable - we know handler is callable at this point
                callable_handler = handler

                async def async_callable_wrapper(event: DomainEvent) -> None:
                    result = callable_handler(event)
                    if asyncio.iscoroutine(result):
                        await result

                return (handler, async_callable_wrapper)
        else:
            raise TypeError(
                f"Handler must have a handle() method or be callable, got {type(handler)}"
            )

    def _get_handler_name(self, handler: Any) -> str:
        """Get a descriptive name for a handler for logging."""
        if hasattr(handler, "__class__"):
            return str(handler.__class__.__name__)
        elif hasattr(handler, "__name__"):
            return str(handler.__name__)
        else:
            return repr(handler)

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

        # Create span for event dispatch if OpenTelemetry is available
        if OTEL_AVAILABLE and trace is not None:
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                f"event.dispatch.{event_type.__name__}",
                attributes={
                    "event.type": event_type.__name__,
                    "event.id": str(event.event_id),
                    "event.aggregate_id": str(event.aggregate_id),
                    "event.handler_count": len(handlers),
                },
            ):
                await self._invoke_handlers(handlers, event)
        else:
            await self._invoke_handlers(handlers, event)

    async def _invoke_handlers(self, handlers: list[HandlerWrapper], event: DomainEvent) -> None:
        """
        Invoke all handlers for an event concurrently.

        Args:
            handlers: List of (original_handler, async_handler) tuples
            event: The event to handle
        """
        # Process handlers concurrently but wait for all to complete
        tasks = [self._safe_handle(handler, event) for handler in handlers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_handle(self, handler_wrapper: HandlerWrapper, event: DomainEvent) -> None:
        """
        Safely execute a handler, catching and logging exceptions.

        Args:
            handler_wrapper: Tuple of (original_handler, async_handler)
            event: The event to handle
        """
        original_handler, async_handler = handler_wrapper
        handler_name = self._get_handler_name(original_handler)

        # Create a span for the handler if OpenTelemetry is available
        if OTEL_AVAILABLE and trace is not None:
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                f"event_handler.{handler_name}",
                attributes={
                    "event.type": type(event).__name__,
                    "event.id": str(event.event_id),
                    "event.aggregate_id": str(event.aggregate_id),
                    "handler.name": handler_name,
                },
            ) as span:
                try:
                    await async_handler(event)
                    span.set_attribute("handler.success", True)
                    self._stats["handlers_invoked"] += 1
                    logger.debug(
                        f"Handler {handler_name} processed {type(event).__name__}",
                        extra={
                            "handler": handler_name,
                            "event_type": type(event).__name__,
                            "event_id": str(event.event_id),
                        },
                    )
                except Exception as e:
                    span.set_attribute("handler.success", False)
                    span.record_exception(e)
                    self._stats["handler_errors"] += 1
                    logger.error(
                        f"Handler {handler_name} failed processing {type(event).__name__}: {e}",
                        exc_info=True,
                        extra={
                            "handler": handler_name,
                            "event_type": type(event).__name__,
                            "event_id": str(event.event_id),
                            "error": str(e),
                        },
                    )
        else:
            # Fallback without OpenTelemetry
            try:
                await async_handler(event)
                self._stats["handlers_invoked"] += 1
                logger.debug(
                    f"Handler {handler_name} processed {type(event).__name__}",
                    extra={
                        "handler": handler_name,
                        "event_type": type(event).__name__,
                        "event_id": str(event.event_id),
                    },
                )
            except Exception as e:
                self._stats["handler_errors"] += 1
                logger.error(
                    f"Handler {handler_name} failed processing {type(event).__name__}: {e}",
                    exc_info=True,
                    extra={
                        "handler": handler_name,
                        "event_type": type(event).__name__,
                        "event_id": str(event.event_id),
                        "error": str(e),
                    },
                )

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        Thread-safe: Can be called from any thread.

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable
        """
        wrapper = self._normalize_handler(handler)
        handler_name = self._get_handler_name(handler)

        with self._lock:
            self._subscribers[event_type].append(wrapper)

        logger.info(
            f"Registered handler {handler_name} for {event_type.__name__}",
            extra={
                "handler": handler_name,
                "event_type": event_type.__name__,
            },
        )

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
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
        handler_name = self._get_handler_name(handler)

        with self._lock:
            handlers = self._subscribers.get(event_type, [])
            for i, (orig_handler, _) in enumerate(handlers):
                if orig_handler is handler:
                    handlers.pop(i)
                    logger.info(
                        f"Unsubscribed handler {handler_name} from {event_type.__name__}",
                        extra={
                            "handler": handler_name,
                            "event_type": event_type.__name__,
                        },
                    )
                    return True

        logger.debug(
            f"Handler {handler_name} not found for {event_type.__name__}",
            extra={
                "handler": handler_name,
                "event_type": event_type.__name__,
            },
        )
        return False

    def subscribe_all(self, subscriber: EventSubscriber) -> None:
        """
        Subscribe an EventSubscriber to all its declared event types.

        Args:
            subscriber: The subscriber to register
        """
        event_types = subscriber.subscribed_to()
        for event_type in event_types:
            # EventSubscriber has a handle method compatible with EventHandler
            self.subscribe(event_type, subscriber)

    def subscribe_to_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to all event types (wildcard subscription).

        Thread-safe: Can be called from any thread.

        Args:
            handler: Handler that will receive all events
        """
        wrapper = self._normalize_handler(handler)
        handler_name = self._get_handler_name(handler)

        with self._lock:
            self._all_event_handlers.append(wrapper)

        logger.info(
            f"Registered wildcard handler {handler_name}",
            extra={"handler": handler_name},
        )

    def unsubscribe_from_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from the wildcard subscription.

        Thread-safe: Can be called from any thread.

        Args:
            handler: The handler to remove from wildcard subscriptions

        Returns:
            True if the handler was found and removed, False otherwise
        """
        handler_name = self._get_handler_name(handler)

        with self._lock:
            for i, (orig_handler, _) in enumerate(self._all_event_handlers):
                if orig_handler is handler:
                    self._all_event_handlers.pop(i)
                    logger.info(
                        f"Unsubscribed wildcard handler {handler_name}",
                        extra={"handler": handler_name},
                    )
                    return True

        logger.debug(
            f"Wildcard handler {handler_name} not found",
            extra={"handler": handler_name},
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
