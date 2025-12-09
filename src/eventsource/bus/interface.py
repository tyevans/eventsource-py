"""Event bus interface definitions.

This module contains the EventBus abstract base class and related protocols
for publishing and subscribing to domain events.

The event bus decouples event producers from consumers, allowing projections
and other handlers to react to events independently.

Note: Protocol definitions (EventHandler, EventSubscriber) have been moved to
eventsource.protocols. Imports from this module still work but emit deprecation
warnings. Use `from eventsource.protocols import EventHandler` instead.
"""

import warnings
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from eventsource.events.base import DomainEvent

# Import canonical protocols for re-export
from eventsource.protocols import (
    FlexibleEventHandler as _FlexibleEventHandler,
)
from eventsource.protocols import (
    FlexibleEventSubscriber as _FlexibleEventSubscriber,
)

# Type alias for simple function-based handlers
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]


def __getattr__(name: str) -> type:
    """
    Handle deprecated imports with warnings.

    This enables deprecation warnings when importing EventHandler or
    EventSubscriber from this module.
    """
    if name == "EventHandler":
        warnings.warn(
            "Importing 'EventHandler' from eventsource.bus.interface is deprecated. "
            "Use 'from eventsource.protocols import FlexibleEventHandler' instead "
            "(or EventHandler for async-only handlers). "
            "This import will be removed in version 0.3.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _FlexibleEventHandler
    elif name == "EventSubscriber":
        warnings.warn(
            "Importing 'EventSubscriber' from eventsource.bus.interface is deprecated. "
            "Use 'from eventsource.protocols import EventSubscriber' instead. "
            "This import will be removed in version 0.3.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _FlexibleEventSubscriber
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# For internal use without deprecation warnings (used by bus implementations)
# These are the flexible versions that support both sync and async
EventHandler = _FlexibleEventHandler
EventSubscriber = _FlexibleEventSubscriber


class EventBus(ABC):
    """
    Abstract event bus for publishing and subscribing to domain events.

    The event bus decouples event producers from consumers, allowing
    projections and other handlers to react to events independently.

    Implementations must be thread-safe and support both synchronous
    and asynchronous handlers.

    Tracing Support:
        Implementations SHOULD inherit from ``TracingMixin`` from
        ``eventsource.observability`` to provide standardized OpenTelemetry
        tracing. The standard tracing pattern includes:

        1. **Inherit from TracingMixin:**
           ``class MyEventBus(TracingMixin, EventBus): ...``

        2. **Accept enable_tracing in constructor:**
           ``def __init__(self, ..., enable_tracing: bool = True): ...``

        3. **Initialize tracing in constructor:**
           ``self._init_tracing(__name__, enable_tracing)``

        4. **Use standard span naming convention:**
           - ``eventsource.event_bus.publish`` - For publish operations
           - ``eventsource.event_bus.dispatch`` - For dispatching to handlers
           - ``eventsource.event_bus.handle`` - For individual handler invocations
           - ``eventsource.event_bus.consume`` - For message consumption (distributed)
           - ``eventsource.event_bus.process`` - For message processing (distributed)

        5. **Use standard attribute constants from eventsource.observability.attributes:**
           - ``ATTR_EVENT_TYPE`` - Event class name
           - ``ATTR_EVENT_ID`` - Unique event identifier
           - ``ATTR_AGGREGATE_ID`` - Aggregate identifier
           - ``ATTR_HANDLER_NAME`` - Handler class/function name
           - ``ATTR_HANDLER_COUNT`` - Number of handlers for event
           - ``ATTR_HANDLER_SUCCESS`` - Whether handler succeeded
           - ``ATTR_MESSAGING_SYSTEM`` - Backend system (e.g., 'redis', 'rabbitmq')
           - ``ATTR_MESSAGING_DESTINATION`` - Queue/stream name

        6. **Create spans with dynamic attributes:**
           Use ``self._create_span_context()`` for operations with runtime attributes.

        Distributed event buses (Redis, RabbitMQ, Kafka) should additionally:
        - Inject trace context into message headers on publish
        - Extract trace context from message headers on consume
        - Use ``opentelemetry.propagate.inject/extract`` for context propagation

    Example:
        >>> event_bus = InMemoryEventBus()
        >>> event_bus.subscribe(OrderCreated, order_handler)
        >>> event_bus.subscribe_all(order_projection)
        >>> await event_bus.publish([OrderCreated(...)])

    Example Traced Implementation:
        >>> from eventsource.observability import TracingMixin
        >>> from eventsource.observability.attributes import (
        ...     ATTR_EVENT_TYPE, ATTR_EVENT_ID, ATTR_HANDLER_NAME
        ... )
        >>>
        >>> class MyEventBus(TracingMixin, EventBus):
        ...     def __init__(self, enable_tracing: bool = True):
        ...         self._init_tracing(__name__, enable_tracing)
        ...         self._subscribers = {}
        ...
        ...     async def publish(self, events, background=False):
        ...         with self._create_span_context(
        ...             "eventsource.event_bus.publish",
        ...             {"eventsource.event.count": len(events)}
        ...         ):
        ...             # implementation
        ...             pass

    See Also:
        - ``eventsource.observability.TracingMixin`` - Tracing mixin class
        - ``eventsource.observability.attributes`` - Standard attribute constants
        - ``InMemoryEventBus`` - Reference implementation with tracing
    """

    @abstractmethod
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """
        Publish events to all registered subscribers.

        Events are processed in order, and all handlers for each event
        are invoked before moving to the next event.

        Args:
            events: List of events to publish
            background: If True, publish asynchronously without blocking
                       (fire-and-forget). This improves response times but
                       introduces eventual consistency.

        Raises:
            Exception: If publishing fails critically (only in synchronous mode)

        Note:
            Handler errors are caught and logged but don't prevent other
            handlers from executing.
        """
        pass

    @abstractmethod
    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: _FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        The handler will be invoked whenever an event of the specified type
        is published. Multiple handlers can be subscribed to the same event type.

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable

        Example:
            >>> event_bus.subscribe(OrderCreated, order_handler)
            >>> event_bus.subscribe(OrderCreated, lambda e: print(e))
        """
        pass

    @abstractmethod
    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: _FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from a specific event type.

        Args:
            event_type: The event class to unsubscribe from
            handler: The handler to remove

        Returns:
            True if the handler was found and removed, False otherwise
        """
        pass

    @abstractmethod
    def subscribe_all(self, subscriber: _FlexibleEventSubscriber) -> None:
        """
        Subscribe an EventSubscriber to all its declared event types.

        This is a convenience method that calls subscribe() for each
        event type returned by subscriber.subscribed_to().

        Args:
            subscriber: The subscriber to register

        Example:
            >>> event_bus.subscribe_all(order_projection)
        """
        pass

    @abstractmethod
    def subscribe_to_all_events(
        self,
        handler: _FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to all event types (wildcard subscription).

        The handler will receive every event published to the bus,
        regardless of event type. Useful for audit logging, metrics,
        debugging, or other cross-cutting concerns.

        Args:
            handler: Handler that will receive all events

        Example:
            >>> event_bus.subscribe_to_all_events(audit_logger)
        """
        pass

    @abstractmethod
    def unsubscribe_from_all_events(
        self,
        handler: _FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from the wildcard subscription.

        Args:
            handler: The handler to remove from wildcard subscriptions

        Returns:
            True if the handler was found and removed, False otherwise
        """
        pass


class AsyncEventHandler(ABC):
    """
    Base class for async event handlers.

    Provides a more structured approach than the EventHandler protocol
    for handlers that need additional methods like event filtering.

    Example:
        >>> class OrderEmailHandler(AsyncEventHandler):
        ...     def event_types(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self.send_confirmation_email(event)
    """

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle an event asynchronously.

        Args:
            event: The event to handle

        Raises:
            Exception: If handling fails
        """
        pass

    def can_handle(self, event: DomainEvent) -> bool:
        """
        Check if this handler can handle the given event.

        By default, checks if event type is in the handler's event types.
        Override for custom filtering logic.

        Args:
            event: The event to check

        Returns:
            True if this handler can handle the event
        """
        return type(event) in self.event_types()

    @abstractmethod
    def event_types(self) -> list[type[DomainEvent]]:
        """
        Return list of event types this handler supports.

        Returns:
            List of event classes this handler can process
        """
        pass


__all__ = [
    "EventBus",
    "EventHandler",
    "EventSubscriber",
    "EventHandlerFunc",
    "AsyncEventHandler",
]
