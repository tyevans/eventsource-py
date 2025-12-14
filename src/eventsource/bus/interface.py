"""Event bus interface definitions.

This module contains the EventBus abstract base class and related protocols
for publishing and subscribing to domain events.

The event bus decouples event producers from consumers, allowing projections
and other handlers to react to events independently.

Protocol definitions are in eventsource.protocols.
Use `from eventsource.protocols import EventHandler` instead.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from eventsource.events.base import DomainEvent
from eventsource.protocols import (
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

# Type alias for simple function-based handlers
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]


class EventBus(ABC):
    """
    Abstract event bus for publishing and subscribing to domain events.

    The event bus decouples event producers from consumers, allowing
    projections and other handlers to react to events independently.

    Implementations must be thread-safe and support both synchronous
    and asynchronous handlers.

    Tracing Support:
        Implementations SHOULD use the composition-based ``Tracer`` from
        ``eventsource.observability`` to provide standardized OpenTelemetry
        tracing. The standard tracing pattern includes:

        1. **Inject Tracer via composition:**
           ``self._tracer = tracer or create_tracer(__name__, enable_tracing)``

        2. **Accept enable_tracing in constructor:**
           ``def __init__(self, ..., enable_tracing: bool = True): ...``

        3. **Use standard span naming convention:**
           - ``eventsource.event_bus.publish`` - For publish operations
           - ``eventsource.event_bus.dispatch`` - For dispatching to handlers
           - ``eventsource.event_bus.handle`` - For individual handler invocations
           - ``eventsource.event_bus.consume`` - For message consumption (distributed)
           - ``eventsource.event_bus.process`` - For message processing (distributed)

        4. **Use standard attribute constants from eventsource.observability.attributes:**
           - ``ATTR_EVENT_TYPE`` - Event class name
           - ``ATTR_EVENT_ID`` - Unique event identifier
           - ``ATTR_AGGREGATE_ID`` - Aggregate identifier
           - ``ATTR_HANDLER_NAME`` - Handler class/function name
           - ``ATTR_HANDLER_COUNT`` - Number of handlers for event
           - ``ATTR_HANDLER_SUCCESS`` - Whether handler succeeded
           - ``ATTR_MESSAGING_SYSTEM`` - Backend system (e.g., 'redis', 'rabbitmq')
           - ``ATTR_MESSAGING_DESTINATION`` - Queue/stream name

        5. **Create spans with dynamic attributes:**
           Use ``self._tracer.start_span()`` for operations with runtime attributes.

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
        >>> from eventsource.observability import create_tracer
        >>> from eventsource.observability.attributes import (
        ...     ATTR_EVENT_TYPE, ATTR_EVENT_ID, ATTR_HANDLER_NAME
        ... )
        >>>
        >>> class MyEventBus(EventBus):
        ...     def __init__(self, enable_tracing: bool = True):
        ...         self._tracer = create_tracer(__name__, enable_tracing)
        ...         self._enable_tracing = self._tracer.enabled
        ...         self._subscribers = {}
        ...
        ...     async def publish(self, events, background=False):
        ...         with self._tracer.start_span(
        ...             "eventsource.event_bus.publish",
        ...             {"eventsource.event.count": len(events)}
        ...         ):
        ...             # implementation
        ...             pass

    See Also:
        - ``eventsource.observability.Tracer`` - Tracer protocol
        - ``eventsource.observability.create_tracer`` - Tracer factory function
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
        handler: FlexibleEventHandler | EventHandlerFunc,
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
        handler: FlexibleEventHandler | EventHandlerFunc,
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
    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
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
        handler: FlexibleEventHandler | EventHandlerFunc,
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
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from the wildcard subscription.

        Args:
            handler: The handler to remove from wildcard subscriptions

        Returns:
            True if the handler was found and removed, False otherwise
        """
        pass


__all__ = [
    "EventBus",
    "EventHandlerFunc",
]
