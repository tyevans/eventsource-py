"""Event publisher port: contract for pushing persisted events outward.

Also home to the ``EventBus`` abstract base class and related protocols
for publishing and subscribing to domain events.

The event bus decouples event producers from consumers, allowing projections
and other handlers to react to events independently.

Protocol definitions are in eventsource.ports.handlers.
Use `from eventsource.ports.handlers import EventHandler` instead.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Protocol

from eventsource.events.base import DomainEvent
from eventsource.ports.handlers import (
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

__all__ = ["EventBus", "EventHandlerFunc", "EventPublisher", "SubscribableEventBus"]

# Type alias for simple function-based handlers
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]


class EventPublisher(Protocol):
    """
    Protocol for publishing events to external systems.

    Event publishers enable downstream consumers to react to events
    asynchronously (e.g., sending notifications, updating search indices).

    This protocol defines the contract that event bus implementations
    and other publishing mechanisms should follow.

    Example:
        >>> class NotificationPublisher:
        ...     async def publish(self, events: list[DomainEvent]) -> None:
        ...         for event in events:
        ...             await send_notification(event)
    """

    async def publish(self, events: list[DomainEvent]) -> None:
        """
        Publish events to external systems.

        Args:
            events: Events to publish

        Raises:
            Exception: If publishing fails (implementation-specific)
        """
        ...


class SubscribableEventBus(Protocol):
    """
    Protocol for the narrow slice of ``EventBus`` that live subscription
    runners depend on: registering and removing a handler for a single
    event type.

    Use cases that only need to (un)subscribe handlers should type-hint
    this port instead of the concrete ``EventBus`` ABC below -- every
    ``EventBus`` implementation satisfies it structurally, with no
    explicit subclassing required.

    Example:
        >>> def wire_runner(bus: SubscribableEventBus) -> None:
        ...     bus.subscribe(OrderCreated, handler)
    """

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable
        """
        ...

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
        ...


class EventBus(ABC):
    """
    Abstract event bus for publishing and subscribing to domain events.

    The event bus decouples event producers from consumers, allowing
    projections and other handlers to react to events independently.

    Subscription management (subscribe, unsubscribe, and their wildcard
    counterparts) is thread-safe in all bundled implementations, which inherit
    it from ``BaseEventBus``. Publishing must be called from an async context.
    Implementations support both sync and async handlers.

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
        - ``BaseEventBus`` - Concrete base providing subscription management,
          background task tracking, and event class resolution. Prefer
          subclassing it over implementing this ABC directly.
        - ``EventBusConformanceSuite`` - Contract tests every implementation
          should subclass.
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
            background: If True, return as soon as the events are handed off,
                       without waiting for delivery to be confirmed or handled.
                       Improves response times at the cost of eventual
                       consistency -- a read immediately after publishing may
                       not observe the event.

                       Backends realize this differently: InMemory dispatches
                       in a background task, Redis defers the stream write,
                       Kafka skips the broker acknowledgment, and RabbitMQ
                       skips the publisher confirm. In every case the event is
                       still delivered.

                       Handler errors during delivery are always isolated per
                       handler -- a failing handler never starves its peers.
                       On broker consume paths (Redis, RabbitMQ, Kafka),
                       errors are additionally aggregated into
                       ``HandlerDispatchError`` and the message's ack is
                       withheld so the broker redelivers it. InMemory has no
                       broker to redeliver from, so its handler errors are
                       logged rather than raised.

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
