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

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import (
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

__all__ = ["EventBus", "EventHandlerFunc", "EventPublisher", "SubscribableEventBus"]

# Type alias for simple function-based handlers
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]


class EventPublisher(Protocol):
    """
    Protocol for publishing persisted events to external systems.

    Enables downstream consumers to react to events asynchronously
    (e.g., sending notifications, updating search indices).
    """

    async def publish(self, events: list[DomainEvent]) -> None:
        """Publish events to external systems."""
        ...


class SubscribableEventBus(Protocol):
    """
    Protocol for the narrow slice of ``EventBus`` that live subscription
    runners depend on: registering and removing a handler for a single
    event type.

    Every ``EventBus`` implementation satisfies it structurally.
    """

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to a specific event type."""
        ...

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from a specific event type; returns True if removed."""
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
        tracing:

        1. Inject ``Tracer`` via composition:
           ``self._tracer = tracer or create_tracer(__name__, enable_tracing)``
        2. Accept ``enable_tracing: bool = True`` in constructor.
        3. Use standard span naming convention:
           - ``eventsource.event_bus.publish``
           - ``eventsource.event_bus.dispatch``
           - ``eventsource.event_bus.handle``
           - ``eventsource.event_bus.consume``
           - ``eventsource.event_bus.process``
        4. Use standard attribute constants from ``eventsource.observability.attributes``
           (``ATTR_EVENT_TYPE``, ``ATTR_EVENT_ID``, ``ATTR_AGGREGATE_ID``,
           ``ATTR_HANDLER_NAME``, ``ATTR_HANDLER_COUNT``, ``ATTR_HANDLER_SUCCESS``,
           ``ATTR_MESSAGING_SYSTEM``, ``ATTR_MESSAGING_DESTINATION``).
        5. Distributed event buses (Redis, RabbitMQ, Kafka) should support
           context propagation via ``opentelemetry.propagate.inject/extract``.

    See Also:
        - ``eventsource.observability.Tracer`` / ``create_tracer``
        - ``InMemoryEventBus`` - Reference implementation with tracing
        - ``BaseEventBus`` - Concrete base providing subscription management,
          background task tracking, and event class resolution.
        - ``EventBusConformanceSuite`` - Contract tests for implementations.
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
                       Backends realize this differently (InMemory background task,
                       Redis deferred stream write, Kafka/RabbitMQ skipped broker
                       acknowledgment/confirm).
                       Handler errors during delivery are isolated per handler;
                       on broker consume paths, errors are aggregated into
                       ``HandlerDispatchError`` so the message is redelivered.
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

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable
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
