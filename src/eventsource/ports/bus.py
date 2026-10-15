"""Event publisher port and EventBus abstract base class for domain events."""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Protocol

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import FlexibleEventHandler, FlexibleEventSubscriber

__all__ = ["EventBus", "EventHandlerFunc", "EventPublisher", "SubscribableEventBus"]

EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]


class EventPublisher(Protocol):
    """Protocol for publishing persisted events to external systems."""

    async def publish(self, events: list[DomainEvent]) -> None:
        """Publish events to external systems."""
        ...


class SubscribableEventBus(Protocol):
    """Protocol for registering and removing a handler for a single event type."""

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
    """

    @abstractmethod
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """
        Publish events to all registered subscribers.

        Args:
            events: List of events to publish
            background: If True, return as soon as the events are handed off,
                       without waiting for delivery to be confirmed or handled.
        """
        pass

    @abstractmethod
    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to a specific event type."""
        pass

    @abstractmethod
    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from a specific event type; returns True if removed."""
        pass

    @abstractmethod
    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        """Subscribe an EventSubscriber to all its declared event types."""
        pass

    @abstractmethod
    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to all event types (wildcard subscription)."""
        pass

    @abstractmethod
    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from the wildcard subscription; returns True if removed."""
        pass
