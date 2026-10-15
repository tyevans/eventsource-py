"""
Conformance test suite for EventBus implementations.

Provides a base test class that verifies backend implementations conform to the
EventBus contract. All test methods are async (use pytest with asyncio_mode="auto").
"""

from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID, uuid4

from eventsource.domain.event import DomainEvent
from eventsource.ports.bus import EventBus


class EventBusConformanceSuite(ABC):
    """
    Base test suite for EventBus implementations.

    Subclasses must implement ``create_bus``, ``create_test_event``, and
    ``create_subscriber`` factory methods to verify the EventBus contract:
    publish/subscribe roundtrip, multiple subscribers, unsubscribe,
    ``subscribe_all``, wildcard subscriptions, handler error isolation,
    background delivery, and per-aggregate ordering.
    """

    @abstractmethod
    def create_bus(self) -> EventBus:
        """Create a fresh, empty EventBus instance for testing."""
        pass

    @abstractmethod
    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        """Create a test event with the given aggregate_id."""
        pass

    @abstractmethod
    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        """
        Create an EventSubscriber that appends handled events to ``received``.

        The subscriber's ``subscribed_to()`` must return the type produced by
        ``create_test_event``.
        """
        pass

    async def await_delivery(self, bus: EventBus) -> None:
        """
        Wait for in-flight deliveries to land.

        Defaults to a no-op for in-process buses; distributed backends override
        this with a bounded poll or consumer drain.
        """
        return None

    async def test_publish_and_subscribe_roundtrip(self) -> None:
        """Verify that events can be published and received by subscribers."""
        bus = self.create_bus()
        event = self.create_test_event(uuid4())
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(event), handler)
        await bus.publish([event])

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id

    async def test_multiple_subscribers(self) -> None:
        """Verify that multiple subscribers receive the same event."""
        bus = self.create_bus()
        event = self.create_test_event(uuid4())
        received_by_handler1: list[DomainEvent] = []
        received_by_handler2: list[DomainEvent] = []

        async def handler1(e: DomainEvent) -> None:
            received_by_handler1.append(e)

        async def handler2(e: DomainEvent) -> None:
            received_by_handler2.append(e)

        event_type = type(event)
        bus.subscribe(event_type, handler1)
        bus.subscribe(event_type, handler2)
        await bus.publish([event])

        assert len(received_by_handler1) == 1
        assert len(received_by_handler2) == 1
        assert received_by_handler1[0].event_id == event.event_id
        assert received_by_handler2[0].event_id == event.event_id

    async def test_unsubscribe_stops_delivery(self) -> None:
        """Verify that unsubscribe stops event delivery to handler."""
        bus = self.create_bus()
        aggregate_id = uuid4()
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        event1 = self.create_test_event(aggregate_id)
        event_type = type(event1)
        bus.subscribe(event_type, handler)
        await bus.publish([event1])
        assert len(received_events) == 1

        assert bus.unsubscribe(event_type, handler)

        event2 = self.create_test_event(aggregate_id)
        await bus.publish([event2])
        assert len(received_events) == 1

    async def test_subscribe_to_all_events(self) -> None:
        """Verify that wildcard subscriptions receive all event types."""
        bus = self.create_bus()
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe_to_all_events(handler)
        event1 = self.create_test_event(uuid4())
        event2 = self.create_test_event(uuid4())
        await bus.publish([event1, event2])

        assert len(received_events) == 2
        assert received_events[0].event_id == event1.event_id
        assert received_events[1].event_id == event2.event_id

    async def test_unsubscribe_from_all_events(self) -> None:
        """Verify that unsubscribing from wildcard stops delivery."""
        bus = self.create_bus()
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe_to_all_events(handler)
        event1 = self.create_test_event(uuid4())
        await bus.publish([event1])
        assert len(received_events) == 1

        assert bus.unsubscribe_from_all_events(handler)

        event2 = self.create_test_event(uuid4())
        await bus.publish([event2])
        assert len(received_events) == 1

    async def test_handler_error_isolation(self) -> None:
        """Verify that handler errors don't prevent other handlers from running."""
        bus = self.create_bus()
        event = self.create_test_event(uuid4())
        received_by_good_handler: list[DomainEvent] = []

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler error for testing")

        async def good_handler(e: DomainEvent) -> None:
            received_by_good_handler.append(e)

        event_type = type(event)
        bus.subscribe(event_type, failing_handler)
        bus.subscribe(event_type, good_handler)
        await bus.publish([event])

        assert len(received_by_good_handler) == 1

    async def test_background_publish_delivers(self) -> None:
        """Verify that background publishing still delivers the event."""
        bus = self.create_bus()
        event = self.create_test_event(uuid4())
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(event), handler)
        await bus.publish([event], background=True)
        await self.await_delivery(bus)

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id

    async def test_per_aggregate_ordering(self) -> None:
        """Verify that events for one aggregate arrive in publish order."""
        bus = self.create_bus()
        aggregate_id = uuid4()
        events = [self.create_test_event(aggregate_id) for _ in range(5)]
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(events[0]), handler)
        await bus.publish(events)
        await self.await_delivery(bus)

        assert len(received_events) == len(events)
        assert [e.event_id for e in received_events] == [e.event_id for e in events]

    async def test_subscribe_all_registers_declared_types(self) -> None:
        """Verify that subscribe_all registers for every declared event type."""
        bus = self.create_bus()
        received_events: list[DomainEvent] = []
        subscriber = self.create_subscriber(received_events)
        bus.subscribe_all(subscriber)

        event = self.create_test_event(uuid4())
        await bus.publish([event])
        await self.await_delivery(bus)

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id


__all__ = [
    "EventBusConformanceSuite",
]
