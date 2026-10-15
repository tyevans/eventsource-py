"""Conformance test suite for EventBus implementations."""

from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID, uuid4

from eventsource.domain.event import DomainEvent
from eventsource.ports.bus import EventBus


class EventBusConformanceSuite(ABC):
    """Base test suite verifying the EventBus contract across backend implementations."""

    @abstractmethod
    def create_bus(self) -> EventBus:
        """Create a fresh, empty EventBus instance for testing."""

    @abstractmethod
    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        """Create a test event with the given aggregate_id."""

    @abstractmethod
    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        """Create an EventSubscriber that appends handled events to ``received``."""

    async def await_delivery(self, bus: EventBus) -> None:
        """Wait for in-flight deliveries to land (no-op for in-process buses)."""
        return None

    async def test_publish_and_subscribe_roundtrip(self) -> None:
        """Verify that events can be published and received by subscribers."""
        bus, event = self.create_bus(), self.create_test_event(uuid4())
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(event), handler)
        await bus.publish([event])
        assert [e.event_id for e in received_events] == [event.event_id]

    async def test_multiple_subscribers(self) -> None:
        """Verify that multiple subscribers receive the same event."""
        bus, event = self.create_bus(), self.create_test_event(uuid4())
        received1: list[DomainEvent] = []
        received2: list[DomainEvent] = []

        async def handler1(e: DomainEvent) -> None:
            received1.append(e)

        async def handler2(e: DomainEvent) -> None:
            received2.append(e)

        bus.subscribe(type(event), handler1)
        bus.subscribe(type(event), handler2)
        await bus.publish([event])
        assert [e.event_id for e in received1] == [event.event_id]
        assert [e.event_id for e in received2] == [event.event_id]

    async def test_unsubscribe_stops_delivery(self) -> None:
        """Verify that unsubscribe stops event delivery to handler."""
        bus, aggregate_id = self.create_bus(), uuid4()
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        event1 = self.create_test_event(aggregate_id)
        bus.subscribe(type(event1), handler)
        await bus.publish([event1])
        assert len(received_events) == 1
        assert bus.unsubscribe(type(event1), handler)
        await bus.publish([self.create_test_event(aggregate_id)])
        assert len(received_events) == 1

    async def test_subscribe_to_all_events(self) -> None:
        """Verify that wildcard subscriptions receive all event types."""
        bus = self.create_bus()
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe_to_all_events(handler)
        e1, e2 = self.create_test_event(uuid4()), self.create_test_event(uuid4())
        await bus.publish([e1, e2])
        assert [e.event_id for e in received_events] == [e1.event_id, e2.event_id]

    async def test_unsubscribe_from_all_events(self) -> None:
        """Verify that unsubscribing from wildcard stops delivery."""
        bus = self.create_bus()
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe_to_all_events(handler)
        await bus.publish([self.create_test_event(uuid4())])
        assert len(received_events) == 1
        assert bus.unsubscribe_from_all_events(handler)
        await bus.publish([self.create_test_event(uuid4())])
        assert len(received_events) == 1

    async def test_handler_error_isolation(self) -> None:
        """Verify that handler errors don't prevent other handlers from running."""
        bus, event = self.create_bus(), self.create_test_event(uuid4())
        received_by_good_handler: list[DomainEvent] = []

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler error for testing")

        async def good_handler(e: DomainEvent) -> None:
            received_by_good_handler.append(e)

        bus.subscribe(type(event), failing_handler)
        bus.subscribe(type(event), good_handler)
        await bus.publish([event])
        assert len(received_by_good_handler) == 1

    async def test_background_publish_delivers(self) -> None:
        """Verify that background publishing still delivers the event."""
        bus, event = self.create_bus(), self.create_test_event(uuid4())
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(event), handler)
        await bus.publish([event], background=True)
        await self.await_delivery(bus)
        assert [e.event_id for e in received_events] == [event.event_id]

    async def test_per_aggregate_ordering(self) -> None:
        """Verify that events for one aggregate arrive in publish order."""
        bus, aggregate_id = self.create_bus(), uuid4()
        events = [self.create_test_event(aggregate_id) for _ in range(5)]
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(events[0]), handler)
        await bus.publish(events)
        await self.await_delivery(bus)
        assert [e.event_id for e in received_events] == [e.event_id for e in events]

    async def test_subscribe_all_registers_declared_types(self) -> None:
        """Verify that subscribe_all registers for every declared event type."""
        bus = self.create_bus()
        received_events: list[DomainEvent] = []
        bus.subscribe_all(self.create_subscriber(received_events))
        event = self.create_test_event(uuid4())
        await bus.publish([event])
        await self.await_delivery(bus)
        assert [e.event_id for e in received_events] == [event.event_id]


__all__ = ["EventBusConformanceSuite"]
