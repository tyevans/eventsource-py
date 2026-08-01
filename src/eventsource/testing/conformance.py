"""
Conformance test suite for EventBus implementations.

This module provides a base test class that verifies backend implementations
conform to the EventBus contract. Backend implementations can subclass this
suite to validate correctness.

Store implementers should use `eventsource.testing.conformance_ports` instead:
the per-port suites there (`AppenderConformance`, `StreamReaderConformance`,
`EventLookupConformance`, `GlobalFeedConformance`, `CategoryQueryConformance`,
`TypeQueryConformance`) are the store conformance surface -- richer and more
granular than the retired `EventStoreConformanceSuite` that used to live here.

Example:
    >>> class MyEventBusConformanceTests(EventBusConformanceSuite):
    ...     def create_bus(self) -> EventBus:
    ...         return MyEventBus()
    ...
    ...     def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
    ...         return TestEvent(aggregate_id=aggregate_id)
    ...
    ...     async def test_publish_and_subscribe_roundtrip(self):
    ...         await super().test_publish_and_subscribe_roundtrip()

Note:
    All test methods are async since the interfaces are async.
    Test runners should use pytest with asyncio_mode="auto".
"""

from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID, uuid4

from eventsource.events.base import DomainEvent
from eventsource.ports.bus import EventBus


class EventBusConformanceSuite(ABC):
    """
    Base test suite for EventBus implementations.

    Subclasses must implement factory methods to create bus instances
    and test events. The suite provides test methods that verify the
    EventBus contract.

    Abstract Methods:
        create_bus: Factory method to create a fresh bus instance
        create_test_event: Factory method to create test events

    Test Coverage:
        - publish and subscribe roundtrip
        - multiple subscribers receive same event
        - unsubscribe stops delivery
        - subscribe_all registers for all declared event types
        - wildcard subscriptions (subscribe_to_all_events)
        - handler errors don't prevent other handlers from running

    Example:
        >>> class MyBusConformanceTests(EventBusConformanceSuite):
        ...     def create_bus(self) -> EventBus:
        ...         return MyEventBus()
        ...
        ...     def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        ...         return MyTestEvent(aggregate_id=aggregate_id)
    """

    @abstractmethod
    def create_bus(self) -> EventBus:
        """
        Create a fresh EventBus instance for testing.

        This method should return a new, empty bus instance.
        Called before each test method to ensure isolation.

        Returns:
            EventBus instance ready for testing
        """
        pass

    @abstractmethod
    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        """
        Create a test event with the given aggregate_id.

        Args:
            aggregate_id: The aggregate's unique identifier

        Returns:
            DomainEvent instance for testing
        """
        pass

    @abstractmethod
    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        """
        Create an EventSubscriber that appends handled events to ``received``.

        The subscriber's ``subscribed_to()`` must return the type produced by
        ``create_test_event``.

        Args:
            received: List the subscriber appends each handled event to.

        Returns:
            An object with ``subscribed_to()`` and ``handle()``.
        """
        pass

    async def await_delivery(self, bus: EventBus) -> None:
        """
        Wait for in-flight deliveries to land.

        Defaults to a no-op, which is correct for in-process buses that
        dispatch synchronously within ``publish``. Distributed backends
        override this with a bounded poll or a consumer drain.

        Args:
            bus: The bus under test.
        """
        return None

    async def test_publish_and_subscribe_roundtrip(self) -> None:
        """
        Verify that events can be published and received by subscribers.

        Tests the basic publish/subscribe cycle:
        1. Subscribe a handler to an event type
        2. Publish an event of that type
        3. Verify handler received the event
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)

        # Track received events
        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        # Subscribe and publish
        event_type = type(event)
        bus.subscribe(event_type, handler)
        await bus.publish([event])

        # Verify handler received the event
        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id

    async def test_multiple_subscribers(self) -> None:
        """
        Verify that multiple subscribers receive the same event.

        Tests that:
        1. Multiple handlers can subscribe to the same event type
        2. All handlers receive the event when published
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)

        # Track received events for two handlers
        received_by_handler1: list[DomainEvent] = []
        received_by_handler2: list[DomainEvent] = []

        async def handler1(e: DomainEvent) -> None:
            received_by_handler1.append(e)

        async def handler2(e: DomainEvent) -> None:
            received_by_handler2.append(e)

        # Subscribe both handlers
        event_type = type(event)
        bus.subscribe(event_type, handler1)
        bus.subscribe(event_type, handler2)

        # Publish event
        await bus.publish([event])

        # Verify both handlers received the event
        assert len(received_by_handler1) == 1
        assert len(received_by_handler2) == 1
        assert received_by_handler1[0].event_id == event.event_id
        assert received_by_handler2[0].event_id == event.event_id

    async def test_unsubscribe_stops_delivery(self) -> None:
        """
        Verify that unsubscribe stops event delivery to handler.

        Tests that:
        1. Handler receives events before unsubscribe
        2. Handler does not receive events after unsubscribe
        """
        bus = self.create_bus()
        aggregate_id = uuid4()

        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        # Subscribe, publish, verify receipt
        event1 = self.create_test_event(aggregate_id)
        event_type = type(event1)
        bus.subscribe(event_type, handler)
        await bus.publish([event1])
        assert len(received_events) == 1

        # Unsubscribe
        removed = bus.unsubscribe(event_type, handler)
        assert removed

        # Publish again, verify no receipt
        event2 = self.create_test_event(aggregate_id)
        await bus.publish([event2])
        assert len(received_events) == 1  # Still 1, not 2

    async def test_subscribe_to_all_events(self) -> None:
        """
        Verify that wildcard subscriptions receive all event types.

        Tests that:
        1. Handler subscribed to all events receives different event types
        2. Handler receives all published events regardless of type
        """
        bus = self.create_bus()

        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        # Subscribe to all events
        bus.subscribe_to_all_events(handler)

        # Publish different event types
        event1 = self.create_test_event(uuid4())
        event2 = self.create_test_event(uuid4())
        await bus.publish([event1, event2])

        # Verify handler received both
        assert len(received_events) == 2
        assert received_events[0].event_id == event1.event_id
        assert received_events[1].event_id == event2.event_id

    async def test_unsubscribe_from_all_events(self) -> None:
        """
        Verify that unsubscribing from wildcard stops delivery.

        Tests that:
        1. Handler receives events before unsubscribe
        2. Handler does not receive events after unsubscribe
        """
        bus = self.create_bus()

        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        # Subscribe to all, publish, verify
        bus.subscribe_to_all_events(handler)
        event1 = self.create_test_event(uuid4())
        await bus.publish([event1])
        assert len(received_events) == 1

        # Unsubscribe
        removed = bus.unsubscribe_from_all_events(handler)
        assert removed

        # Publish again, verify no receipt
        event2 = self.create_test_event(uuid4())
        await bus.publish([event2])
        assert len(received_events) == 1  # Still 1, not 2

    async def test_handler_error_isolation(self) -> None:
        """
        Verify that handler errors don't prevent other handlers from running.

        Tests that:
        1. If one handler raises an error, other handlers still execute
        2. Event bus continues to function after handler errors
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)

        received_by_good_handler: list[DomainEvent] = []

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler error for testing")

        async def good_handler(e: DomainEvent) -> None:
            received_by_good_handler.append(e)

        # Subscribe both handlers
        event_type = type(event)
        bus.subscribe(event_type, failing_handler)
        bus.subscribe(event_type, good_handler)

        # Publish event - should not raise
        await bus.publish([event])

        # Verify good handler still ran
        assert len(received_by_good_handler) == 1

    async def test_background_publish_delivers(self) -> None:
        """
        Verify that background publishing still delivers the event.

        ``background=True`` means "do not wait for durability" -- publish
        returns without waiting for the delivery to be confirmed or handled --
        but the event must still arrive.
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)

        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(event), handler)

        # Must not raise, and must not block on durability.
        await bus.publish([event], background=True)

        await self.await_delivery(bus)

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id

    async def test_per_aggregate_ordering(self) -> None:
        """
        Verify that events for one aggregate arrive in publish order.

        Deliberately per-aggregate rather than global: Kafka partitions by
        aggregate_id, so global ordering is not a contract any distributed
        backend can honor.
        """
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
        """
        Verify that subscribe_all registers for every declared event type.

        Tests that a subscriber registered via subscribe_all receives events
        of the types returned by its subscribed_to() method.
        """
        bus = self.create_bus()
        received_events: list[DomainEvent] = []
        subscriber = self.create_subscriber(received_events)

        bus.subscribe_all(subscriber)

        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)
        await bus.publish([event])
        await self.await_delivery(bus)

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id


__all__ = [
    "EventBusConformanceSuite",
]
