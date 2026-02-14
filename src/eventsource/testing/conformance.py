"""
Conformance test suites for EventStore and EventBus implementations.

This module provides base test classes that verify backend implementations
conform to the EventStore and EventBus contracts. Backend implementations
can subclass these suites to validate correctness.

Example:
    >>> class PostgreSQLEventStoreConformanceTests(EventStoreConformanceSuite):
    ...     def create_store(self) -> EventStore:
    ...         return PostgreSQLEventStore(engine=self.engine)
    ...
    ...     def create_test_event(self, aggregate_id: UUID, version: int = 1) -> DomainEvent:
    ...         return TestEvent(aggregate_id=aggregate_id, aggregate_version=version)
    ...
    ...     async def test_append_and_get(self):
    ...         await super().test_append_and_get()  # Run base conformance test

Note:
    All test methods are async since the interfaces are async.
    Test runners should use pytest with asyncio_mode="auto".
"""

from abc import ABC, abstractmethod
from uuid import UUID, uuid4

from eventsource.bus.interface import EventBus
from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.stores.interface import EventStore, ExpectedVersion


class EventStoreConformanceSuite(ABC):
    """
    Base test suite for EventStore implementations.

    Subclasses must implement factory methods to create store instances
    and test events. The suite provides test methods that verify the
    EventStore contract.

    Abstract Methods:
        create_store: Factory method to create a fresh store instance
        create_test_event: Factory method to create test events

    Test Coverage:
        - append and get roundtrip
        - stream isolation (events from different streams don't mix)
        - optimistic locking (wrong expected_version raises OptimisticLockError)
        - empty stream returns empty list
        - event metadata preserved (event_type, timestamp, aggregate_id, version)
        - event_exists idempotency check
        - global position tracking

    Example:
        >>> class MyStoreConformanceTests(EventStoreConformanceSuite):
        ...     def create_store(self) -> EventStore:
        ...         return MyEventStore()
        ...
        ...     def create_test_event(self, aggregate_id: UUID, version: int = 1) -> DomainEvent:
        ...         return MyTestEvent(aggregate_id=aggregate_id, aggregate_version=version)
    """

    @abstractmethod
    def create_store(self) -> EventStore:
        """
        Create a fresh EventStore instance for testing.

        This method should return a new, empty store instance.
        Called before each test method to ensure isolation.

        Returns:
            EventStore instance ready for testing
        """
        pass

    @abstractmethod
    def create_test_event(self, aggregate_id: UUID, version: int = 1) -> DomainEvent:
        """
        Create a test event with the given aggregate_id and version.

        Args:
            aggregate_id: The aggregate's unique identifier
            version: The event's aggregate_version (default: 1)

        Returns:
            DomainEvent instance for testing
        """
        pass

    async def test_append_and_get_roundtrip(self) -> None:
        """
        Verify that events can be appended and retrieved.

        Tests the basic append/get cycle:
        1. Append events to a new aggregate
        2. Retrieve events using get_events
        3. Verify retrieved events match appended events
        """
        store = self.create_store()
        aggregate_id = uuid4()

        # Create test events
        event1 = self.create_test_event(aggregate_id, version=1)
        event2 = self.create_test_event(aggregate_id, version=2)
        events = [event1, event2]
        aggregate_type = event1.aggregate_type

        # Append events
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=events,
            expected_version=0,
        )

        assert result.success
        assert result.new_version == 2

        # Retrieve events
        stream = await store.get_events(aggregate_id, aggregate_type)

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == aggregate_type
        assert stream.version == 2
        assert len(stream.events) == 2
        assert stream.events[0].event_id == event1.event_id
        assert stream.events[1].event_id == event2.event_id

    async def test_stream_isolation(self) -> None:
        """
        Verify that events from different streams don't mix.

        Tests that:
        1. Events appended to aggregate A don't appear in aggregate B's stream
        2. Each aggregate maintains its own independent event stream
        """
        store = self.create_store()
        aggregate_id_a = uuid4()
        aggregate_id_b = uuid4()

        # Append events to aggregate A
        event_a = self.create_test_event(aggregate_id_a, version=1)
        aggregate_type = event_a.aggregate_type
        await store.append_events(
            aggregate_id=aggregate_id_a,
            aggregate_type=aggregate_type,
            events=[event_a],
            expected_version=0,
        )

        # Append events to aggregate B
        event_b = self.create_test_event(aggregate_id_b, version=1)
        await store.append_events(
            aggregate_id=aggregate_id_b,
            aggregate_type=aggregate_type,
            events=[event_b],
            expected_version=0,
        )

        # Verify stream A only contains event A
        stream_a = await store.get_events(aggregate_id_a, aggregate_type)
        assert len(stream_a.events) == 1
        assert stream_a.events[0].event_id == event_a.event_id

        # Verify stream B only contains event B
        stream_b = await store.get_events(aggregate_id_b, aggregate_type)
        assert len(stream_b.events) == 1
        assert stream_b.events[0].event_id == event_b.event_id

    async def test_optimistic_locking(self) -> None:
        """
        Verify that optimistic locking prevents concurrent modifications.

        Tests that:
        1. Appending with wrong expected_version raises OptimisticLockError
        2. The error includes correct expected vs actual version info
        """
        store = self.create_store()
        aggregate_id = uuid4()

        # Create aggregate with version 1
        event1 = self.create_test_event(aggregate_id, version=1)
        aggregate_type = event1.aggregate_type
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event1],
            expected_version=0,
        )

        # Try to append with wrong expected version
        event2 = self.create_test_event(aggregate_id, version=2)
        try:
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
                events=[event2],
                expected_version=5,  # Wrong: actual version is 1
            )
            raise AssertionError("Expected OptimisticLockError")
        except OptimisticLockError as e:
            assert e.aggregate_id == aggregate_id
            assert e.expected_version == 5
            assert e.actual_version == 1

    async def test_empty_stream(self) -> None:
        """
        Verify that retrieving a non-existent aggregate returns empty stream.

        Tests that:
        1. get_events on non-existent aggregate returns empty EventStream
        2. Version is 0 for empty streams
        """
        store = self.create_store()
        aggregate_id = uuid4()

        # Create a dummy event just to get the aggregate_type
        dummy_event = self.create_test_event(aggregate_id, version=1)
        aggregate_type = dummy_event.aggregate_type

        stream = await store.get_events(aggregate_id, aggregate_type)

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == aggregate_type
        assert stream.version == 0
        assert len(stream.events) == 0
        assert stream.is_empty

    async def test_event_metadata_preserved(self) -> None:
        """
        Verify that event metadata is preserved during storage.

        Tests that:
        1. event_type is preserved
        2. aggregate_id is preserved
        3. aggregate_version is preserved
        4. occurred_at timestamp is preserved
        """
        store = self.create_store()
        aggregate_id = uuid4()

        event = self.create_test_event(aggregate_id, version=1)
        aggregate_type = event.aggregate_type
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event],
            expected_version=0,
        )

        stream = await store.get_events(aggregate_id, aggregate_type)
        retrieved_event = stream.events[0]

        assert retrieved_event.event_type == event.event_type
        assert retrieved_event.aggregate_id == event.aggregate_id
        assert retrieved_event.aggregate_version == event.aggregate_version
        assert retrieved_event.occurred_at == event.occurred_at

    async def test_event_exists_idempotency(self) -> None:
        """
        Verify that event_exists correctly checks for event presence.

        Tests that:
        1. event_exists returns False for non-existent events
        2. event_exists returns True after event is appended
        """
        store = self.create_store()
        aggregate_id = uuid4()

        event = self.create_test_event(aggregate_id, version=1)
        aggregate_type = event.aggregate_type

        # Event doesn't exist yet
        assert not await store.event_exists(event.event_id)

        # Append event
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event],
            expected_version=0,
        )

        # Event now exists
        assert await store.event_exists(event.event_id)

    async def test_expected_version_any(self) -> None:
        """
        Verify that ExpectedVersion.ANY disables optimistic locking.

        Tests that:
        1. Appending with ExpectedVersion.ANY always succeeds
        2. Works for both new and existing streams
        """
        store = self.create_store()
        aggregate_id = uuid4()

        # First append with ANY
        event1 = self.create_test_event(aggregate_id, version=1)
        aggregate_type = event1.aggregate_type
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event1],
            expected_version=ExpectedVersion.ANY,
        )
        assert result.success

        # Second append with ANY (should not raise OptimisticLockError)
        event2 = self.create_test_event(aggregate_id, version=2)
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event2],
            expected_version=ExpectedVersion.ANY,
        )
        assert result.success

    async def test_global_position_tracking(self) -> None:
        """
        Verify that global position is tracked correctly.

        Tests that:
        1. get_global_position returns 0 for empty store
        2. Global position increases with each append
        3. AppendResult includes correct global_position
        """
        store = self.create_store()

        # Empty store has position 0
        position = await store.get_global_position()
        assert position == 0

        # Append events and verify position increases
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id, version=1)
        aggregate_type = event.aggregate_type

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event],
            expected_version=0,
        )

        assert result.global_position > 0
        new_position = await store.get_global_position()
        assert new_position == result.global_position


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


__all__ = [
    "EventStoreConformanceSuite",
    "EventBusConformanceSuite",
]
