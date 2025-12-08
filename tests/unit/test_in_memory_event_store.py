"""
Unit tests for the InMemoryEventStore implementation.

Tests cover:
- Basic CRUD operations (append, get, exists)
- Optimistic concurrency control with all ExpectedVersion constants
- Idempotency (duplicate event handling)
- Event filtering (by type, version, timestamp)
- Stream reading (forward, backward, with limits)
- read_all functionality
- Thread safety
- Helper methods (clear, get_all_events, etc.)
"""

import asyncio
import warnings
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.stores.interface import (
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)

# --- Test Event Classes ---


class SampleEvent(DomainEvent):
    """Simple test event."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


class OrderCreated(DomainEvent):
    """Order creation event for testing."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str


class OrderUpdated(DomainEvent):
    """Order update event for testing."""

    event_type: str = "OrderUpdated"
    aggregate_type: str = "Order"
    new_status: str


class UserRegistered(DomainEvent):
    """User registration event for testing multi-type scenarios."""

    event_type: str = "UserRegistered"
    aggregate_type: str = "User"
    email: str


# --- Fixtures ---


@pytest.fixture
def store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore for each test."""
    return InMemoryEventStore()


@pytest.fixture
def aggregate_id() -> UUID:
    """Create a random aggregate ID."""
    return uuid4()


@pytest.fixture
def tenant_id() -> UUID:
    """Create a random tenant ID."""
    return uuid4()


# --- Basic Functionality Tests ---


class TestInMemoryEventStoreBasic:
    """Tests for basic append and get operations."""

    @pytest.mark.asyncio
    async def test_append_single_event(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test appending a single event to a new stream."""
        event = SampleEvent(aggregate_id=aggregate_id, data="first")

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1
        assert result.global_position == 1

    @pytest.mark.asyncio
    async def test_append_multiple_events(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test appending multiple events at once."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 3
        assert result.global_position == 3

    @pytest.mark.asyncio
    async def test_append_empty_list(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test appending empty event list returns success with current version."""
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[],
            expected_version=5,
        )

        assert result.success is True
        assert result.new_version == 5

    @pytest.mark.asyncio
    async def test_get_events_from_stream(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test getting events from an existing stream."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "TestAggregate"
        assert len(stream.events) == 3
        assert stream.version == 3

    @pytest.mark.asyncio
    async def test_get_events_from_empty_stream(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test getting events from a non-existent stream returns empty."""
        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "TestAggregate"
        assert len(stream.events) == 0
        assert stream.version == 0
        assert stream.is_empty is True

    @pytest.mark.asyncio
    async def test_event_exists(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test checking if an event exists."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        assert await store.event_exists(event.event_id) is True
        assert await store.event_exists(uuid4()) is False


# --- Optimistic Concurrency Tests ---


class TestOptimisticConcurrency:
    """Tests for optimistic locking with various ExpectedVersion values."""

    @pytest.mark.asyncio
    async def test_version_conflict_raises_error(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test that version mismatch raises OptimisticLockError."""
        # First append
        event1 = SampleEvent(aggregate_id=aggregate_id, data="first")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Try to append with wrong version
        event2 = SampleEvent(aggregate_id=aggregate_id, data="second")
        with pytest.raises(OptimisticLockError) as exc_info:
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event2],
                expected_version=0,  # Should be 1
            )

        assert exc_info.value.aggregate_id == aggregate_id
        assert exc_info.value.expected_version == 0
        assert exc_info.value.actual_version == 1

    @pytest.mark.asyncio
    async def test_expected_version_any(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test ExpectedVersion.ANY skips version check."""
        # Append first event
        event1 = SampleEvent(aggregate_id=aggregate_id, data="first")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Append with ANY version
        event2 = SampleEvent(aggregate_id=aggregate_id, data="second")
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event2],
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True
        assert result.new_version == 2

    @pytest.mark.asyncio
    async def test_expected_version_no_stream_success(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test NO_STREAM succeeds when stream doesn't exist."""
        event = SampleEvent(aggregate_id=aggregate_id, data="first")

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=ExpectedVersion.NO_STREAM,
        )

        assert result.success is True
        assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_expected_version_no_stream_fails_when_exists(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test NO_STREAM fails when stream already exists."""
        # Create stream first
        event1 = SampleEvent(aggregate_id=aggregate_id, data="first")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Try with NO_STREAM
        event2 = SampleEvent(aggregate_id=aggregate_id, data="second")
        with pytest.raises(OptimisticLockError):
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event2],
                expected_version=ExpectedVersion.NO_STREAM,
            )

    @pytest.mark.asyncio
    async def test_expected_version_stream_exists_success(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test STREAM_EXISTS succeeds when stream has events."""
        # Create stream first
        event1 = SampleEvent(aggregate_id=aggregate_id, data="first")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Append with STREAM_EXISTS
        event2 = SampleEvent(aggregate_id=aggregate_id, data="second")
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event2],
            expected_version=ExpectedVersion.STREAM_EXISTS,
        )

        assert result.success is True
        assert result.new_version == 2

    @pytest.mark.asyncio
    async def test_expected_version_stream_exists_fails_when_empty(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test STREAM_EXISTS fails when stream is empty."""
        event = SampleEvent(aggregate_id=aggregate_id, data="first")

        with pytest.raises(OptimisticLockError):
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=ExpectedVersion.STREAM_EXISTS,
            )

    @pytest.mark.asyncio
    async def test_specific_version_success(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test specific version check succeeds with correct version."""
        # Append multiple events
        for i in range(5):
            event = SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}")
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=i,
            )

        # Append with correct specific version
        event = SampleEvent(aggregate_id=aggregate_id, data="final")
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=5,
        )

        assert result.success is True
        assert result.new_version == 6


# --- Idempotency Tests ---


class TestIdempotency:
    """Tests for duplicate event handling (idempotency)."""

    @pytest.mark.asyncio
    async def test_duplicate_event_skipped(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test that duplicate events are silently skipped."""
        event = SampleEvent(aggregate_id=aggregate_id, data="unique")

        # Append first time
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        # Append same event again (simulate retry) with ANY to skip version check
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=ExpectedVersion.ANY,
        )

        # Should still succeed but event count stays the same
        assert result.success is True
        assert await store.get_event_count() == 1

    @pytest.mark.asyncio
    async def test_mixed_new_and_duplicate_events(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test batch with mix of new and duplicate events."""
        event1 = SampleEvent(aggregate_id=aggregate_id, data="first")

        # Append first event
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Append batch with duplicate and new event
        event2 = SampleEvent(aggregate_id=aggregate_id, data="second")
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1, event2],  # event1 is duplicate
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True
        assert await store.get_event_count() == 2  # Only 2 unique events


# --- Event Filtering Tests ---


class TestEventFiltering:
    """Tests for filtering events by various criteria."""

    @pytest.mark.asyncio
    async def test_get_events_by_aggregate_type(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test filtering by aggregate type."""
        # Add Order event
        order_event = OrderCreated(aggregate_id=aggregate_id, customer_name="John")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[order_event],
            expected_version=0,
        )

        # Add User event to different aggregate
        user_id = uuid4()
        user_event = UserRegistered(aggregate_id=user_id, email="john@example.com")
        await store.append_events(
            aggregate_id=user_id,
            aggregate_type="User",
            events=[user_event],
            expected_version=0,
        )

        # Get only Order events
        order_events = await store.get_events_by_type("Order")
        assert len(order_events) == 1
        assert order_events[0].event_type == "OrderCreated"

        # Get only User events
        user_events = await store.get_events_by_type("User")
        assert len(user_events) == 1
        assert user_events[0].event_type == "UserRegistered"

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_tenant_filter(
        self, store: InMemoryEventStore, aggregate_id: UUID, tenant_id: UUID
    ) -> None:
        """Test filtering by aggregate type and tenant."""
        # Add event with tenant
        event1 = SampleEvent(aggregate_id=aggregate_id, data="with_tenant", tenant_id=tenant_id)
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Add event without tenant
        other_id = uuid4()
        event2 = SampleEvent(aggregate_id=other_id, data="no_tenant")
        await store.append_events(
            aggregate_id=other_id,
            aggregate_type="TestAggregate",
            events=[event2],
            expected_version=0,
        )

        # Get only events for specific tenant
        events = await store.get_events_by_type("TestAggregate", tenant_id=tenant_id)
        assert len(events) == 1
        assert events[0].data == "with_tenant"

    @pytest.mark.asyncio
    async def test_get_events_from_version(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test getting events from a specific version."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        # Get events from version 2 (skip first 2)
        stream = await store.get_events(aggregate_id, "TestAggregate", from_version=2)

        assert len(stream.events) == 3
        assert stream.events[0].data == "event_2"

    @pytest.mark.asyncio
    async def test_get_events_with_timestamp_filter(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test filtering events by timestamp range."""
        now = datetime.now(UTC)

        # Create events with different timestamps
        event1 = SampleEvent(aggregate_id=aggregate_id, data="old")
        # Use model_copy to set specific timestamp
        event1 = event1.model_copy(update={"occurred_at": now - timedelta(hours=2)})

        event2 = SampleEvent(aggregate_id=aggregate_id, data="middle")
        event2 = event2.model_copy(update={"occurred_at": now - timedelta(hours=1)})

        event3 = SampleEvent(aggregate_id=aggregate_id, data="new")
        event3 = event3.model_copy(update={"occurred_at": now})

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1, event2, event3],
            expected_version=0,
        )

        # Get events from last 90 minutes
        from_ts = now - timedelta(minutes=90)
        stream = await store.get_events(aggregate_id, "TestAggregate", from_timestamp=from_ts)

        assert len(stream.events) == 2
        assert stream.events[0].data == "middle"
        assert stream.events[1].data == "new"

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_timestamp_filter(
        self, store: InMemoryEventStore
    ) -> None:
        """Test get_events_by_type with Unix timestamp filter."""
        now = datetime.now(UTC)

        # Create events with different timestamps
        agg_id = uuid4()
        event1 = SampleEvent(aggregate_id=agg_id, data="old")
        event1 = event1.model_copy(update={"occurred_at": now - timedelta(hours=2)})

        event2 = SampleEvent(aggregate_id=agg_id, data="new")
        event2 = event2.model_copy(update={"occurred_at": now})

        await store.append_events(
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            events=[event1, event2],
            expected_version=0,
        )

        # Get events from last 90 minutes using Unix timestamp (deprecated)
        # Use warnings.catch_warnings to suppress the deprecation warning
        # since we're intentionally testing backward compatibility
        from_ts = (now - timedelta(minutes=90)).timestamp()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            events = await store.get_events_by_type("TestAggregate", from_timestamp=from_ts)

        assert len(events) == 1
        assert events[0].data == "new"


# --- Stream Version Tests ---


class TestStreamVersion:
    """Tests for get_stream_version method."""

    @pytest.mark.asyncio
    async def test_get_stream_version(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test getting stream version."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        version = await store.get_stream_version(aggregate_id, "TestAggregate")
        assert version == 5

    @pytest.mark.asyncio
    async def test_get_stream_version_empty(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test getting version of non-existent stream."""
        version = await store.get_stream_version(aggregate_id, "TestAggregate")
        assert version == 0


# --- Read Stream Tests ---


class TestReadStream:
    """Tests for read_stream async iterator."""

    @pytest.mark.asyncio
    async def test_read_stream_forward(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test reading stream in forward direction."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        stream_id = f"{aggregate_id}:TestAggregate"
        stored_events = []
        async for stored in store.read_stream(stream_id):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert all(isinstance(e, StoredEvent) for e in stored_events)
        assert stored_events[0].stream_position == 1
        assert stored_events[1].stream_position == 2
        assert stored_events[2].stream_position == 3
        assert stored_events[0].event.data == "event_0"

    @pytest.mark.asyncio
    async def test_read_stream_backward(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test reading stream in backward direction."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        # Events should be in reverse order
        assert stored_events[0].event.data == "event_2"
        assert stored_events[1].event.data == "event_1"
        assert stored_events[2].event.data == "event_0"

    @pytest.mark.asyncio
    async def test_read_stream_with_limit(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test reading stream with limit."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(limit=2)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 2
        assert stored_events[0].event.data == "event_0"
        assert stored_events[1].event.data == "event_1"

    @pytest.mark.asyncio
    async def test_read_stream_from_position(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test reading stream from specific position."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(from_position=2)  # Skip first 2
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert stored_events[0].event.data == "event_2"

    @pytest.mark.asyncio
    async def test_read_stream_with_global_position(self, store: InMemoryEventStore) -> None:
        """Test that read_stream includes global position."""
        agg1 = uuid4()
        agg2 = uuid4()

        # Add events to two different aggregates
        event1 = SampleEvent(aggregate_id=agg1, data="first_agg1")
        await store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        event2 = SampleEvent(aggregate_id=agg2, data="first_agg2")
        await store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestAggregate",
            events=[event2],
            expected_version=0,
        )

        event3 = SampleEvent(aggregate_id=agg1, data="second_agg1")
        await store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[event3],
            expected_version=1,
        )

        # Read first aggregate's stream
        stream_id = f"{agg1}:TestAggregate"
        stored_events = []
        async for stored in store.read_stream(stream_id):
            stored_events.append(stored)

        assert len(stored_events) == 2
        assert stored_events[0].global_position == 1  # First global
        assert stored_events[1].global_position == 3  # Third global


# --- Read All Tests ---


class TestReadAll:
    """Tests for read_all async iterator."""

    @pytest.mark.asyncio
    async def test_read_all_events(self, store: InMemoryEventStore) -> None:
        """Test reading all events across streams."""
        agg1 = uuid4()
        agg2 = uuid4()

        # Add events to different aggregates
        event1 = SampleEvent(aggregate_id=agg1, data="agg1_event")
        await store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )

        event2 = OrderCreated(aggregate_id=agg2, customer_name="Jane")
        await store.append_events(
            aggregate_id=agg2,
            aggregate_type="Order",
            events=[event2],
            expected_version=0,
        )

        stored_events = []
        async for stored in store.read_all():
            stored_events.append(stored)

        assert len(stored_events) == 2
        assert stored_events[0].global_position == 1
        assert stored_events[1].global_position == 2

    @pytest.mark.asyncio
    async def test_read_all_with_limit(self, store: InMemoryEventStore) -> None:
        """Test read_all with limit."""
        agg_id = uuid4()
        events = [SampleEvent(aggregate_id=agg_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        options = ReadOptions(limit=3)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 3

    @pytest.mark.asyncio
    async def test_read_all_backward(self, store: InMemoryEventStore) -> None:
        """Test read_all in backward direction."""
        agg_id = uuid4()
        events = [SampleEvent(aggregate_id=agg_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        # Events should be in reverse global order
        assert stored_events[0].event.data == "event_2"
        assert stored_events[1].event.data == "event_1"
        assert stored_events[2].event.data == "event_0"

    @pytest.mark.asyncio
    async def test_read_all_from_position(self, store: InMemoryEventStore) -> None:
        """Test read_all from specific global position."""
        agg_id = uuid4()
        events = [SampleEvent(aggregate_id=agg_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        options = ReadOptions(from_position=3)  # Skip first 3
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 2
        assert stored_events[0].global_position == 4
        assert stored_events[1].global_position == 5


# --- Helper Methods Tests ---


class TestHelperMethods:
    """Tests for testing helper methods."""

    @pytest.mark.asyncio
    async def test_clear(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test clearing all events."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert await store.get_event_count() == 3

        await store.clear()

        assert await store.get_event_count() == 0
        assert await store.get_global_position() == 0
        assert len(await store.get_aggregate_ids()) == 0
        assert len(await store.get_all_events()) == 0

    @pytest.mark.asyncio
    async def test_get_all_events(self, store: InMemoryEventStore) -> None:
        """Test getting all events in chronological order."""
        now = datetime.now(UTC)
        agg1 = uuid4()
        agg2 = uuid4()

        # Add events with controlled timestamps
        event1 = SampleEvent(aggregate_id=agg1, data="first")
        event1 = event1.model_copy(update={"occurred_at": now})

        event2 = SampleEvent(aggregate_id=agg2, data="second")
        event2 = event2.model_copy(update={"occurred_at": now + timedelta(seconds=1)})

        event3 = SampleEvent(aggregate_id=agg1, data="third")
        event3 = event3.model_copy(update={"occurred_at": now + timedelta(seconds=2)})

        await store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[event1],
            expected_version=0,
        )
        await store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestAggregate",
            events=[event2],
            expected_version=0,
        )
        await store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[event3],
            expected_version=1,
        )

        all_events = await store.get_all_events()

        assert len(all_events) == 3
        # Should be in chronological order
        assert all_events[0].data == "first"
        assert all_events[1].data == "second"
        assert all_events[2].data == "third"

    @pytest.mark.asyncio
    async def test_get_event_count(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test getting total event count."""
        assert await store.get_event_count() == 0

        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert await store.get_event_count() == 5

    @pytest.mark.asyncio
    async def test_get_aggregate_ids(self, store: InMemoryEventStore) -> None:
        """Test getting all aggregate IDs."""
        agg1 = uuid4()
        agg2 = uuid4()
        agg3 = uuid4()

        for agg_id in [agg1, agg2, agg3]:
            event = SampleEvent(aggregate_id=agg_id, data="test")
            await store.append_events(
                aggregate_id=agg_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=0,
            )

        ids = await store.get_aggregate_ids()

        assert len(ids) == 3
        assert agg1 in ids
        assert agg2 in ids
        assert agg3 in ids

    @pytest.mark.asyncio
    async def test_get_global_position(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test getting current global position."""
        assert await store.get_global_position() == 0

        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert await store.get_global_position() == 3


# --- Thread Safety Tests ---


class TestThreadSafety:
    """Tests for thread-safe operations."""

    @pytest.mark.asyncio
    async def test_concurrent_appends(self, store: InMemoryEventStore) -> None:
        """Test concurrent append operations."""
        aggregate_id = uuid4()
        num_tasks = 10
        events_per_task = 5

        async def append_events(task_id: int) -> None:
            for i in range(events_per_task):
                event = SampleEvent(
                    aggregate_id=aggregate_id,
                    data=f"task_{task_id}_event_{i}",
                )
                # Use ANY to allow concurrent appends
                await store.append_events(
                    aggregate_id=aggregate_id,
                    aggregate_type="TestAggregate",
                    events=[event],
                    expected_version=ExpectedVersion.ANY,
                )

        # Run concurrent tasks
        tasks = [append_events(i) for i in range(num_tasks)]
        await asyncio.gather(*tasks)

        # All events should be stored
        assert await store.get_event_count() == num_tasks * events_per_task

    @pytest.mark.asyncio
    async def test_concurrent_reads_and_writes(self, store: InMemoryEventStore) -> None:
        """Test concurrent read and write operations."""
        aggregate_id = uuid4()
        num_operations = 50

        async def write_event(i: int) -> None:
            event = SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}")
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=ExpectedVersion.ANY,
            )

        async def read_events() -> None:
            await store.get_events(aggregate_id, "TestAggregate")

        # Mix reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(write_event(i))
            tasks.append(read_events())

        # Should complete without errors
        await asyncio.gather(*tasks)

        # All events should be stored
        assert await store.get_event_count() == num_operations


# --- Multiple Aggregate Types Tests ---


class TestMultipleAggregateTypes:
    """Tests for handling multiple aggregate types for same aggregate ID."""

    @pytest.mark.asyncio
    async def test_different_types_same_aggregate_id(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test storing different aggregate types with same ID."""
        # This is unusual but the store should handle it
        order_event = OrderCreated(aggregate_id=aggregate_id, customer_name="John")
        user_event = UserRegistered(aggregate_id=aggregate_id, email="john@example.com")

        # Append as Order
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[order_event],
            expected_version=0,
        )

        # Append as User (same aggregate_id, different type)
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="User",
            events=[user_event],
            expected_version=0,  # Version is per aggregate_type
        )

        # Get Order events
        order_stream = await store.get_events(aggregate_id, "Order")
        assert len(order_stream.events) == 1
        assert order_stream.version == 1

        # Get User events
        user_stream = await store.get_events(aggregate_id, "User")
        assert len(user_stream.events) == 1
        assert user_stream.version == 1


# --- Import Tests ---


class TestImports:
    """Tests for verifying correct module exports."""

    def test_import_from_stores(self) -> None:
        """Test importing InMemoryEventStore from stores module."""
        from eventsource.stores import InMemoryEventStore

        assert InMemoryEventStore is not None
        store = InMemoryEventStore()
        assert store is not None

    def test_import_from_main(self) -> None:
        """Test importing InMemoryEventStore from main eventsource module."""
        from eventsource import InMemoryEventStore

        assert InMemoryEventStore is not None
        store = InMemoryEventStore()
        assert store is not None


# --- Edge Cases Tests ---


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_large_event_batch(self, store: InMemoryEventStore, aggregate_id: UUID) -> None:
        """Test appending a large batch of events."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(1000)]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1000
        assert await store.get_event_count() == 1000

    @pytest.mark.asyncio
    async def test_event_order_preserved(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test that event order is preserved in stream."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(10)]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        stream = await store.get_events(aggregate_id, "TestAggregate")

        for i, event in enumerate(stream.events):
            assert event.data == f"event_{i}"

    @pytest.mark.asyncio
    async def test_stream_id_without_colon(
        self, store: InMemoryEventStore, aggregate_id: UUID
    ) -> None:
        """Test read_stream with stream_id without colon (just UUID)."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        # Use just the UUID as stream_id
        stream_id = str(aggregate_id)
        stored_events = []
        async for stored in store.read_stream(stream_id):
            stored_events.append(stored)

        assert len(stored_events) == 1
