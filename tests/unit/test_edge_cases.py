"""
Unit tests for edge cases across various components.

These tests focus on:
- Boundary conditions
- Error paths
- Concurrent access scenarios
- Unusual data patterns
- Edge cases not covered by main tests
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.repositories.outbox import InMemoryOutboxRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.stores.interface import (
    ReadDirection,
    ReadOptions,
)

# --- Test Event Classes ---


class EdgeTestEvent(DomainEvent):
    """Simple test event."""

    event_type: str = "EdgeTestEvent"
    aggregate_type: str = "Test"
    data: str = ""


class SampleOrderEvent(DomainEvent):
    """Order event for testing."""

    event_type: str = "SampleOrderEvent"
    aggregate_type: str = "Order"
    order_data: str = ""


# --- InMemoryEventStore Edge Cases ---


class TestInMemoryEventStoreEdgeCases:
    """Edge case tests for InMemoryEventStore."""

    @pytest.fixture
    def store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.mark.asyncio
    async def test_get_events_with_to_timestamp_filter(self, store: InMemoryEventStore):
        """Test filtering events by to_timestamp."""
        aggregate_id = uuid4()

        # Create events with different timestamps
        event1 = EdgeTestEvent(aggregate_id=aggregate_id, data="first")
        await store.append_events(aggregate_id, "Test", [event1], 0)

        # Get events with future to_timestamp
        future_time = datetime.now(UTC) + timedelta(hours=1)
        stream = await store.get_events(aggregate_id, "Test", to_timestamp=future_time)
        assert len(stream.events) == 1

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_timestamp_filter(self, store: InMemoryEventStore):
        """Test get_events_by_type with timestamp filter."""
        aggregate_id = uuid4()
        old_timestamp = datetime.now(UTC) - timedelta(hours=1)

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(aggregate_id, "Test", [event], 0)

        # Get events created after old timestamp
        events = await store.get_events_by_type("Test", from_timestamp=old_timestamp)
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_tenant_filter(self, store: InMemoryEventStore):
        """Test get_events_by_type with tenant_id filter."""
        aggregate_id = uuid4()
        tenant_id = uuid4()
        other_tenant = uuid4()

        event1 = EdgeTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            data="tenant1",
        )
        event2 = EdgeTestEvent(
            aggregate_id=uuid4(),
            tenant_id=other_tenant,
            data="tenant2",
        )

        await store.append_events(event1.aggregate_id, "Test", [event1], 0)
        await store.append_events(event2.aggregate_id, "Test", [event2], 0)

        # Filter by tenant_id
        events = await store.get_events_by_type("Test", tenant_id=tenant_id)
        assert len(events) == 1
        assert events[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_read_stream_backward(self, store: InMemoryEventStore):
        """Test reading stream in backward direction."""
        aggregate_id = uuid4()

        events = [EdgeTestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        await store.append_events(aggregate_id, "Test", events, 0)

        stream_id = f"{aggregate_id}:Test"
        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = [se async for se in store.read_stream(stream_id, options)]

        assert len(stored_events) == 5
        # Should be in reverse order (last event first)
        assert "event_4" in stored_events[0].event.data
        assert "event_0" in stored_events[4].event.data

    @pytest.mark.asyncio
    async def test_read_stream_without_aggregate_type(self, store: InMemoryEventStore):
        """Test reading stream with ID only (no aggregate type in stream_id)."""
        aggregate_id = uuid4()

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(aggregate_id, "Test", [event], 0)

        # Use just the aggregate ID as stream_id
        stream_id = str(aggregate_id)
        stored_events = [se async for se in store.read_stream(stream_id)]

        # Should return events (though may not filter by type)
        assert len(stored_events) >= 1

    @pytest.mark.asyncio
    async def test_read_stream_with_from_timestamp(self, store: InMemoryEventStore):
        """Test reading stream with from_timestamp filter."""
        aggregate_id = uuid4()

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(aggregate_id, "Test", [event], 0)

        stream_id = f"{aggregate_id}:Test"
        past_time = datetime.now(UTC) - timedelta(hours=1)
        options = ReadOptions(from_timestamp=past_time)

        stored_events = [se async for se in store.read_stream(stream_id, options)]
        assert len(stored_events) == 1

    @pytest.mark.asyncio
    async def test_read_stream_with_to_timestamp(self, store: InMemoryEventStore):
        """Test reading stream with to_timestamp filter."""
        aggregate_id = uuid4()

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(aggregate_id, "Test", [event], 0)

        stream_id = f"{aggregate_id}:Test"
        future_time = datetime.now(UTC) + timedelta(hours=1)
        options = ReadOptions(to_timestamp=future_time)

        stored_events = [se async for se in store.read_stream(stream_id, options)]
        assert len(stored_events) == 1

    @pytest.mark.asyncio
    async def test_read_all_with_from_timestamp(self, store: InMemoryEventStore):
        """Test read_all with from_timestamp filter."""
        aggregate_id = uuid4()

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(aggregate_id, "Test", [event], 0)

        past_time = datetime.now(UTC) - timedelta(hours=1)
        options = ReadOptions(from_timestamp=past_time)

        stored_events = [se async for se in store.read_all(options)]
        assert len(stored_events) == 1

    @pytest.mark.asyncio
    async def test_read_all_with_to_timestamp(self, store: InMemoryEventStore):
        """Test read_all with to_timestamp filter."""
        aggregate_id = uuid4()

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append_events(aggregate_id, "Test", [event], 0)

        future_time = datetime.now(UTC) + timedelta(hours=1)
        options = ReadOptions(to_timestamp=future_time)

        stored_events = [se async for se in store.read_all(options)]
        assert len(stored_events) == 1

    @pytest.mark.asyncio
    async def test_read_all_backward_direction(self, store: InMemoryEventStore):
        """Test read_all in backward direction."""
        aggregate_id = uuid4()

        events = [EdgeTestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(aggregate_id, "Test", events, 0)

        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = [se async for se in store.read_all(options)]

        assert len(stored_events) == 3
        # Last event should be first in backward direction
        assert "event_2" in stored_events[0].event.data

    @pytest.mark.asyncio
    async def test_read_all_stream_position_calculation(self, store: InMemoryEventStore):
        """Test that stream position is correctly calculated in read_all."""
        aggregate_id = uuid4()

        events = [EdgeTestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append_events(aggregate_id, "Test", events, 0)

        stored_events = [se async for se in store.read_all()]

        assert len(stored_events) == 3
        # Each event should have a stream position
        for se in stored_events:
            assert se.stream_position >= 0

    @pytest.mark.asyncio
    async def test_concurrent_appends(self, store: InMemoryEventStore):
        """Test concurrent appends to different aggregates."""
        aggregate_ids = [uuid4() for _ in range(10)]

        async def append_event(agg_id: UUID):
            event = EdgeTestEvent(aggregate_id=agg_id, data="concurrent")
            await store.append_events(agg_id, "Test", [event], 0)

        await asyncio.gather(*[append_event(agg_id) for agg_id in aggregate_ids])

        # Verify all events were stored
        for agg_id in aggregate_ids:
            stream = await store.get_events(agg_id, "Test")
            assert len(stream.events) == 1


# --- InMemoryEventBus Edge Cases ---


class TestInMemoryEventBusEdgeCases:
    """Edge case tests for InMemoryEventBus."""

    @pytest.fixture
    def bus(self) -> InMemoryEventBus:
        return InMemoryEventBus()

    @pytest.mark.asyncio
    async def test_handler_exception_isolation(self, bus: InMemoryEventBus):
        """Test that one handler's exception doesn't affect others."""
        received = []

        async def failing_handler(event: DomainEvent):
            raise RuntimeError("Handler failure")

        async def working_handler(event: DomainEvent):
            received.append(event)

        bus.subscribe(EdgeTestEvent, failing_handler)
        bus.subscribe(EdgeTestEvent, working_handler)

        event = EdgeTestEvent(aggregate_id=uuid4(), data="test")
        await bus.publish([event])

        # Working handler should still receive the event
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_publish_empty_event_list(self, bus: InMemoryEventBus):
        """Test publishing an empty list of events."""
        received = []

        async def handler(event: DomainEvent):
            received.append(event)

        bus.subscribe(EdgeTestEvent, handler)
        await bus.publish([])

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_sync_handler(self, bus: InMemoryEventBus):
        """Test sync (non-async) handler."""
        received = []

        def sync_handler(event: DomainEvent):
            received.append(event)

        bus.subscribe(EdgeTestEvent, sync_handler)

        event = EdgeTestEvent(aggregate_id=uuid4(), data="test")
        await bus.publish([event])

        assert len(received) == 1

    def test_unsubscribe_returns_false_for_unknown_handler(self, bus: InMemoryEventBus):
        """Test that unsubscribe returns False for unknown handler."""

        async def handler(event: DomainEvent):
            pass

        result = bus.unsubscribe(EdgeTestEvent, handler)
        assert result is False

    def test_subscribe_all_wildcard_handlers(self, bus: InMemoryEventBus):
        """Test wildcard subscription receives all events."""
        received = []

        async def handler(event: DomainEvent):
            received.append(event)

        bus.subscribe_to_all_events(handler)

        # This is just testing the subscription mechanism
        assert bus.get_wildcard_subscriber_count() == 1


# --- InMemoryCheckpointRepository Edge Cases ---


class TestCheckpointRepositoryEdgeCases:
    """Edge case tests for checkpoint repository."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_lag_metrics_with_event_types(self, repo: InMemoryCheckpointRepository):
        """Test get_lag_metrics with event_types parameter."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "EdgeTestEvent")

        # Even though event_types is ignored in in-memory impl,
        # it should still work
        result = await repo.get_lag_metrics(projection_name, event_types=["EdgeTestEvent"])
        assert result is not None
        assert result.projection_name == projection_name


# --- InMemoryDLQRepository Edge Cases ---


class TestDLQRepositoryEdgeCases:
    """Edge case tests for DLQ repository."""

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_delete_resolved_events_with_none_resolved_at(self, repo: InMemoryDLQRepository):
        """Test delete_resolved_events doesn't fail with None resolved_at."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id,
            "TestProjection",
            "EdgeTestEvent",
            {"test": "data"},
            Exception("Test error"),
        )

        # Mark resolved (this sets resolved_at)
        await repo.mark_resolved(1, "admin")

        # Try to delete (should work without error)
        deleted = await repo.delete_resolved_events(older_than_days=0)
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts_multiple_projections(
        self, repo: InMemoryDLQRepository
    ):
        """Test failure counts with multiple projections."""
        for i in range(3):
            await repo.add_failed_event(
                uuid4(),
                f"Projection{i}",
                "EdgeTestEvent",
                {"test": "data"},
                Exception(f"Error {i}"),
            )

        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 3

    @pytest.mark.asyncio
    async def test_failure_stats_with_no_active_entries(self, repo: InMemoryDLQRepository):
        """Test failure stats when all entries are resolved."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id,
            "TestProjection",
            "EdgeTestEvent",
            {"test": "data"},
            Exception("Test error"),
        )

        await repo.mark_resolved(1, "admin")

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0


# --- InMemoryOutboxRepository Edge Cases ---


class TestOutboxRepositoryEdgeCases:
    """Edge case tests for outbox repository."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        return InMemoryOutboxRepository()

    @pytest.mark.asyncio
    async def test_cleanup_published_no_events(self, repo: InMemoryOutboxRepository):
        """Test cleanup when no published events exist."""
        deleted = await repo.cleanup_published(days=0)
        assert deleted == 0

    @pytest.mark.asyncio
    async def test_increment_retry_nonexistent(self, repo: InMemoryOutboxRepository):
        """Test increment_retry on non-existent entry."""
        # Should not raise
        await repo.increment_retry(uuid4(), "Error")


# --- Concurrent Access Tests ---


class TestConcurrentAccess:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_checkpoint_updates(self):
        """Test concurrent updates to the same checkpoint."""
        repo = InMemoryCheckpointRepository()
        projection_name = "TestProjection"

        async def update_checkpoint():
            for _ in range(10):
                event_id = uuid4()
                await repo.update_checkpoint(projection_name, event_id, "EdgeTestEvent")

        await asyncio.gather(*[update_checkpoint() for _ in range(5)])

        # Should have processed 50 events total
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 50

    @pytest.mark.asyncio
    async def test_concurrent_dlq_operations(self):
        """Test concurrent DLQ operations."""
        repo = InMemoryDLQRepository()

        async def add_and_mark():
            event_id = uuid4()
            await repo.add_failed_event(
                event_id,
                "TestProjection",
                "EdgeTestEvent",
                {"test": "data"},
                Exception("Error"),
            )

        await asyncio.gather(*[add_and_mark() for _ in range(10)])

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 10

    @pytest.mark.asyncio
    async def test_concurrent_event_bus_operations(self):
        """Test concurrent publish operations."""
        bus = InMemoryEventBus()
        received = []
        lock = asyncio.Lock()

        async def handler(event: DomainEvent):
            async with lock:
                received.append(event)

        bus.subscribe(EdgeTestEvent, handler)

        async def publish_event():
            event = EdgeTestEvent(aggregate_id=uuid4(), data="concurrent")
            await bus.publish([event])

        await asyncio.gather(*[publish_event() for _ in range(10)])

        # All events should be received
        assert len(received) == 10

    @pytest.mark.asyncio
    async def test_concurrent_outbox_operations(self):
        """Test concurrent outbox add and get operations."""
        repo = InMemoryOutboxRepository()

        async def add_event(i: int):
            event = EdgeTestEvent(aggregate_id=uuid4(), data=f"outbox_{i}")
            await repo.add_event(event)

        # Add 20 events concurrently
        await asyncio.gather(*[add_event(i) for i in range(20)])

        # Verify all events were added
        stats = await repo.get_stats()
        assert stats.pending_count == 20

    @pytest.mark.asyncio
    async def test_concurrent_outbox_mark_published(self):
        """Test concurrent mark_published operations."""
        repo = InMemoryOutboxRepository()

        # Add events first
        outbox_ids = []
        for i in range(10):
            event = EdgeTestEvent(aggregate_id=uuid4(), data=f"event_{i}")
            outbox_id = await repo.add_event(event)
            outbox_ids.append(outbox_id)

        # Mark published concurrently
        await asyncio.gather(*[repo.mark_published(oid) for oid in outbox_ids])

        # All should be published
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 10

    @pytest.mark.asyncio
    async def test_concurrent_event_store_reads_writes(self):
        """Test concurrent reads and writes to event store with asyncio.Lock."""
        store = InMemoryEventStore()
        aggregate_ids = [uuid4() for _ in range(5)]

        async def write_events(agg_id: uuid4):
            for i in range(5):
                event = EdgeTestEvent(aggregate_id=agg_id, data=f"event_{i}")
                await store.append_events(agg_id, "Test", [event], expected_version=-1)  # ANY

        async def read_events(agg_id: uuid4):
            for _ in range(5):
                await store.get_events(agg_id, "Test")

        # Mix reads and writes
        tasks = []
        for agg_id in aggregate_ids:
            tasks.append(write_events(agg_id))
            tasks.append(read_events(agg_id))

        await asyncio.gather(*tasks)

        # Each aggregate should have 5 events
        for agg_id in aggregate_ids:
            stream = await store.get_events(agg_id, "Test")
            assert len(stream.events) == 5


# --- Serialization Edge Cases ---


class TestSerializationEdgeCases:
    """Tests for serialization edge cases."""

    @pytest.mark.asyncio
    async def test_event_with_special_characters(self):
        """Test event with special characters in data."""
        store = InMemoryEventStore()
        aggregate_id = uuid4()

        event = EdgeTestEvent(
            aggregate_id=aggregate_id,
            data="Special chars: '\"\n\t\r\\",
        )
        result = await store.append_events(aggregate_id, "Test", [event], 0)
        assert result.success

        stream = await store.get_events(aggregate_id, "Test")
        assert stream.events[0].data == "Special chars: '\"\n\t\r\\"

    @pytest.mark.asyncio
    async def test_event_with_unicode_characters(self):
        """Test event with Unicode characters."""
        store = InMemoryEventStore()
        aggregate_id = uuid4()

        event = EdgeTestEvent(
            aggregate_id=aggregate_id,
            data="Unicode: 你好世界 ",
        )
        result = await store.append_events(aggregate_id, "Test", [event], 0)
        assert result.success

        stream = await store.get_events(aggregate_id, "Test")
        assert "" in stream.events[0].data

    @pytest.mark.asyncio
    async def test_event_with_empty_string(self):
        """Test event with empty string data."""
        store = InMemoryEventStore()
        aggregate_id = uuid4()

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="")
        result = await store.append_events(aggregate_id, "Test", [event], 0)
        assert result.success

        stream = await store.get_events(aggregate_id, "Test")
        assert stream.events[0].data == ""
