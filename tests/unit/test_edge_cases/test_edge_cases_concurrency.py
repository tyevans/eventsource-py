"""Unit tests for concurrency and serialization edge cases."""

import asyncio
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion, collect
from tests.unit.test_edge_cases.test_edge_cases_events import _REGISTRY, EdgeTestEvent

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
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_ids = [uuid4() for _ in range(5)]

        async def write_events(agg_id: UUID):
            stream = StreamId(aggregate_id=agg_id, category="Test")
            for i in range(5):
                event = EdgeTestEvent(aggregate_id=agg_id, data=f"event_{i}")
                await store.append(stream, [event], ExpectedVersion.any_())

        async def read_events(agg_id: UUID):
            stream = StreamId(aggregate_id=agg_id, category="Test")
            for _ in range(5):
                await collect(store.read_stream(stream))

        # Mix reads and writes
        tasks = []
        for agg_id in aggregate_ids:
            tasks.append(write_events(agg_id))
            tasks.append(read_events(agg_id))

        await asyncio.gather(*tasks)

        # Each aggregate should have 5 events
        for agg_id in aggregate_ids:
            envelopes = await collect(
                store.read_stream(StreamId(aggregate_id=agg_id, category="Test"))
            )
            assert len(envelopes) == 5


# --- Serialization Edge Cases ---


class TestSerializationEdgeCases:
    """Tests for serialization edge cases."""

    @pytest.mark.asyncio
    async def test_event_with_special_characters(self):
        """Test event with special characters in data."""
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")

        event = EdgeTestEvent(
            aggregate_id=aggregate_id,
            data="Special chars: '\"\n\t\r\\",
        )
        # append raises on failure rather than returning success/conflict.
        await store.append(stream, [event], ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream))
        assert envelopes[0].event.data == "Special chars: '\"\n\t\r\\"

    @pytest.mark.asyncio
    async def test_event_with_unicode_characters(self):
        """Test event with Unicode characters."""
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")

        event = EdgeTestEvent(
            aggregate_id=aggregate_id,
            data="Unicode: 你好世界 ",
        )
        await store.append(stream, [event], ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream))
        assert "" in envelopes[0].event.data

    @pytest.mark.asyncio
    async def test_event_with_empty_string(self):
        """Test event with empty string data."""
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="")
        await store.append(stream, [event], ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream))
        assert envelopes[0].event.data == ""
