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
        repo = InMemoryCheckpointRepository()
        projection_name = "TestProjection"

        async def update_checkpoint():
            for _ in range(10):
                await repo.update_checkpoint(projection_name, uuid4(), "EdgeTestEvent")

        await asyncio.gather(*[update_checkpoint() for _ in range(5)])
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 50

    @pytest.mark.asyncio
    async def test_concurrent_dlq_operations(self):
        repo = InMemoryDLQRepository()

        async def add_and_mark():
            await repo.add_failed_event(
                uuid4(), "TestProjection", "EdgeTestEvent", {"test": "data"}, Exception("Error")
            )

        await asyncio.gather(*[add_and_mark() for _ in range(10)])
        stats = await repo.get_failure_stats()
        assert stats.total_failed == 10

    @pytest.mark.asyncio
    async def test_concurrent_event_bus_operations(self):
        bus = InMemoryEventBus()
        received = []
        lock = asyncio.Lock()

        async def handler(event: DomainEvent):
            async with lock:
                received.append(event)

        bus.subscribe(EdgeTestEvent, handler)

        async def publish_event():
            await bus.publish([EdgeTestEvent(aggregate_id=uuid4(), data="concurrent")])

        await asyncio.gather(*[publish_event() for _ in range(10)])
        assert len(received) == 10

    @pytest.mark.asyncio
    async def test_concurrent_outbox_operations(self):
        repo = InMemoryOutboxRepository()

        async def add_event(i: int):
            await repo.add_event(EdgeTestEvent(aggregate_id=uuid4(), data=f"outbox_{i}"))

        await asyncio.gather(*[add_event(i) for i in range(20)])
        stats = await repo.get_stats()
        assert stats.pending_count == 20

    @pytest.mark.asyncio
    async def test_concurrent_outbox_mark_published(self):
        repo = InMemoryOutboxRepository()
        outbox_ids = [
            await repo.add_event(EdgeTestEvent(aggregate_id=uuid4(), data=f"event_{i}"))
            for i in range(10)
        ]
        await asyncio.gather(*[repo.mark_published(oid) for oid in outbox_ids])
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 10

    @pytest.mark.asyncio
    async def test_concurrent_event_store_reads_writes(self):
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

        tasks = []
        for agg_id in aggregate_ids:
            tasks.extend([write_events(agg_id), read_events(agg_id)])
        await asyncio.gather(*tasks)

        for agg_id in aggregate_ids:
            envelopes = await collect(
                store.read_stream(StreamId(aggregate_id=agg_id, category="Test"))
            )
            assert len(envelopes) == 5


class TestSerializationEdgeCases:
    """Tests for serialization edge cases."""

    @pytest.mark.asyncio
    async def test_event_with_special_characters(self):
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")
        event = EdgeTestEvent(aggregate_id=aggregate_id, data="Special chars: '\"\n\t\r\\")
        await store.append(stream, [event], ExpectedVersion.no_stream())
        envelopes = await collect(store.read_stream(stream))
        assert envelopes[0].event.data == "Special chars: '\"\n\t\r\\"

    @pytest.mark.asyncio
    async def test_event_with_unicode_characters(self):
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")
        event = EdgeTestEvent(aggregate_id=aggregate_id, data="Unicode: 你好世界 ")
        await store.append(stream, [event], ExpectedVersion.no_stream())
        envelopes = await collect(store.read_stream(stream))
        assert "" in envelopes[0].event.data

    @pytest.mark.asyncio
    async def test_event_with_empty_string(self):
        store = InMemoryEventStore(event_registry=_REGISTRY)
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")
        event = EdgeTestEvent(aggregate_id=aggregate_id, data="")
        await store.append(stream, [event], ExpectedVersion.no_stream())
        envelopes = await collect(store.read_stream(stream))
        assert envelopes[0].event.data == ""
