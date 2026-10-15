"""Unit tests for InMemoryOutboxRepository concurrency and OutboxEntry typed returns."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.ports.outbox import OutboxEntry
from tests.unit.adapters.test_memory_outbox._events import SampleEvent


class TestInMemoryOutboxRepositoryConcurrency:
    """Tests for concurrent access to InMemoryOutboxRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        return InMemoryOutboxRepository()

    @pytest.mark.asyncio
    async def test_concurrent_add_events_no_deadlock(self, repo: InMemoryOutboxRepository):
        num_events = 100

        async def add_event(i: int):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))

        tasks = [add_event(i) for i in range(num_events)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
        pending = await repo.get_pending_events(limit=200)
        assert len(pending) == num_events

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryOutboxRepository):
        num_operations = 50
        read_results: list[int] = []

        async def writer(i: int):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))

        async def reader():
            pending = await repo.get_pending_events()
            read_results.append(len(pending))

        tasks = []
        for i in range(num_operations):
            tasks.extend([writer(i), reader()])

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
        assert len(read_results) == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_publish_workflow(self, repo: InMemoryOutboxRepository):
        num_events = 20
        published_count = 0

        async def add_and_publish():
            nonlocal published_count
            outbox_id = await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field="test"))
            await repo.mark_published(outbox_id)
            published_count += 1

        tasks = [add_and_publish() for _ in range(num_events)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
        stats = await repo.get_stats()
        assert stats.published_count == num_events
        assert stats.pending_count == 0

    @pytest.mark.asyncio
    async def test_concurrent_retry_increment(self, repo: InMemoryOutboxRepository):
        outbox_id = await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field="test"))
        num_retries = 50

        async def increment():
            await repo.increment_retry(outbox_id, "Error")

        tasks = [increment() for _ in range(num_retries)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == num_retries

    @pytest.mark.asyncio
    async def test_concurrent_stats_during_updates(self, repo: InMemoryOutboxRepository):
        stats_results: list[dict] = []

        async def add_events():
            for i in range(20):
                await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))

        async def get_stats():
            for _ in range(10):
                stats_results.append(await repo.get_stats())
                await asyncio.sleep(0.001)

        await asyncio.wait_for(asyncio.gather(add_events(), get_stats()), timeout=5.0)
        assert len(stats_results) == 10
        for stats in stats_results:
            assert hasattr(stats, "pending_count")


class TestOutboxEntryTypedReturns:
    """Tests for typed returns and backward compatibility."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        return InMemoryOutboxRepository()

    @pytest.mark.asyncio
    async def test_get_pending_events_returns_outbox_entries(self, repo: InMemoryOutboxRepository):
        await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field="test_data"))
        entries = await repo.get_pending_events()
        assert len(entries) == 1
        assert isinstance(entries[0], OutboxEntry)
        assert hasattr(entries[0], "event_id")
        assert hasattr(entries[0], "event_type")
        assert hasattr(entries[0], "status")

    @pytest.mark.asyncio
    async def test_attribute_access_works(self, repo: InMemoryOutboxRepository):
        event = SampleEvent(aggregate_id=uuid4(), test_field="test_data")
        await repo.add_event(event)
        entry = (await repo.get_pending_events())[0]
        assert entry.event_id == event.event_id
        assert entry.event_type == "SampleEvent"
        assert entry.aggregate_type == "SampleAggregate"
        assert entry.status == "pending"
