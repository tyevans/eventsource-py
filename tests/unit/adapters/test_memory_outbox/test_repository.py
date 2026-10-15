"""Unit tests for InMemoryOutboxRepository core operations and protocol conformance."""

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.ports.outbox import OutboxRepository
from tests.unit.adapters.test_memory_outbox._events import SampleEvent


class TestInMemoryOutboxRepository:
    """Tests for InMemoryOutboxRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        return InMemoryOutboxRepository()

    @pytest.fixture
    def sample_event(self) -> SampleEvent:
        return SampleEvent(aggregate_id=uuid4(), tenant_id=uuid4(), test_field="test_data")

    @pytest.mark.asyncio
    async def test_add_event(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        assert isinstance(outbox_id, UUID)

        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_type == "SampleEvent"
        assert pending[0].aggregate_type == "SampleAggregate"

    @pytest.mark.asyncio
    async def test_add_event_serializes_data(
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        await repo.add_event(sample_event)
        pending = await repo.get_pending_events()
        assert isinstance(pending[0].event_data, str)
        assert "test_data" in pending[0].event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_order(self, repo: InMemoryOutboxRepository):
        for i in range(3):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))

        pending = await repo.get_pending_events()
        assert len(pending) == 3
        for i in range(3):
            assert f"event_{i}" in pending[i].event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_limit(self, repo: InMemoryOutboxRepository):
        for i in range(5):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))
        assert len(await repo.get_pending_events(limit=3)) == 3

    @pytest.mark.asyncio
    async def test_get_pending_events_empty(self, repo: InMemoryOutboxRepository):
        assert await repo.get_pending_events() == []

    @pytest.mark.asyncio
    async def test_mark_published(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        await repo.mark_published(outbox_id)

        assert len(await repo.get_pending_events()) == 0
        stats = await repo.get_stats()
        assert stats.pending_count == 0 and stats.published_count == 1

    @pytest.mark.asyncio
    async def test_mark_failed(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        await repo.mark_failed(outbox_id, "Connection refused")

        assert len(await repo.get_pending_events()) == 0
        stats = await repo.get_stats()
        assert stats.pending_count == 0 and stats.failed_count == 1

    @pytest.mark.asyncio
    async def test_increment_retry(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        await repo.increment_retry(outbox_id, "Temporary error")
        await repo.increment_retry(outbox_id, "Another error")

        pending = await repo.get_pending_events()
        assert len(pending) == 1 and pending[0].retry_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_published(self, repo: InMemoryOutboxRepository):
        outbox_id = await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field="test"))
        await repo.mark_published(outbox_id)
        assert await repo.cleanup_published(days=0) >= 0

    @pytest.mark.asyncio
    async def test_get_stats(self, repo: InMemoryOutboxRepository):
        for i in range(5):
            outbox_id = await repo.add_event(
                SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            )
            if i < 2:
                await repo.mark_published(outbox_id)
            elif i >= 4:
                await repo.mark_failed(outbox_id, "Error")

        stats = await repo.get_stats()
        assert (stats.pending_count, stats.published_count, stats.failed_count) == (2, 2, 1)
        assert stats.oldest_pending is not None and stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, repo: InMemoryOutboxRepository):
        stats = await repo.get_stats()
        assert (stats.pending_count, stats.published_count, stats.failed_count) == (0, 0, 0)
        assert stats.oldest_pending is None and stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_avg_retries(self, repo: InMemoryOutboxRepository):
        for i in range(3):
            outbox_id = await repo.add_event(
                SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            )
            for _ in range(i * 2):
                await repo.increment_retry(outbox_id, "Error")

        assert (await repo.get_stats()).avg_retries == 2.0

    @pytest.mark.asyncio
    async def test_clear(self, repo: InMemoryOutboxRepository):
        for i in range(3):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))

        await repo.clear()
        assert len(await repo.get_pending_events()) == 0
        assert (await repo.get_stats()).pending_count == 0

    @pytest.mark.asyncio
    async def test_event_with_tenant_id(self, repo: InMemoryOutboxRepository):
        tenant_id = uuid4()
        await repo.add_event(
            SampleEvent(aggregate_id=uuid4(), tenant_id=tenant_id, test_field="test")
        )
        assert (await repo.get_pending_events())[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_event_without_tenant_id(self, repo: InMemoryOutboxRepository):
        await repo.add_event(SampleEvent(aggregate_id=uuid4(), tenant_id=None, test_field="test"))
        assert (await repo.get_pending_events())[0].tenant_id is None

    @pytest.mark.asyncio
    async def test_multiple_events_same_aggregate(self, repo: InMemoryOutboxRepository):
        aggregate_id = uuid4()
        for i in range(3):
            await repo.add_event(SampleEvent(aggregate_id=aggregate_id, test_field=f"event_{i}"))

        pending = await repo.get_pending_events()
        assert len(pending) == 3 and all(e.aggregate_id == aggregate_id for e in pending)

    @pytest.mark.asyncio
    async def test_mark_published_nonexistent(self, repo: InMemoryOutboxRepository):
        await repo.mark_published(uuid4())

    @pytest.mark.asyncio
    async def test_mark_failed_nonexistent(self, repo: InMemoryOutboxRepository):
        await repo.mark_failed(uuid4(), "Error")


class TestOutboxRepositoryProtocol:
    """Tests to verify InMemoryOutboxRepository implements the protocol."""

    def test_implements_protocol(self):
        assert isinstance(InMemoryOutboxRepository(), OutboxRepository)
