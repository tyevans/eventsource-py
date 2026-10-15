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
        return SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            test_field="test_data",
        )

    @pytest.mark.asyncio
    async def test_add_event(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        assert outbox_id is not None
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
        event_data = pending[0].event_data
        assert isinstance(event_data, str)
        assert "test_data" in event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_order(self, repo: InMemoryOutboxRepository):
        events = []
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            events.append(event)
            await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 3
        assert "event_0" in pending[0].event_data
        assert "event_1" in pending[1].event_data
        assert "event_2" in pending[2].event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_limit(self, repo: InMemoryOutboxRepository):
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        pending = await repo.get_pending_events(limit=3)
        assert len(pending) == 3

    @pytest.mark.asyncio
    async def test_get_pending_events_empty(self, repo: InMemoryOutboxRepository):
        pending = await repo.get_pending_events()
        assert pending == []

    @pytest.mark.asyncio
    async def test_mark_published(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        await repo.mark_published(outbox_id)

        pending = await repo.get_pending_events()
        assert len(pending) == 0

        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 1

    @pytest.mark.asyncio
    async def test_mark_failed(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        await repo.mark_failed(outbox_id, "Connection refused")

        pending = await repo.get_pending_events()
        assert len(pending) == 0

        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.failed_count == 1

    @pytest.mark.asyncio
    async def test_increment_retry(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        await repo.increment_retry(outbox_id, "Temporary error")
        await repo.increment_retry(outbox_id, "Another error")

        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_published(self, repo: InMemoryOutboxRepository):
        event = SampleEvent(aggregate_id=uuid4(), test_field="test")
        outbox_id = await repo.add_event(event)
        await repo.mark_published(outbox_id)

        deleted = await repo.cleanup_published(days=0)
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_get_stats(self, repo: InMemoryOutboxRepository):
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            outbox_id = await repo.add_event(event)
            if i < 2:
                await repo.mark_published(outbox_id)
            elif i < 4:
                pass
            else:
                await repo.mark_failed(outbox_id, "Error")

        stats = await repo.get_stats()
        assert stats.pending_count == 2
        assert stats.published_count == 2
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, repo: InMemoryOutboxRepository):
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.oldest_pending is None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_avg_retries(self, repo: InMemoryOutboxRepository):
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            outbox_id = await repo.add_event(event)
            for _ in range(i * 2):
                await repo.increment_retry(outbox_id, "Error")

        stats = await repo.get_stats()
        assert stats.avg_retries == 2.0

    @pytest.mark.asyncio
    async def test_clear(self, repo: InMemoryOutboxRepository):
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        await repo.clear()
        pending = await repo.get_pending_events()
        assert len(pending) == 0
        stats = await repo.get_stats()
        assert stats.pending_count == 0

    @pytest.mark.asyncio
    async def test_event_with_tenant_id(self, repo: InMemoryOutboxRepository):
        tenant_id = uuid4()
        event = SampleEvent(aggregate_id=uuid4(), tenant_id=tenant_id, test_field="test")
        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert pending[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_event_without_tenant_id(self, repo: InMemoryOutboxRepository):
        event = SampleEvent(aggregate_id=uuid4(), tenant_id=None, test_field="test")
        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert pending[0].tenant_id is None

    @pytest.mark.asyncio
    async def test_multiple_events_same_aggregate(self, repo: InMemoryOutboxRepository):
        aggregate_id = uuid4()
        for i in range(3):
            event = SampleEvent(aggregate_id=aggregate_id, test_field=f"event_{i}")
            await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 3
        for entry in pending:
            assert entry.aggregate_id == aggregate_id

    @pytest.mark.asyncio
    async def test_mark_published_nonexistent(self, repo: InMemoryOutboxRepository):
        await repo.mark_published(uuid4())

    @pytest.mark.asyncio
    async def test_mark_failed_nonexistent(self, repo: InMemoryOutboxRepository):
        await repo.mark_failed(uuid4(), "Error")


class TestOutboxRepositoryProtocol:
    """Tests to verify InMemoryOutboxRepository implements the protocol."""

    def test_implements_protocol(self):
        repo = InMemoryOutboxRepository()
        assert isinstance(repo, OutboxRepository)
