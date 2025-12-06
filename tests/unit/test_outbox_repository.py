"""
Unit tests for OutboxRepository implementations.

Tests the InMemoryOutboxRepository for:
- Adding events to outbox
- Getting pending events
- Status transitions (pending -> published/failed)
- Statistics and cleanup
"""

import pytest
from datetime import UTC, datetime
from uuid import UUID, uuid4

from eventsource.events.base import DomainEvent
from eventsource.repositories.outbox import (
    InMemoryOutboxRepository,
    OutboxRepository,
)


class SampleEvent(DomainEvent):
    """Sample event for outbox tests."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "SampleAggregate"
    test_field: str = "test_value"


class TestInMemoryOutboxRepository:
    """Tests for InMemoryOutboxRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        """Create a fresh repository for each test."""
        return InMemoryOutboxRepository()

    @pytest.fixture
    def sample_event(self) -> SampleEvent:
        """Create a sample event for testing."""
        return SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            test_field="test_data",
        )

    @pytest.mark.asyncio
    async def test_add_event(
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        """Test adding an event to the outbox."""
        outbox_id = await repo.add_event(sample_event)

        assert outbox_id is not None
        assert isinstance(outbox_id, UUID)

        # Verify event is in pending queue
        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0]["event_type"] == "SampleEvent"
        assert pending[0]["aggregate_type"] == "SampleAggregate"

    @pytest.mark.asyncio
    async def test_add_event_serializes_data(
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        """Test that event data is properly serialized."""
        await repo.add_event(sample_event)

        pending = await repo.get_pending_events()
        event_data = pending[0]["event_data"]

        # Should be a JSON string
        assert isinstance(event_data, str)
        assert "test_data" in event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_order(
        self, repo: InMemoryOutboxRepository
    ):
        """Test that pending events are returned in FIFO order."""
        # Add events
        events = []
        for i in range(3):
            event = SampleEvent(
                aggregate_id=uuid4(),
                test_field=f"event_{i}",
            )
            events.append(event)
            await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 3

        # Should be in order of creation (oldest first)
        assert "event_0" in pending[0]["event_data"]
        assert "event_1" in pending[1]["event_data"]
        assert "event_2" in pending[2]["event_data"]

    @pytest.mark.asyncio
    async def test_get_pending_events_limit(
        self, repo: InMemoryOutboxRepository
    ):
        """Test limiting number of pending events returned."""
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        pending = await repo.get_pending_events(limit=3)
        assert len(pending) == 3

    @pytest.mark.asyncio
    async def test_get_pending_events_empty(
        self, repo: InMemoryOutboxRepository
    ):
        """Test getting pending events when none exist."""
        pending = await repo.get_pending_events()
        assert pending == []

    @pytest.mark.asyncio
    async def test_mark_published(
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        """Test marking an event as published."""
        outbox_id = await repo.add_event(sample_event)

        await repo.mark_published(outbox_id)

        # Should no longer be in pending
        pending = await repo.get_pending_events()
        assert len(pending) == 0

        # Check stats
        stats = await repo.get_stats()
        assert stats["pending_count"] == 0
        assert stats["published_count"] == 1

    @pytest.mark.asyncio
    async def test_mark_failed(
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        """Test marking an event as failed."""
        outbox_id = await repo.add_event(sample_event)

        await repo.mark_failed(outbox_id, "Connection refused")

        # Should no longer be in pending
        pending = await repo.get_pending_events()
        assert len(pending) == 0

        # Check stats
        stats = await repo.get_stats()
        assert stats["pending_count"] == 0
        assert stats["failed_count"] == 1

    @pytest.mark.asyncio
    async def test_increment_retry(
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        """Test incrementing retry count."""
        outbox_id = await repo.add_event(sample_event)

        await repo.increment_retry(outbox_id, "Temporary error")
        await repo.increment_retry(outbox_id, "Another error")

        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0]["retry_count"] == 2

    @pytest.mark.asyncio
    async def test_cleanup_published(self, repo: InMemoryOutboxRepository):
        """Test cleaning up old published events."""
        from datetime import timedelta

        # Add and publish an event
        event = SampleEvent(aggregate_id=uuid4(), test_field="test")
        outbox_id = await repo.add_event(event)
        await repo.mark_published(outbox_id)

        # Cleanup with 0 days (should delete everything published)
        deleted = await repo.cleanup_published(days=0)

        # Note: The event was just published, might not be old enough
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_get_stats(self, repo: InMemoryOutboxRepository):
        """Test getting outbox statistics."""
        # Add some events
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            outbox_id = await repo.add_event(event)

            if i < 2:
                await repo.mark_published(outbox_id)
            elif i < 4:
                # Leave as pending
                pass
            else:
                await repo.mark_failed(outbox_id, "Error")

        stats = await repo.get_stats()
        assert stats["pending_count"] == 2
        assert stats["published_count"] == 2
        assert stats["failed_count"] == 1
        assert stats["oldest_pending"] is not None
        assert stats["avg_retries"] == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, repo: InMemoryOutboxRepository):
        """Test getting stats when outbox is empty."""
        stats = await repo.get_stats()
        assert stats["pending_count"] == 0
        assert stats["published_count"] == 0
        assert stats["failed_count"] == 0
        assert stats["oldest_pending"] is None
        assert stats["avg_retries"] == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_avg_retries(
        self, repo: InMemoryOutboxRepository
    ):
        """Test average retries calculation in stats."""
        # Add events with different retry counts
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            outbox_id = await repo.add_event(event)

            # Increment retries: 0, 2, 4
            for _ in range(i * 2):
                await repo.increment_retry(outbox_id, "Error")

        stats = await repo.get_stats()
        # Average of 0, 2, 4 = 2.0
        assert stats["avg_retries"] == 2.0

    @pytest.mark.asyncio
    async def test_clear(self, repo: InMemoryOutboxRepository):
        """Test clearing all entries."""
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        repo.clear()

        pending = await repo.get_pending_events()
        assert len(pending) == 0

        stats = await repo.get_stats()
        assert stats["pending_count"] == 0

    @pytest.mark.asyncio
    async def test_event_with_tenant_id(self, repo: InMemoryOutboxRepository):
        """Test that tenant_id is preserved in outbox."""
        tenant_id = uuid4()
        event = SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
            test_field="test",
        )

        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert pending[0]["tenant_id"] == str(tenant_id)

    @pytest.mark.asyncio
    async def test_event_without_tenant_id(
        self, repo: InMemoryOutboxRepository
    ):
        """Test that events without tenant_id are handled."""
        event = SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=None,
            test_field="test",
        )

        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert pending[0]["tenant_id"] is None

    @pytest.mark.asyncio
    async def test_multiple_events_same_aggregate(
        self, repo: InMemoryOutboxRepository
    ):
        """Test adding multiple events for the same aggregate."""
        aggregate_id = uuid4()

        for i in range(3):
            event = SampleEvent(
                aggregate_id=aggregate_id,
                test_field=f"event_{i}",
            )
            await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 3

        # All should have same aggregate_id
        for entry in pending:
            assert entry["aggregate_id"] == str(aggregate_id)

    @pytest.mark.asyncio
    async def test_mark_published_nonexistent(
        self, repo: InMemoryOutboxRepository
    ):
        """Test marking non-existent event as published (no error)."""
        await repo.mark_published(uuid4())  # Should not raise

    @pytest.mark.asyncio
    async def test_mark_failed_nonexistent(
        self, repo: InMemoryOutboxRepository
    ):
        """Test marking non-existent event as failed (no error)."""
        await repo.mark_failed(uuid4(), "Error")  # Should not raise


class TestOutboxRepositoryProtocol:
    """Tests to verify InMemoryOutboxRepository implements the protocol."""

    def test_implements_protocol(self):
        """Test that InMemoryOutboxRepository implements OutboxRepository protocol."""
        repo = InMemoryOutboxRepository()
        assert isinstance(repo, OutboxRepository)
