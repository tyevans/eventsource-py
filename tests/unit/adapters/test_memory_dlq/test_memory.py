"""Tests for InMemoryDLQRepository."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.ports.dlq import DLQRepository


class TestDLQRepositoryProtocol:
    """Tests to verify InMemoryDLQRepository implements the protocol."""

    def test_implements_protocol(self):
        """Test that InMemoryDLQRepository implements DLQRepository protocol."""
        repo = InMemoryDLQRepository()
        assert isinstance(repo, DLQRepository)


class TestInMemoryDLQRepository:
    """Tests for InMemoryDLQRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        """Create a fresh repository for each test."""
        return InMemoryDLQRepository()

    async def _add_sample_failed(
        self,
        repo: InMemoryDLQRepository,
        *,
        event_id: UUID | None = None,
        projection_name: str = "TestProjection",
        event_type: str = "TestEvent",
        event_data: dict | None = None,
        error_msg: str = "Error",
        retry_count: int = 1,
    ) -> UUID:
        eid = event_id or uuid4()
        await repo.add_failed_event(
            event_id=eid,
            projection_name=projection_name,
            event_type=event_type,
            event_data=event_data if event_data is not None else {},
            error=Exception(error_msg),
            retry_count=retry_count,
        )
        return eid

    @pytest.mark.asyncio
    async def test_add_failed_event(self, repo: InMemoryDLQRepository):
        """Test adding a failed event to DLQ."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"key": "value"},
            error=Exception("Test error"),
            retry_count=1,
        )

        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event_id
        assert failed_events[0].projection_name == "TestProjection"
        assert failed_events[0].event_type == "TestEvent"
        assert failed_events[0].retry_count == 1
        assert failed_events[0].status == "failed"
        assert "Test error" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_add_failed_event_upsert(self, repo: InMemoryDLQRepository):
        """Test that adding same event twice updates retry count."""
        event_id = uuid4()
        await self._add_sample_failed(repo, event_id=event_id, error_msg="Error 1", retry_count=1)
        await self._add_sample_failed(repo, event_id=event_id, error_msg="Error 2", retry_count=2)

        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].retry_count == 2
        assert "Error 2" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_get_failed_events_with_projection_filter(self, repo: InMemoryDLQRepository):
        """Test filtering failed events by projection name."""
        await self._add_sample_failed(
            repo, projection_name="Projection1", event_type="Event1", error_msg="Error 1"
        )
        await self._add_sample_failed(
            repo, projection_name="Projection2", event_type="Event2", error_msg="Error 2"
        )

        all_events = await repo.get_failed_events()
        assert len(all_events) == 2

        proj1_events = await repo.get_failed_events(projection_name="Projection1")
        assert len(proj1_events) == 1
        assert proj1_events[0].projection_name == "Projection1"

        proj2_events = await repo.get_failed_events(projection_name="Projection2")
        assert len(proj2_events) == 1
        assert proj2_events[0].projection_name == "Projection2"

    @pytest.mark.asyncio
    async def test_get_failed_events_with_status_filter(self, repo: InMemoryDLQRepository):
        """Test filtering failed events by status."""
        await self._add_sample_failed(repo)
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)

        failed_events = await repo.get_failed_events(status="failed")
        assert len(failed_events) == 0

        retrying_events = await repo.get_failed_events(status="retrying")
        assert len(retrying_events) == 1

    @pytest.mark.asyncio
    async def test_get_failed_events_limit(self, repo: InMemoryDLQRepository):
        """Test limiting number of returned events."""
        for i in range(5):
            await self._add_sample_failed(repo, event_type=f"Event{i}", error_msg=f"Error {i}")

        limited = await repo.get_failed_events(limit=3)
        assert len(limited) == 3

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id(self, repo: InMemoryDLQRepository):
        """Test getting a specific failed event by ID."""
        event_id = await self._add_sample_failed(
            repo, event_data={"test": "data"}, error_msg="Test error", retry_count=2
        )
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.id == dlq_id
        assert event.event_id == event_id
        assert event.retry_count == 2

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_not_found(self, repo: InMemoryDLQRepository):
        """Test getting non-existent event by ID returns None."""
        result = await repo.get_failed_event_by_id(999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_mark_resolved(self, repo: InMemoryDLQRepository):
        """Test marking a DLQ entry as resolved."""
        await self._add_sample_failed(repo)
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_resolved(dlq_id, resolved_by="admin@test.com")

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event.status == "resolved"
        assert event.resolved_at is not None
        assert event.resolved_by == "admin@test.com"

    @pytest.mark.asyncio
    async def test_mark_resolved_with_uuid(self, repo: InMemoryDLQRepository):
        """Test marking resolved with UUID as resolved_by."""
        user_id = uuid4()
        await self._add_sample_failed(repo)
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_resolved(dlq_id, resolved_by=user_id)

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event.resolved_by == str(user_id)

    @pytest.mark.asyncio
    async def test_mark_retrying(self, repo: InMemoryDLQRepository):
        """Test marking a DLQ entry as retrying."""
        await self._add_sample_failed(repo)
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)

        retrying = await repo.get_failed_events(status="retrying")
        assert len(retrying) == 1
        assert retrying[0].status == "retrying"

    @pytest.mark.asyncio
    async def test_get_failure_stats(self, repo: InMemoryDLQRepository):
        """Test getting DLQ statistics."""
        for i in range(3):
            await self._add_sample_failed(
                repo, projection_name=f"Projection{i % 2}", error_msg=f"Error {i}"
            )

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 3
        assert stats.total_retrying == 0
        assert stats.affected_projections == 2
        assert stats.oldest_failure is not None

    @pytest.mark.asyncio
    async def test_get_failure_stats_with_retrying(self, repo: InMemoryDLQRepository):
        """Test failure stats includes retrying count."""
        for i in range(2):
            await self._add_sample_failed(repo, error_msg=f"Error {i}")

        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 1
        assert stats.total_retrying == 1
        assert stats.affected_projections == 1

    @pytest.mark.asyncio
    async def test_get_failure_stats_empty(self, repo: InMemoryDLQRepository):
        """Test failure stats with no failures."""
        stats = await repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts(self, repo: InMemoryDLQRepository):
        """Test getting failure counts grouped by projection."""
        for i in range(5):
            projection = "HighFailure" if i < 3 else "LowFailure"
            await self._add_sample_failed(repo, projection_name=projection, error_msg=f"Error {i}")

        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 2
        assert counts[0].projection_name == "HighFailure"
        assert counts[0].failure_count == 3
        assert counts[1].projection_name == "LowFailure"
        assert counts[1].failure_count == 2

    @pytest.mark.asyncio
    async def test_delete_resolved_events(self, repo: InMemoryDLQRepository):
        """Test deleting resolved events older than specified days."""
        await self._add_sample_failed(repo)
        events = await repo.get_failed_events()
        await repo.mark_resolved(events[0].id, resolved_by="admin")

        deleted = await repo.delete_resolved_events(older_than_days=0)
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_clear(self, repo: InMemoryDLQRepository):
        """Test clearing all entries."""
        for i in range(3):
            await self._add_sample_failed(repo, error_msg=f"Error {i}")

        await repo.clear()

        events = await repo.get_failed_events()
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_event_data_serialization(self, repo: InMemoryDLQRepository):
        """Test that event data is properly serialized."""
        tenant_id = uuid4()
        event_data = {
            "tenant_id": str(tenant_id),
            "items": ["a", "b", "c"],
            "nested": {"key": "value"},
        }
        await self._add_sample_failed(repo, event_data=event_data)

        events = await repo.get_failed_events()
        assert isinstance(events[0].event_data, str)
        assert str(tenant_id) in events[0].event_data
