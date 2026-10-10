"""Query, statistics, and maintenance tests for SQLDLQRepository."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.adapters.sql.dlq import SQLDLQRepository

# Check if aiosqlite is available
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLDLQRepositoryQueries:
    """Tests for SQLDLQRepository queries, statistics, and maintenance."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.schemas import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))
        yield engine
        await engine.dispose()

    @pytest.fixture
    def repo(self, sqlite_engine) -> SQLDLQRepository:
        """Create a SQLDLQRepository for each test."""
        return SQLDLQRepository(sqlite_engine)

    @pytest.mark.asyncio
    async def test_get_failed_events_with_projection_filter(self, repo: SQLDLQRepository):
        """Test filtering failed events by projection name."""
        # Add events for different projections
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection1",
            event_type="Event1",
            event_data={},
            error=Exception("Error 1"),
        )
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection2",
            event_type="Event2",
            event_data={},
            error=Exception("Error 2"),
        )

        # Get all
        all_events = await repo.get_failed_events()
        assert len(all_events) == 2

        # Filter by projection1
        proj1_events = await repo.get_failed_events(projection_name="Projection1")
        assert len(proj1_events) == 1
        assert proj1_events[0].projection_name == "Projection1"

        # Filter by projection2
        proj2_events = await repo.get_failed_events(projection_name="Projection2")
        assert len(proj2_events) == 1
        assert proj2_events[0].projection_name == "Projection2"

    @pytest.mark.asyncio
    async def test_get_failed_events_with_status_filter(self, repo: SQLDLQRepository):
        """Test filtering failed events by status."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        # Get dlq_id
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        # Mark as retrying
        await repo.mark_retrying(dlq_id)

        # Filter by failed status (should be empty)
        failed_events = await repo.get_failed_events(status="failed")
        assert len(failed_events) == 0

        # Filter by retrying status
        retrying_events = await repo.get_failed_events(status="retrying")
        assert len(retrying_events) == 1

    @pytest.mark.asyncio
    async def test_get_failed_events_limit(self, repo: SQLDLQRepository):
        """Test limiting number of returned events."""
        # Add 5 events
        for i in range(5):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type=f"Event{i}",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        # Get with limit
        limited = await repo.get_failed_events(limit=3)
        assert len(limited) == 3

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id(self, repo: SQLDLQRepository):
        """Test getting a specific failed event by ID."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"test": "data"},
            error=Exception("Test error"),
            retry_count=2,
        )

        # Get the DLQ ID
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        # Get by ID
        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.id == dlq_id
        assert event.event_id == event_id
        assert event.retry_count == 2

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_not_found(self, repo: SQLDLQRepository):
        """Test getting non-existent event by ID returns None."""
        result = await repo.get_failed_event_by_id(999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_failure_stats(self, repo: SQLDLQRepository):
        """Test getting DLQ statistics."""
        # Add some failed events for different projections
        for i in range(3):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=f"Projection{i % 2}",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 3
        assert stats.total_retrying == 0
        assert stats.affected_projections == 2
        assert stats.oldest_failure is not None

    @pytest.mark.asyncio
    async def test_get_failure_stats_with_retrying(self, repo: SQLDLQRepository):
        """Test failure stats includes retrying count."""
        # Add two events
        for i in range(2):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        # Mark one as retrying
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 1
        assert stats.total_retrying == 1
        assert stats.affected_projections == 1

    @pytest.mark.asyncio
    async def test_get_failure_stats_empty(self, repo: SQLDLQRepository):
        """Test failure stats with no failures."""
        stats = await repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts(self, repo: SQLDLQRepository):
        """Test getting failure counts grouped by projection."""
        # Add events for different projections
        for i in range(5):
            projection = "HighFailure" if i < 3 else "LowFailure"
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=projection,
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 2

        # Should be ordered by count descending
        assert counts[0].projection_name == "HighFailure"
        assert counts[0].failure_count == 3
        assert counts[1].projection_name == "LowFailure"
        assert counts[1].failure_count == 2

    @pytest.mark.asyncio
    async def test_delete_resolved_events(self, repo: SQLDLQRepository, sqlite_engine):
        """Test deleting resolved events older than specified days."""
        event_id = uuid4()

        # Add and resolve an event
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Manually update resolved_at to be old (35 days ago)
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE dead_letter_queue
                    SET resolved_at = datetime('now', '-35 days')
                    WHERE id = :dlq_id
                    """),
                {"dlq_id": dlq_id},
            )

        # Delete resolved events older than 30 days
        deleted = await repo.delete_resolved_events(older_than_days=30)

        assert deleted == 1

        # Verify it's gone
        result = await repo.get_failed_event_by_id(dlq_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_resolved_events_keeps_recent(self, repo: SQLDLQRepository):
        """Test that recent resolved events are not deleted."""
        event_id = uuid4()

        # Add and resolve an event
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Try to delete (event is recent, should not be deleted)
        deleted = await repo.delete_resolved_events(older_than_days=30)

        assert deleted == 0

        # Verify it still exists
        result = await repo.get_failed_event_by_id(dlq_id)
        assert result is not None

    @pytest.mark.asyncio
    async def test_event_data_serialization(self, repo: SQLDLQRepository):
        """Test that event data is properly serialized."""
        event_id = uuid4()
        tenant_id = uuid4()
        event_data = {
            "tenant_id": str(tenant_id),
            "items": ["a", "b", "c"],
            "nested": {"key": "value"},
        }

        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data=event_data,
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        # event_data is decoded via json_result on read, so it comes back
        # as the original dict rather than a JSON string.
        assert events[0].event_data == event_data

    @pytest.mark.asyncio
    async def test_error_stacktrace_captured(self, repo: SQLDLQRepository):
        """Test that stacktrace is captured when adding failed event."""
        event_id = uuid4()

        try:
            raise RuntimeError("Intentional error for test")
        except RuntimeError as e:
            await repo.add_failed_event(
                event_id=event_id,
                projection_name="StacktraceProjection",
                event_type="TestEvent",
                event_data={"test": True},
                error=e,
                retry_count=0,
            )

        failed_events = await repo.get_failed_events(projection_name="StacktraceProjection")

        assert len(failed_events) == 1
        assert "RuntimeError" in failed_events[0].error_stacktrace
        assert "Intentional error for test" in failed_events[0].error_stacktrace
