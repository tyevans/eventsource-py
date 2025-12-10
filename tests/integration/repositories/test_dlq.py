"""
Integration tests for PostgreSQL DLQ (Dead Letter Queue) Repository.

These tests verify actual database operations for DLQ management including:
- Adding failed events
- Retrieving failed events with filters
- Marking events as resolved/retrying
- Getting failure statistics
- Cleanup operations
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource import PostgreSQLDLQRepository

from ..conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestPostgreSQLDLQRepositoryBasics:
    """Basic DLQ repository operations."""

    async def test_add_failed_event(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test adding a failed event to the DLQ."""
        event_id = uuid4()
        projection_name = "TestProjection"
        event_type = "TestEvent"
        event_data = {"key": "value", "number": 42}
        error = ValueError("Test error message")

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type=event_type,
            event_data=event_data,
            error=error,
            retry_count=0,
        )

        # Retrieve and verify
        failed_events = await postgres_dlq_repo.get_failed_events(projection_name=projection_name)

        assert len(failed_events) == 1
        assert failed_events[0].event_id == event_id
        assert failed_events[0].projection_name == projection_name
        assert failed_events[0].event_type == event_type
        assert failed_events[0].error_message == "Test error message"
        assert failed_events[0].status == "failed"

    async def test_add_failed_event_with_stacktrace(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test that stacktrace is captured when adding failed event."""
        event_id = uuid4()

        try:
            raise RuntimeError("Intentional error for test")
        except RuntimeError as e:
            await postgres_dlq_repo.add_failed_event(
                event_id=event_id,
                projection_name="StacktraceProjection",
                event_type="TestEvent",
                event_data={"test": True},
                error=e,
                retry_count=0,
            )

        failed_events = await postgres_dlq_repo.get_failed_events(
            projection_name="StacktraceProjection"
        )

        assert len(failed_events) == 1
        assert failed_events[0].error_stacktrace is not None
        assert "RuntimeError" in failed_events[0].error_stacktrace
        assert "Intentional error for test" in failed_events[0].error_stacktrace

    async def test_add_failed_event_upsert_on_retry(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test that adding same event/projection updates retry count."""
        event_id = uuid4()
        projection_name = "RetryProjection"

        # First failure
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={"attempt": 1},
            error=ValueError("First failure"),
            retry_count=0,
        )

        # Second failure (same event, same projection)
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={"attempt": 2},
            error=ValueError("Second failure"),
            retry_count=1,
        )

        # Should only have one entry
        failed_events = await postgres_dlq_repo.get_failed_events(projection_name=projection_name)

        assert len(failed_events) == 1
        assert failed_events[0].retry_count == 1
        assert failed_events[0].error_message == "Second failure"


class TestPostgreSQLDLQRepositoryRetrieval:
    """Tests for DLQ retrieval operations."""

    async def test_get_failed_events_by_projection(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test filtering failed events by projection name."""
        # Add events for different projections
        for i, proj in enumerate(["ProjectionA", "ProjectionB", "ProjectionA"]):
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name=proj,
                event_type="TestEvent",
                event_data={"index": i},
                error=ValueError(f"Error {i}"),
            )

        # Get for ProjectionA only
        events_a = await postgres_dlq_repo.get_failed_events(projection_name="ProjectionA")
        assert len(events_a) == 2

        # Get for ProjectionB only
        events_b = await postgres_dlq_repo.get_failed_events(projection_name="ProjectionB")
        assert len(events_b) == 1

    async def test_get_failed_events_by_status(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test filtering failed events by status."""
        event_id = uuid4()
        projection_name = "StatusProjection"

        # Add failed event
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        # Get failed events (default status)
        failed = await postgres_dlq_repo.get_failed_events(status="failed")
        assert len(failed) >= 1

        # Mark as retrying
        dlq_id = failed[0].id
        await postgres_dlq_repo.mark_retrying(dlq_id)

        # Get retrying events
        retrying = await postgres_dlq_repo.get_failed_events(status="retrying")
        assert any(e.id == dlq_id for e in retrying)

    async def test_get_failed_events_with_limit(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test limiting number of returned events."""
        # Add 10 events
        for i in range(10):
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="LimitProjection",
                event_type="TestEvent",
                event_data={"index": i},
                error=ValueError(f"Error {i}"),
            )

        # Get with limit
        events = await postgres_dlq_repo.get_failed_events(
            projection_name="LimitProjection",
            limit=5,
        )

        assert len(events) == 5

    async def test_get_failed_event_by_id(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test retrieving a specific failed event by DLQ ID."""
        event_id = uuid4()

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="ByIdProjection",
            event_type="TestEvent",
            event_data={"specific": True},
            error=ValueError("Specific error"),
        )

        # Get all to find the ID
        all_events = await postgres_dlq_repo.get_failed_events(projection_name="ByIdProjection")
        dlq_id = all_events[0].id

        # Get by ID
        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)

        assert result is not None
        assert result.event_id == event_id
        assert result.projection_name == "ByIdProjection"

    async def test_get_failed_event_by_id_nonexistent(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test getting non-existent event returns None."""
        result = await postgres_dlq_repo.get_failed_event_by_id(999999)
        assert result is None


class TestPostgreSQLDLQRepositoryStatusTransitions:
    """Tests for DLQ status transition operations."""

    async def test_mark_resolved(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test marking a DLQ entry as resolved."""
        event_id = uuid4()
        resolver_id = "user123"

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="ResolveProjection",
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        # Get ID
        events = await postgres_dlq_repo.get_failed_events(projection_name="ResolveProjection")
        dlq_id = events[0].id

        # Mark resolved
        await postgres_dlq_repo.mark_resolved(dlq_id, resolver_id)

        # Verify
        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is not None
        assert result.status == "resolved"
        assert result.resolved_by == resolver_id
        assert result.resolved_at is not None

    async def test_mark_retrying(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test marking a DLQ entry as retrying."""
        event_id = uuid4()

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="RetryingProjection",
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        # Get ID
        events = await postgres_dlq_repo.get_failed_events(projection_name="RetryingProjection")
        dlq_id = events[0].id

        # Mark retrying
        await postgres_dlq_repo.mark_retrying(dlq_id)

        # Verify
        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is not None
        assert result.status == "retrying"


class TestPostgreSQLDLQRepositoryStatistics:
    """Tests for DLQ statistics operations."""

    async def test_get_failure_stats_empty(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test getting failure stats when DLQ is empty."""
        stats = await postgres_dlq_repo.get_failure_stats()

        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    async def test_get_failure_stats_with_entries(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test getting failure stats with entries."""
        # Add entries for multiple projections
        for proj in ["Stats1", "Stats2", "Stats1"]:
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name=proj,
                event_type="TestEvent",
                event_data={},
                error=ValueError("Error"),
            )

        # Mark one as retrying
        events = await postgres_dlq_repo.get_failed_events(projection_name="Stats1")
        await postgres_dlq_repo.mark_retrying(events[0].id)

        stats = await postgres_dlq_repo.get_failure_stats()

        assert stats.total_failed == 2  # One was marked retrying
        assert stats.total_retrying == 1
        assert stats.affected_projections == 2
        assert stats.oldest_failure is not None

    async def test_get_projection_failure_counts(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test getting failure counts grouped by projection."""
        # Add entries
        for _ in range(3):
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="HighFailure",
                event_type="TestEvent",
                event_data={},
                error=ValueError("Error"),
            )
        for _ in range(1):
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="LowFailure",
                event_type="TestEvent",
                event_data={},
                error=ValueError("Error"),
            )

        counts = await postgres_dlq_repo.get_projection_failure_counts()

        # Should be sorted by failure count descending
        high = next((c for c in counts if c.projection_name == "HighFailure"), None)
        low = next((c for c in counts if c.projection_name == "LowFailure"), None)

        assert high is not None
        assert high.failure_count == 3
        assert low is not None
        assert low.failure_count == 1

        # High should come before low in results
        high_idx = next(i for i, c in enumerate(counts) if c.projection_name == "HighFailure")
        low_idx = next(i for i, c in enumerate(counts) if c.projection_name == "LowFailure")
        assert high_idx < low_idx


class TestPostgreSQLDLQRepositoryCleanup:
    """Tests for DLQ cleanup operations."""

    async def test_delete_resolved_events(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
        postgres_engine: AsyncEngine,
    ) -> None:
        """Test deleting old resolved events."""
        from sqlalchemy import text

        event_id = uuid4()

        # Add and resolve an event
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="CleanupProjection",
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        events = await postgres_dlq_repo.get_failed_events(projection_name="CleanupProjection")
        dlq_id = events[0].id

        await postgres_dlq_repo.mark_resolved(dlq_id, "user")

        # Manually update resolved_at to be old (35 days ago)
        async with postgres_engine.begin() as conn:
            old_date = datetime.now(UTC) - timedelta(days=35)
            await conn.execute(
                text("""
                    UPDATE dead_letter_queue
                    SET resolved_at = :old_date
                    WHERE id = :dlq_id
                """),
                {"old_date": old_date, "dlq_id": dlq_id},
            )

        # Delete old resolved events (default 30 days)
        deleted_count = await postgres_dlq_repo.delete_resolved_events(older_than_days=30)

        assert deleted_count == 1

        # Verify it's gone
        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is None

    async def test_delete_resolved_events_keeps_recent(
        self,
        postgres_dlq_repo: PostgreSQLDLQRepository,
    ) -> None:
        """Test that recent resolved events are not deleted."""
        event_id = uuid4()

        # Add and resolve an event
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="RecentProjection",
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        events = await postgres_dlq_repo.get_failed_events(projection_name="RecentProjection")
        dlq_id = events[0].id

        await postgres_dlq_repo.mark_resolved(dlq_id, "user")

        # Try to delete (event is recent, should not be deleted)
        deleted_count = await postgres_dlq_repo.delete_resolved_events(older_than_days=30)

        assert deleted_count == 0

        # Verify it still exists
        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is not None
