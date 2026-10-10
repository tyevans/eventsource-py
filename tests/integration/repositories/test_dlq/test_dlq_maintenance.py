"""Integration tests for DLQ statistics and cleanup operations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource import SQLDLQRepository
from tests.integration.conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestSQLDLQRepositoryStatistics:
    """Tests for DLQ statistics operations."""

    async def test_get_failure_stats_empty(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test getting failure stats when DLQ is empty."""
        stats = await postgres_dlq_repo.get_failure_stats()

        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    async def test_get_failure_stats_with_entries(
        self,
        postgres_dlq_repo: SQLDLQRepository,
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
        postgres_dlq_repo: SQLDLQRepository,
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


class TestSQLDLQRepositoryCleanup:
    """Tests for DLQ cleanup operations."""

    async def test_delete_resolved_events(
        self,
        postgres_dlq_repo: SQLDLQRepository,
        postgres_engine: AsyncEngine,
    ) -> None:
        """Test deleting old resolved events."""
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
        postgres_dlq_repo: SQLDLQRepository,
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
