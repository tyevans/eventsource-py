"""Tests for SQLDLQRepository statistics, maintenance, and serialization against SQLite."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sql.dlq import SQLDLQRepository

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLDLQRepositoryGetFailureStats:
    """Tests for get_failure_stats method."""

    async def test_get_failure_stats_empty(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test failure stats with no failures."""
        stats = await sqlite_dlq_repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    async def test_get_failure_stats_with_data(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test failure stats with failures."""
        for i in range(3):
            await sqlite_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name=f"Projection{i % 2}",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        stats = await sqlite_dlq_repo.get_failure_stats()
        assert stats.total_failed == 3
        assert stats.affected_projections == 2
        assert stats.oldest_failure is not None

    async def test_get_failure_stats_with_retrying(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test failure stats includes retrying count."""
        for i in range(2):
            await sqlite_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        events = await sqlite_dlq_repo.get_failed_events()
        await sqlite_dlq_repo.mark_retrying(events[0].id)

        stats = await sqlite_dlq_repo.get_failure_stats()
        assert stats.total_failed == 1
        assert stats.total_retrying == 1


class TestSQLDLQRepositoryGetProjectionFailureCounts:
    """Tests for get_projection_failure_counts method."""

    async def test_get_projection_failure_counts(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test getting failure counts by projection."""
        for i in range(5):
            projection = "HighFailure" if i < 3 else "LowFailure"
            await sqlite_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name=projection,
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        counts = await sqlite_dlq_repo.get_projection_failure_counts()
        assert len(counts) == 2

        # Should be ordered by count descending
        assert counts[0].projection_name == "HighFailure"
        assert counts[0].failure_count == 3
        assert counts[1].projection_name == "LowFailure"
        assert counts[1].failure_count == 2


class TestSQLDLQRepositoryDeleteResolvedEvents:
    """Tests for delete_resolved_events method."""

    async def test_delete_resolved_events(
        self,
        sqlite_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test deleting old resolved events."""
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id
        await sqlite_dlq_repo.mark_resolved(dlq_id, resolved_by="admin")

        # Backdate the resolved_at, using the repo's own engine
        async with sqlite_dlq_repo.conn.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE dead_letter_queue
                    SET resolved_at = datetime('now', '-35 days')
                    WHERE id = :dlq_id
                    """),
                {"dlq_id": dlq_id},
            )

        deleted = await sqlite_dlq_repo.delete_resolved_events(older_than_days=30)
        assert deleted == 1

        result = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is None

    async def test_delete_resolved_events_keeps_recent(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        """Test that recent resolved events are not deleted."""
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id
        await sqlite_dlq_repo.mark_resolved(dlq_id, resolved_by="admin")

        deleted = await sqlite_dlq_repo.delete_resolved_events(older_than_days=30)
        assert deleted == 0


class TestSQLDLQRepositoryEventDataSerialization:
    """Tests for event data serialization."""

    async def test_event_data_serialization(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test that event data is properly serialized."""
        tenant_id = uuid4()
        event_data = {
            "tenant_id": str(tenant_id),
            "items": ["a", "b", "c"],
            "nested": {"key": "value"},
        }

        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data=event_data,
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        # event_data is decoded via json_result on read, so it comes back
        # as the original dict rather than a JSON string.
        assert events[0].event_data == event_data

    async def test_error_stacktrace_captured(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test that stacktrace is captured."""
        try:
            raise RuntimeError("Intentional error")
        except RuntimeError as e:
            await sqlite_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=e,
            )

        events = await sqlite_dlq_repo.get_failed_events()
        assert "RuntimeError" in events[0].error_stacktrace
        assert "Intentional error" in events[0].error_stacktrace
