"""
SQLite Repository Integration Tests.

Comprehensive integration tests for all SQLite repository implementations:
- SQLCheckpointRepository
- SQLDLQRepository

SQLiteOutboxRepository tests live in tests/unit/adapters/test_sqlite_outbox.py.

These tests verify that all repository methods work correctly with
the SQLite backend and the repository fixtures from conftest.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.domain.event import DomainEvent
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
    from eventsource.adapters.sql.dlq import SQLDLQRepository
    from eventsource.ports.checkpoints import CheckpointRepository
    from eventsource.ports.dlq import DLQRepository

if TYPE_CHECKING:
    pass


# ============================================================================
# Test Markers
# ============================================================================

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


# ============================================================================
# Test Event
# ============================================================================


class TestSampleEvent(DomainEvent):
    """Sample event for repository tests."""

    event_type: str = "TestSampleEvent"
    aggregate_type: str = "TestSampleAggregate"
    data: str = "test_data"


# ============================================================================
# SQLCheckpointRepository Tests
# ============================================================================


class TestSQLCheckpointRepositoryProtocol:
    """Verify SQLCheckpointRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_checkpoint_repo: SQLCheckpointRepository) -> None:
        """Test that SQLCheckpointRepository implements CheckpointRepository."""
        assert isinstance(sqlite_checkpoint_repo, CheckpointRepository)


class TestSQLCheckpointRepositoryGetCheckpoint:
    """Tests for get_checkpoint method."""

    async def test_get_checkpoint_returns_none_for_new_projection(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test that get_checkpoint returns None for a new projection."""
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint("NewProjection")
        assert checkpoint is None

    async def test_get_checkpoint_returns_saved_checkpoint(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test that get_checkpoint returns a previously saved checkpoint."""
        event_id = uuid4()
        projection_name = "TestProjection"

        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type="TestEvent",
        )

        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)

        assert checkpoint is not None
        assert checkpoint == event_id


class TestSQLCheckpointRepositoryUpdateCheckpoint:
    """Tests for update_checkpoint method."""

    async def test_update_checkpoint_creates_new(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test creating a new checkpoint."""
        event_id = uuid4()
        projection_name = "NewProjection"

        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type="FirstEvent",
        )

        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint is not None
        assert checkpoint == event_id

    async def test_update_checkpoint_updates_existing(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test updating an existing checkpoint."""
        projection_name = "UpdatedProjection"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        # Save first checkpoint
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id_1,
            event_type="FirstEvent",
        )

        # Update checkpoint
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id_2,
            event_type="SecondEvent",
        )

        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint is not None
        assert checkpoint == event_id_2

    async def test_update_checkpoint_preserves_processed_count(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test that events_processed count increases correctly."""
        projection_name = "CountingProjection"

        for i in range(5):
            await sqlite_checkpoint_repo.update_checkpoint(
                projection_name=projection_name,
                event_id=uuid4(),
                event_type=f"Event{i}",
            )

        # Get all checkpoints to verify count
        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        checkpoint = next((cp for cp in checkpoints if cp.projection_name == projection_name), None)
        assert checkpoint is not None
        assert checkpoint.events_processed == 5


class TestSQLCheckpointRepositoryGetAllCheckpoints:
    """Tests for get_all_checkpoints method."""

    async def test_get_all_checkpoints_empty(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test getting checkpoints when none exist."""
        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        assert checkpoints == []

    async def test_get_all_checkpoints_returns_all(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test getting all checkpoints."""
        projections = ["Projection1", "Projection2", "Projection3"]

        for projection in projections:
            await sqlite_checkpoint_repo.update_checkpoint(
                projection_name=projection,
                event_id=uuid4(),
                event_type="TestEvent",
            )

        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        assert len(checkpoints) == 3

        returned_names = {cp.projection_name for cp in checkpoints}
        assert returned_names == set(projections)


class TestSQLCheckpointRepositoryResetCheckpoint:
    """Tests for reset_checkpoint method."""

    async def test_reset_checkpoint_deletes_checkpoint(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test that reset_checkpoint deletes the checkpoint."""
        projection_name = "ToBeReset"

        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=uuid4(),
            event_type="TestEvent",
        )

        # Verify it exists
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint is not None

        # Reset (delete) it
        await sqlite_checkpoint_repo.reset_checkpoint(projection_name)

        # Verify it's gone
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint is None

    async def test_reset_nonexistent_checkpoint(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test resetting a non-existent checkpoint (no error)."""
        await sqlite_checkpoint_repo.reset_checkpoint("NonExistent")
        # Should not raise


class TestSQLCheckpointRepositoryGetLagMetrics:
    """Tests for get_lag_metrics method."""

    async def test_get_lag_metrics_returns_none_for_new_projection(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test that get_lag_metrics returns None for unknown projection."""
        metrics = await sqlite_checkpoint_repo.get_lag_metrics("NonExistent")
        assert metrics is None

    async def test_get_lag_metrics_returns_metrics_for_existing(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        """Test that get_lag_metrics returns metrics for existing checkpoint."""
        projection_name = "MetricsProjection"
        event_id = uuid4()

        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type="TestEvent",
        )

        metrics = await sqlite_checkpoint_repo.get_lag_metrics(projection_name)
        assert metrics is not None
        assert metrics.projection_name == projection_name
        assert metrics.last_event_id == str(event_id)
        assert metrics.events_processed == 1


# ============================================================================
# SQLDLQRepository Tests
# ============================================================================


class TestSQLDLQRepositoryProtocol:
    """Verify SQLDLQRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test that SQLDLQRepository implements DLQRepository."""
        assert isinstance(sqlite_dlq_repo, DLQRepository)


class TestSQLDLQRepositoryAddFailedEvent:
    """Tests for add_failed_event method."""

    async def test_add_failed_event(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test adding a failed event to DLQ."""
        event_id = uuid4()

        await sqlite_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"key": "value"},
            error=Exception("Test error"),
            retry_count=1,
        )

        failed = await sqlite_dlq_repo.get_failed_events()
        assert len(failed) == 1
        assert failed[0].event_id == event_id
        assert failed[0].projection_name == "TestProjection"
        assert failed[0].retry_count == 1
        assert "Test error" in failed[0].error_message

    async def test_add_failed_event_upsert(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test that adding same event updates retry count (upsert)."""
        event_id = uuid4()

        await sqlite_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 1"),
            retry_count=1,
        )

        await sqlite_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 2"),
            retry_count=2,
        )

        failed = await sqlite_dlq_repo.get_failed_events()
        assert len(failed) == 1
        assert failed[0].retry_count == 2
        assert "Error 2" in failed[0].error_message

    async def test_add_failed_event_different_projections(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        """Test same event can fail for different projections."""
        event_id = uuid4()

        await sqlite_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="Projection1",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 1"),
        )

        await sqlite_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="Projection2",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 2"),
        )

        failed = await sqlite_dlq_repo.get_failed_events()
        assert len(failed) == 2


class TestSQLDLQRepositoryGetFailedEvents:
    """Tests for get_failed_events method."""

    async def test_get_failed_events_empty(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test getting failed events when none exist."""
        failed = await sqlite_dlq_repo.get_failed_events()
        assert failed == []

    async def test_get_failed_events_with_projection_filter(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        """Test filtering by projection name."""
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection1",
            event_type="Event1",
            event_data={},
            error=Exception("Error 1"),
        )
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection2",
            event_type="Event2",
            event_data={},
            error=Exception("Error 2"),
        )

        filtered = await sqlite_dlq_repo.get_failed_events(projection_name="Projection1")
        assert len(filtered) == 1
        assert filtered[0].projection_name == "Projection1"

    async def test_get_failed_events_with_status_filter(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        """Test filtering by status."""
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id
        await sqlite_dlq_repo.mark_retrying(dlq_id)

        failed = await sqlite_dlq_repo.get_failed_events(status="failed")
        assert len(failed) == 0

        retrying = await sqlite_dlq_repo.get_failed_events(status="retrying")
        assert len(retrying) == 1

    async def test_get_failed_events_limit(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test limiting number of returned events."""
        for i in range(5):
            await sqlite_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type=f"Event{i}",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        limited = await sqlite_dlq_repo.get_failed_events(limit=3)
        assert len(limited) == 3


class TestSQLDLQRepositoryGetFailedEventById:
    """Tests for get_failed_event_by_id method."""

    async def test_get_failed_event_by_id(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test getting a specific failed event by ID."""
        event_id = uuid4()
        await sqlite_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"test": "data"},
            error=Exception("Test error"),
            retry_count=2,
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id

        event = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.id == dlq_id
        assert event.event_id == event_id
        assert event.retry_count == 2

    async def test_get_failed_event_by_id_not_found(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        """Test getting non-existent event returns None."""
        result = await sqlite_dlq_repo.get_failed_event_by_id(999999)
        assert result is None


class TestSQLDLQRepositoryMarkResolved:
    """Tests for mark_resolved method."""

    async def test_mark_resolved(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test marking a DLQ entry as resolved."""
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id

        await sqlite_dlq_repo.mark_resolved(dlq_id, resolved_by="admin@test.com")

        event = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert event.status == "resolved"
        assert event.resolved_at is not None
        assert event.resolved_by == "admin@test.com"

    async def test_mark_resolved_with_uuid(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test marking resolved with UUID as resolved_by."""
        user_id = uuid4()
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id

        await sqlite_dlq_repo.mark_resolved(dlq_id, resolved_by=user_id)

        event = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert event.resolved_by == str(user_id)


class TestSQLDLQRepositoryMarkRetrying:
    """Tests for mark_retrying method."""

    async def test_mark_retrying(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        """Test marking a DLQ entry as retrying."""
        await sqlite_dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id

        await sqlite_dlq_repo.mark_retrying(dlq_id)

        retrying = await sqlite_dlq_repo.get_failed_events(status="retrying")
        assert len(retrying) == 1
        assert retrying[0].status == "retrying"


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
