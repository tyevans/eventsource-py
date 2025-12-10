"""
SQLite Repository Integration Tests.

Comprehensive integration tests for all SQLite repository implementations:
- SQLiteCheckpointRepository
- SQLiteOutboxRepository
- SQLiteDLQRepository

These tests verify that all repository methods work correctly with
the SQLite backend and the repository fixtures from conftest.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    import aiosqlite

    from eventsource.repositories.checkpoint import (
        CheckpointRepository,
        SQLiteCheckpointRepository,
    )
    from eventsource.repositories.dlq import (
        DLQRepository,
        SQLiteDLQRepository,
    )
    from eventsource.repositories.outbox import (
        OutboxRepository,
        SQLiteOutboxRepository,
    )

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
# SQLiteCheckpointRepository Tests
# ============================================================================


class TestSQLiteCheckpointRepositoryProtocol:
    """Verify SQLiteCheckpointRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_checkpoint_repo: SQLiteCheckpointRepository) -> None:
        """Test that SQLiteCheckpointRepository implements CheckpointRepository."""
        assert isinstance(sqlite_checkpoint_repo, CheckpointRepository)


class TestSQLiteCheckpointRepositoryGetCheckpoint:
    """Tests for get_checkpoint method."""

    async def test_get_checkpoint_returns_none_for_new_projection(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
    ) -> None:
        """Test that get_checkpoint returns None for a new projection."""
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint("NewProjection")
        assert checkpoint is None

    async def test_get_checkpoint_returns_saved_checkpoint(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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


class TestSQLiteCheckpointRepositoryUpdateCheckpoint:
    """Tests for update_checkpoint method."""

    async def test_update_checkpoint_creates_new(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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


class TestSQLiteCheckpointRepositoryGetAllCheckpoints:
    """Tests for get_all_checkpoints method."""

    async def test_get_all_checkpoints_empty(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
    ) -> None:
        """Test getting checkpoints when none exist."""
        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        assert checkpoints == []

    async def test_get_all_checkpoints_returns_all(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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


class TestSQLiteCheckpointRepositoryResetCheckpoint:
    """Tests for reset_checkpoint method."""

    async def test_reset_checkpoint_deletes_checkpoint(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
    ) -> None:
        """Test resetting a non-existent checkpoint (no error)."""
        await sqlite_checkpoint_repo.reset_checkpoint("NonExistent")
        # Should not raise


class TestSQLiteCheckpointRepositoryGetLagMetrics:
    """Tests for get_lag_metrics method."""

    async def test_get_lag_metrics_returns_none_for_new_projection(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
    ) -> None:
        """Test that get_lag_metrics returns None for unknown projection."""
        metrics = await sqlite_checkpoint_repo.get_lag_metrics("NonExistent")
        assert metrics is None

    async def test_get_lag_metrics_returns_metrics_for_existing(
        self, sqlite_checkpoint_repo: SQLiteCheckpointRepository
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
# SQLiteOutboxRepository Tests
# ============================================================================


class TestSQLiteOutboxRepositoryProtocol:
    """Verify SQLiteOutboxRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test that SQLiteOutboxRepository implements OutboxRepository."""
        assert isinstance(sqlite_outbox_repo, OutboxRepository)


class TestSQLiteOutboxRepositoryAddEvent:
    """Tests for add_event method."""

    async def test_add_event(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test adding an event to the outbox."""
        event = TestSampleEvent(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
        )

        outbox_id = await sqlite_outbox_repo.add_event(event)

        assert outbox_id is not None

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_type == "TestSampleEvent"

    async def test_add_event_preserves_tenant_id(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test that tenant_id is preserved in outbox."""
        tenant_id = uuid4()
        event = TestSampleEvent(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
        )

        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].tenant_id == tenant_id

    async def test_add_event_without_tenant_id(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test adding an event without tenant_id."""
        event = TestSampleEvent(
            aggregate_id=uuid4(),
            tenant_id=None,
        )

        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].tenant_id is None


class TestSQLiteOutboxRepositoryGetPendingEvents:
    """Tests for get_pending_events method."""

    async def test_get_pending_events_empty(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test getting pending events when none exist."""
        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending == []

    async def test_get_pending_events_order(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test that pending events are returned in FIFO order."""
        events = []
        for i in range(3):
            event = TestSampleEvent(
                aggregate_id=uuid4(),
                data=f"event_{i}",
            )
            events.append(event)
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 3

        # Verify FIFO order
        assert "event_0" in pending[0].event_data
        assert "event_1" in pending[1].event_data
        assert "event_2" in pending[2].event_data

    async def test_get_pending_events_limit(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test limiting number of pending events returned."""
        for _ in range(5):
            event = TestSampleEvent(aggregate_id=uuid4())
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events(limit=3)
        assert len(pending) == 3


class TestSQLiteOutboxRepositoryMarkPublished:
    """Tests for mark_published method."""

    async def test_mark_published(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test marking an event as published."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id

        await sqlite_outbox_repo.mark_published(outbox_id)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 0

        stats = await sqlite_outbox_repo.get_stats()
        assert stats.published_count == 1


class TestSQLiteOutboxRepositoryMarkFailed:
    """Tests for mark_failed method."""

    async def test_mark_failed(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test marking an event as failed."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id

        await sqlite_outbox_repo.mark_failed(outbox_id, "Connection error")

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 0

        stats = await sqlite_outbox_repo.get_stats()
        assert stats.failed_count == 1


class TestSQLiteOutboxRepositoryIncrementRetry:
    """Tests for increment_retry method."""

    async def test_increment_retry(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test incrementing retry count."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id

        await sqlite_outbox_repo.increment_retry(outbox_id, "First retry error")
        await sqlite_outbox_repo.increment_retry(outbox_id, "Second retry error")

        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].retry_count == 2


class TestSQLiteOutboxRepositoryCleanupPublished:
    """Tests for cleanup_published method."""

    async def test_cleanup_published(
        self,
        sqlite_outbox_repo: SQLiteOutboxRepository,
        sqlite_connection: aiosqlite.Connection,
    ) -> None:
        """Test cleaning up old published events."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id
        await sqlite_outbox_repo.mark_published(outbox_id)

        # Manually backdate the published_at
        await sqlite_connection.execute(
            """
            UPDATE event_outbox
            SET published_at = datetime('now', '-10 days')
            WHERE id = ?
            """,
            (str(outbox_id),),
        )
        await sqlite_connection.commit()

        deleted = await sqlite_outbox_repo.cleanup_published(days=7)
        assert deleted == 1

    async def test_cleanup_keeps_recent(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test that recent published events are not cleaned up."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id
        await sqlite_outbox_repo.mark_published(outbox_id)

        deleted = await sqlite_outbox_repo.cleanup_published(days=7)
        assert deleted == 0


class TestSQLiteOutboxRepositoryGetStats:
    """Tests for get_stats method."""

    async def test_get_stats_empty(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test stats when outbox is empty."""
        stats = await sqlite_outbox_repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.avg_retries == 0.0
        assert stats.oldest_pending is None

    async def test_get_stats_with_data(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test stats with various event states."""
        # Add multiple events
        for _ in range(5):
            event = TestSampleEvent(aggregate_id=uuid4())
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_ids = [p.id for p in pending]

        # Publish 2, fail 1, leave 2 pending
        await sqlite_outbox_repo.mark_published(outbox_ids[0])
        await sqlite_outbox_repo.mark_published(outbox_ids[1])
        await sqlite_outbox_repo.mark_failed(outbox_ids[4], "Error")

        stats = await sqlite_outbox_repo.get_stats()
        assert stats.pending_count == 2
        assert stats.published_count == 2
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None


class TestSQLiteOutboxRepositoryMultipleEvents:
    """Tests for handling multiple events."""

    async def test_multiple_events_same_aggregate(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test adding multiple events for the same aggregate."""
        aggregate_id = uuid4()

        for i in range(3):
            event = TestSampleEvent(
                aggregate_id=aggregate_id,
                data=f"event_{i}",
            )
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 3

        for entry in pending:
            assert entry.aggregate_id == aggregate_id


# ============================================================================
# SQLiteDLQRepository Tests
# ============================================================================


class TestSQLiteDLQRepositoryProtocol:
    """Verify SQLiteDLQRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
        """Test that SQLiteDLQRepository implements DLQRepository."""
        assert isinstance(sqlite_dlq_repo, DLQRepository)


class TestSQLiteDLQRepositoryAddFailedEvent:
    """Tests for add_failed_event method."""

    async def test_add_failed_event(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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

    async def test_add_failed_event_upsert(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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
        self, sqlite_dlq_repo: SQLiteDLQRepository
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


class TestSQLiteDLQRepositoryGetFailedEvents:
    """Tests for get_failed_events method."""

    async def test_get_failed_events_empty(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
        """Test getting failed events when none exist."""
        failed = await sqlite_dlq_repo.get_failed_events()
        assert failed == []

    async def test_get_failed_events_with_projection_filter(
        self, sqlite_dlq_repo: SQLiteDLQRepository
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
        self, sqlite_dlq_repo: SQLiteDLQRepository
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

    async def test_get_failed_events_limit(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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


class TestSQLiteDLQRepositoryGetFailedEventById:
    """Tests for get_failed_event_by_id method."""

    async def test_get_failed_event_by_id(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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
        self, sqlite_dlq_repo: SQLiteDLQRepository
    ) -> None:
        """Test getting non-existent event returns None."""
        result = await sqlite_dlq_repo.get_failed_event_by_id(999999)
        assert result is None


class TestSQLiteDLQRepositoryMarkResolved:
    """Tests for mark_resolved method."""

    async def test_mark_resolved(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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

    async def test_mark_resolved_with_uuid(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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


class TestSQLiteDLQRepositoryMarkRetrying:
    """Tests for mark_retrying method."""

    async def test_mark_retrying(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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


class TestSQLiteDLQRepositoryGetFailureStats:
    """Tests for get_failure_stats method."""

    async def test_get_failure_stats_empty(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
        """Test failure stats with no failures."""
        stats = await sqlite_dlq_repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    async def test_get_failure_stats_with_data(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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

    async def test_get_failure_stats_with_retrying(
        self, sqlite_dlq_repo: SQLiteDLQRepository
    ) -> None:
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


class TestSQLiteDLQRepositoryGetProjectionFailureCounts:
    """Tests for get_projection_failure_counts method."""

    async def test_get_projection_failure_counts(
        self, sqlite_dlq_repo: SQLiteDLQRepository
    ) -> None:
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


class TestSQLiteDLQRepositoryDeleteResolvedEvents:
    """Tests for delete_resolved_events method."""

    async def test_delete_resolved_events(
        self,
        sqlite_dlq_repo: SQLiteDLQRepository,
        sqlite_connection: aiosqlite.Connection,
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

        # Backdate the resolved_at
        await sqlite_connection.execute(
            """
            UPDATE dead_letter_queue
            SET resolved_at = datetime('now', '-35 days')
            WHERE id = ?
            """,
            (dlq_id,),
        )
        await sqlite_connection.commit()

        deleted = await sqlite_dlq_repo.delete_resolved_events(older_than_days=30)
        assert deleted == 1

        result = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is None

    async def test_delete_resolved_events_keeps_recent(
        self, sqlite_dlq_repo: SQLiteDLQRepository
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


class TestSQLiteDLQRepositoryEventDataSerialization:
    """Tests for event data serialization."""

    async def test_event_data_serialization(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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
        assert isinstance(events[0].event_data, str)
        assert str(tenant_id) in events[0].event_data

    async def test_error_stacktrace_captured(self, sqlite_dlq_repo: SQLiteDLQRepository) -> None:
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
