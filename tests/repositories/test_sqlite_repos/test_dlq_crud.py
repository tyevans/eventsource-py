"""Tests for SQLDLQRepository CRUD operations against SQLite."""

from __future__ import annotations

from uuid import uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sql.dlq import SQLDLQRepository
    from eventsource.ports.dlq import DLQRepository

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


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
