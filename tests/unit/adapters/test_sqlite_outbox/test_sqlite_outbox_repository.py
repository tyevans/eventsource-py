"""Fixture-based tests for SQLiteOutboxRepository."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.ports.outbox import OutboxRepository
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite
from tests.unit.adapters.test_sqlite_outbox._events import TestSampleEvent

if AIOSQLITE_AVAILABLE:
    import aiosqlite


pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLiteOutboxRepositoryProtocol:
    """Verify SQLiteOutboxRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        assert isinstance(sqlite_outbox_repo, OutboxRepository)


class TestSQLiteOutboxRepositoryAddEvent:
    """Tests for add_event method."""

    async def test_add_event(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        event = TestSampleEvent(aggregate_id=uuid4(), tenant_id=uuid4())
        outbox_id = await sqlite_outbox_repo.add_event(event)
        assert outbox_id is not None
        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_type == "TestSampleEvent"

    async def test_add_event_preserves_tenant_id(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        tenant_id = uuid4()
        event = TestSampleEvent(aggregate_id=uuid4(), tenant_id=tenant_id)
        await sqlite_outbox_repo.add_event(event)
        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].tenant_id == tenant_id

    async def test_add_event_without_tenant_id(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        event = TestSampleEvent(aggregate_id=uuid4(), tenant_id=None)
        await sqlite_outbox_repo.add_event(event)
        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].tenant_id is None


class TestSQLiteOutboxRepositoryGetPendingEvents:
    """Tests for get_pending_events method."""

    async def test_get_pending_events_empty(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending == []

    async def test_get_pending_events_order(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        for i in range(3):
            event = TestSampleEvent(aggregate_id=uuid4(), data=f"event_{i}")
            await sqlite_outbox_repo.add_event(event)
        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 3
        assert "event_0" in pending[0].event_data
        assert "event_1" in pending[1].event_data
        assert "event_2" in pending[2].event_data

    async def test_get_pending_events_limit(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        for _ in range(5):
            await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
        pending = await sqlite_outbox_repo.get_pending_events(limit=3)
        assert len(pending) == 3


class TestSQLiteOutboxRepositoryMarkPublished:
    """Tests for mark_published method."""

    async def test_mark_published(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
        pending = await sqlite_outbox_repo.get_pending_events()
        await sqlite_outbox_repo.mark_published(pending[0].id)
        assert len(await sqlite_outbox_repo.get_pending_events()) == 0
        stats = await sqlite_outbox_repo.get_stats()
        assert stats.published_count == 1


class TestSQLiteOutboxRepositoryMarkFailed:
    """Tests for mark_failed method."""

    async def test_mark_failed(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
        pending = await sqlite_outbox_repo.get_pending_events()
        await sqlite_outbox_repo.mark_failed(pending[0].id, "Connection error")
        assert len(await sqlite_outbox_repo.get_pending_events()) == 0
        stats = await sqlite_outbox_repo.get_stats()
        assert stats.failed_count == 1


class TestSQLiteOutboxRepositoryIncrementRetry:
    """Tests for increment_retry method."""

    async def test_increment_retry(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
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
        await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id
        await sqlite_outbox_repo.mark_published(outbox_id)
        await sqlite_connection.execute(
            "UPDATE event_outbox SET published_at = datetime('now', '-10 days') WHERE id = ?",
            (str(outbox_id),),
        )
        await sqlite_connection.commit()
        deleted = await sqlite_outbox_repo.cleanup_published(days=7)
        assert deleted == 1

    async def test_cleanup_keeps_recent(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
        pending = await sqlite_outbox_repo.get_pending_events()
        await sqlite_outbox_repo.mark_published(pending[0].id)
        deleted = await sqlite_outbox_repo.cleanup_published(days=7)
        assert deleted == 0

    async def test_cleanup_published_days_zero_deletes_entry_published_moments_ago(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Regression: `days=0` must delete an entry published moments ago (isoformat comparison)."""
        event = TestSampleEvent(aggregate_id=uuid4())
        outbox_id = await sqlite_outbox_repo.add_event(event)
        await sqlite_outbox_repo.mark_published(outbox_id)
        deleted = await sqlite_outbox_repo.cleanup_published(days=0)
        assert deleted == 1


class TestSQLiteOutboxRepositoryGetStats:
    """Tests for get_stats method."""

    async def test_get_stats_empty(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        stats = await sqlite_outbox_repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.avg_retries == 0.0
        assert stats.oldest_pending is None

    async def test_get_stats_with_data(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        for _ in range(5):
            await sqlite_outbox_repo.add_event(TestSampleEvent(aggregate_id=uuid4()))
        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_ids = [p.id for p in pending]
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
        aggregate_id = uuid4()
        for i in range(3):
            await sqlite_outbox_repo.add_event(
                TestSampleEvent(aggregate_id=aggregate_id, data=f"event_{i}")
            )
        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 3
        for entry in pending:
            assert entry.aggregate_id == aggregate_id
