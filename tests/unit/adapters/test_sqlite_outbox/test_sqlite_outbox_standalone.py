"""Standalone in-memory database tests for SQLiteOutboxRepository."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.ports.outbox import OutboxRepository
from tests.conftest import AIOSQLITE_AVAILABLE
from tests.unit.adapters.test_sqlite_outbox._events import SampleEvent

if AIOSQLITE_AVAILABLE:
    import aiosqlite

_OUTBOX_SCHEMA = """
    CREATE TABLE IF NOT EXISTS event_outbox (
        id TEXT PRIMARY KEY,
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        tenant_id TEXT,
        event_data TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        published_at TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        CHECK (status IN ('pending', 'published', 'failed'))
    )
"""


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteOutboxRepositoryStandalone:
    """Tests for SQLiteOutboxRepository using in-memory SQLite database."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(":memory:")
        await conn.execute(_OUTBOX_SCHEMA)
        await conn.commit()
        yield conn
        await conn.close()

    @pytest.fixture
    def repo(self, db_connection: aiosqlite.Connection) -> SQLiteOutboxRepository:
        return SQLiteOutboxRepository(db_connection)

    @pytest.fixture
    def sample_event(self) -> SampleEvent:
        return SampleEvent(aggregate_id=uuid4(), tenant_id=uuid4(), test_field="test_data")

    @pytest.mark.asyncio
    async def test_add_event(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        outbox_id = await repo.add_event(sample_event)
        assert outbox_id is not None
        assert isinstance(outbox_id, UUID)
        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_type == "SampleEvent"
        assert pending[0].aggregate_type == "SampleAggregate"

    @pytest.mark.asyncio
    async def test_add_event_serializes_data(
        self, repo: SQLiteOutboxRepository, sample_event: SampleEvent
    ):
        await repo.add_event(sample_event)
        pending = await repo.get_pending_events()
        event_data = pending[0].event_data
        assert isinstance(event_data, str)
        assert "test_data" in event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_order(self, repo: SQLiteOutboxRepository):
        events = []
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            events.append(event)
            await repo.add_event(event)
        pending = await repo.get_pending_events()
        assert len(pending) == 3
        assert "event_0" in pending[0].event_data
        assert "event_1" in pending[1].event_data
        assert "event_2" in pending[2].event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_limit(self, repo: SQLiteOutboxRepository):
        for i in range(5):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))
        pending = await repo.get_pending_events(limit=3)
        assert len(pending) == 3

    @pytest.mark.asyncio
    async def test_get_pending_events_empty(self, repo: SQLiteOutboxRepository):
        pending = await repo.get_pending_events()
        assert pending == []

    @pytest.mark.asyncio
    async def test_mark_published(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        await repo.add_event(sample_event)
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id
        await repo.mark_published(outbox_id)
        pending = await repo.get_pending_events()
        assert len(pending) == 0
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 1

    @pytest.mark.asyncio
    async def test_mark_failed(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        await repo.add_event(sample_event)
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id
        await repo.mark_failed(outbox_id, "Connection refused")
        pending = await repo.get_pending_events()
        assert len(pending) == 0
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.failed_count == 1

    @pytest.mark.asyncio
    async def test_increment_retry(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        await repo.add_event(sample_event)
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id
        await repo.increment_retry(outbox_id, "Temporary error")
        await repo.increment_retry(outbox_id, "Another error")
        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_published(
        self, repo: SQLiteOutboxRepository, db_connection: aiosqlite.Connection
    ):
        await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field="test"))
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id
        await repo.mark_published(outbox_id)
        await db_connection.execute(
            "UPDATE event_outbox SET published_at = datetime('now', '-10 days') WHERE id = ?",
            (str(outbox_id),),
        )
        await db_connection.commit()
        deleted = await repo.cleanup_published(days=7)
        assert deleted == 1

    @pytest.mark.asyncio
    async def test_cleanup_keeps_recent_published(self, repo: SQLiteOutboxRepository):
        await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field="test"))
        pending = await repo.get_pending_events()
        await repo.mark_published(pending[0].id)
        deleted = await repo.cleanup_published(days=7)
        assert deleted == 0

    @pytest.mark.asyncio
    async def test_get_stats(self, repo: SQLiteOutboxRepository):
        for i in range(5):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))
        pending = await repo.get_pending_events()
        outbox_ids = [p.id for p in pending]
        await repo.mark_published(outbox_ids[0])
        await repo.mark_published(outbox_ids[1])
        await repo.mark_failed(outbox_ids[4], "Error")
        stats = await repo.get_stats()
        assert stats.pending_count == 2
        assert stats.published_count == 2
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, repo: SQLiteOutboxRepository):
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.oldest_pending is None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_avg_retries(self, repo: SQLiteOutboxRepository):
        for i in range(3):
            await repo.add_event(SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}"))

        pending = await repo.get_pending_events()
        for i, p in enumerate(pending):
            for _ in range(i * 2):
                await repo.increment_retry(p.id, "Error")

        stats = await repo.get_stats()
        assert stats.avg_retries == 2.0

    @pytest.mark.asyncio
    async def test_event_with_tenant_id(self, repo: SQLiteOutboxRepository):
        tenant_id = uuid4()
        await repo.add_event(
            SampleEvent(aggregate_id=uuid4(), tenant_id=tenant_id, test_field="test")
        )
        pending = await repo.get_pending_events()
        assert pending[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_event_without_tenant_id(self, repo: SQLiteOutboxRepository):
        await repo.add_event(SampleEvent(aggregate_id=uuid4(), tenant_id=None, test_field="test"))
        pending = await repo.get_pending_events()
        assert pending[0].tenant_id is None

    @pytest.mark.asyncio
    async def test_multiple_events_same_aggregate(self, repo: SQLiteOutboxRepository):
        aggregate_id = uuid4()
        for i in range(3):
            await repo.add_event(SampleEvent(aggregate_id=aggregate_id, test_field=f"event_{i}"))

        pending = await repo.get_pending_events()
        assert len(pending) == 3
        for entry in pending:
            assert entry.aggregate_id == aggregate_id

    @pytest.mark.asyncio
    async def test_pending_event_fields(self, repo: SQLiteOutboxRepository):
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = SampleEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, test_field="test")
        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 1

        entry = pending[0]
        assert entry.id is not None
        assert entry.event_id == event.event_id
        assert entry.event_type == "SampleEvent"
        assert entry.aggregate_id == aggregate_id
        assert entry.aggregate_type == "SampleAggregate"
        assert entry.tenant_id == tenant_id
        assert entry.event_data is not None
        assert entry.created_at is not None
        assert entry.retry_count == 0


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteOutboxRepositoryStandaloneProtocol:
    """Tests to verify SQLiteOutboxRepository implements the protocol."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(":memory:")
        await conn.execute(_OUTBOX_SCHEMA)
        await conn.commit()
        yield conn
        await conn.close()

    def test_implements_protocol(self, db_connection: aiosqlite.Connection):
        repo = SQLiteOutboxRepository(db_connection)
        assert isinstance(repo, OutboxRepository)
