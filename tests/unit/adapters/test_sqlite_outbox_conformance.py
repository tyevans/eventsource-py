"""Conformance tests for SQLiteOutboxRepository against the port suites."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from eventsource.adapters.sql.schemas import get_schema
from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.testing.conformance_ports import OutboxRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import make_event
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    import aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLiteOutboxRepositoryConformance(OutboxRepositoryConformance):
    """Run full OutboxRepositoryConformance against SQLiteOutboxRepository."""

    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteOutboxRepository]:
        conn = await aiosqlite.connect(":memory:")
        schema = get_schema("all", backend="sqlite")
        await conn.executescript(schema)
        await conn.commit()
        try:
            yield SQLiteOutboxRepository(conn)
        finally:
            await conn.close()

    async def test_sqlite_cleanup_published_cutoff(self, store: SQLiteOutboxRepository) -> None:
        """Verify SQLite-specific cleanup cutoff behavior."""
        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)

        deleted = await store.cleanup_published(days=0)
        assert deleted == 1
        stats = await store.get_stats()
        assert stats.published_count == 0

    async def test_sqlite_cleanup_published_keeps_recent(
        self, store: SQLiteOutboxRepository
    ) -> None:
        """Verify entries within retention window survive cleanup."""
        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)

        deleted = await store.cleanup_published(days=7)
        assert deleted == 0
        stats = await store.get_stats()
        assert stats.published_count == 1
