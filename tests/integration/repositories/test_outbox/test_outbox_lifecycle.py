"""Integration tests for PostgreSQL Outbox Repository publishing, retries, cleanup, and stats."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource import PostgreSQLOutboxRepository
from tests.integration.conftest import TestItemCreated, skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


def _make_item(name: str, quantity: int = 1) -> TestItemCreated:
    return TestItemCreated(aggregate_id=uuid4(), aggregate_version=1, name=name, quantity=quantity)


class TestPostgreSQLOutboxRepositoryPublishing:
    async def test_mark_published(self, postgres_outbox_repo: PostgreSQLOutboxRepository) -> None:
        outbox_id = await postgres_outbox_repo.add_event(_make_item("Publish Test"))
        await postgres_outbox_repo.mark_published(outbox_id)
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 0

    async def test_published_event_not_in_pending(
        self, postgres_outbox_repo: PostgreSQLOutboxRepository
    ) -> None:
        event1 = _make_item("Event 1", 1)
        event2 = _make_item("Event 2", 2)
        outbox_id1 = await postgres_outbox_repo.add_event(event1)
        await postgres_outbox_repo.add_event(event2)
        await postgres_outbox_repo.mark_published(outbox_id1)
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_id == event2.event_id


class TestPostgreSQLOutboxRepositoryRetries:
    async def test_increment_retry(self, postgres_outbox_repo: PostgreSQLOutboxRepository) -> None:
        outbox_id = await postgres_outbox_repo.add_event(_make_item("Retry Test"))
        await postgres_outbox_repo.increment_retry(outbox_id, "Connection failed")
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1 and pending[0].retry_count == 1

    async def test_increment_retry_multiple_times(
        self, postgres_outbox_repo: PostgreSQLOutboxRepository
    ) -> None:
        outbox_id = await postgres_outbox_repo.add_event(_make_item("Multi Retry Test"))
        for i in range(3):
            await postgres_outbox_repo.increment_retry(outbox_id, f"Error {i}")
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1 and pending[0].retry_count == 3

    async def test_mark_failed(self, postgres_outbox_repo: PostgreSQLOutboxRepository) -> None:
        outbox_id = await postgres_outbox_repo.add_event(_make_item("Fail Test"))
        await postgres_outbox_repo.mark_failed(outbox_id, "Permanent failure")
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 0


class TestPostgreSQLOutboxRepositoryCleanup:
    async def test_cleanup_published_events(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
        postgres_engine: AsyncEngine,
    ) -> None:
        from sqlalchemy import text

        outbox_id = await postgres_outbox_repo.add_event(_make_item("Cleanup Test"))
        await postgres_outbox_repo.mark_published(outbox_id)
        async with postgres_engine.begin() as conn:
            old_date = datetime.now(UTC) - timedelta(days=10)
            await conn.execute(
                text("UPDATE event_outbox SET published_at = :old_date WHERE id = :outbox_id"),
                {"old_date": old_date, "outbox_id": outbox_id},
            )
        deleted = await postgres_outbox_repo.cleanup_published(days=7)
        assert deleted == 1

    async def test_cleanup_keeps_recent_published(
        self, postgres_outbox_repo: PostgreSQLOutboxRepository
    ) -> None:
        outbox_id = await postgres_outbox_repo.add_event(_make_item("Recent Test"))
        await postgres_outbox_repo.mark_published(outbox_id)
        deleted = await postgres_outbox_repo.cleanup_published(days=7)
        assert deleted == 0

    async def test_cleanup_keeps_pending(
        self, postgres_outbox_repo: PostgreSQLOutboxRepository
    ) -> None:
        await postgres_outbox_repo.add_event(_make_item("Pending Test"))
        deleted = await postgres_outbox_repo.cleanup_published(days=0)
        assert deleted == 0
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1


class TestPostgreSQLOutboxRepositoryStatistics:
    async def test_get_stats_empty(self, postgres_outbox_repo: PostgreSQLOutboxRepository) -> None:
        stats = await postgres_outbox_repo.get_stats()
        assert (stats.pending_count, stats.published_count, stats.failed_count) == (0, 0, 0)
        assert stats.oldest_pending is None and stats.avg_retries == 0.0

    async def test_get_stats_with_entries(
        self, postgres_outbox_repo: PostgreSQLOutboxRepository
    ) -> None:
        for i in range(3):
            outbox_id = await postgres_outbox_repo.add_event(_make_item(f"Pending {i}", i))
            if i == 0:
                await postgres_outbox_repo.increment_retry(outbox_id, "Error")
                await postgres_outbox_repo.increment_retry(outbox_id, "Error")

        published_id = await postgres_outbox_repo.add_event(_make_item("Published", 99))
        await postgres_outbox_repo.mark_published(published_id)

        failed_id = await postgres_outbox_repo.add_event(_make_item("Failed", 0))
        await postgres_outbox_repo.mark_failed(failed_id, "Permanent failure")

        stats = await postgres_outbox_repo.get_stats()
        assert (stats.pending_count, stats.published_count, stats.failed_count) == (3, 1, 1)
        assert stats.oldest_pending is not None and stats.avg_retries > 0
