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


class TestPostgreSQLOutboxRepositoryPublishing:
    """Tests for marking events as published."""

    async def test_mark_published(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Publish Test",
            quantity=1,
        )
        outbox_id = await postgres_outbox_repo.add_event(event)
        await postgres_outbox_repo.mark_published(outbox_id)
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 0

    async def test_published_event_not_in_pending(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event1 = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Event 1",
            quantity=1,
        )
        event2 = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Event 2",
            quantity=2,
        )
        outbox_id1 = await postgres_outbox_repo.add_event(event1)
        await postgres_outbox_repo.add_event(event2)
        await postgres_outbox_repo.mark_published(outbox_id1)
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_id == event2.event_id


class TestPostgreSQLOutboxRepositoryRetries:
    """Tests for retry handling."""

    async def test_increment_retry(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Retry Test",
            quantity=1,
        )
        outbox_id = await postgres_outbox_repo.add_event(event)
        await postgres_outbox_repo.increment_retry(outbox_id, "Connection failed")
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == 1

    async def test_increment_retry_multiple_times(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Multi Retry Test",
            quantity=1,
        )
        outbox_id = await postgres_outbox_repo.add_event(event)
        for i in range(3):
            await postgres_outbox_repo.increment_retry(outbox_id, f"Error {i}")
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == 3

    async def test_mark_failed(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Fail Test",
            quantity=1,
        )
        outbox_id = await postgres_outbox_repo.add_event(event)
        await postgres_outbox_repo.mark_failed(outbox_id, "Permanent failure")
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 0


class TestPostgreSQLOutboxRepositoryCleanup:
    """Tests for cleanup operations."""

    async def test_cleanup_published_events(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
        postgres_engine: AsyncEngine,
    ) -> None:
        from sqlalchemy import text

        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Cleanup Test",
            quantity=1,
        )
        outbox_id = await postgres_outbox_repo.add_event(event)
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
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Recent Test",
            quantity=1,
        )
        outbox_id = await postgres_outbox_repo.add_event(event)
        await postgres_outbox_repo.mark_published(outbox_id)
        deleted = await postgres_outbox_repo.cleanup_published(days=7)
        assert deleted == 0

    async def test_cleanup_keeps_pending(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Pending Test",
            quantity=1,
        )
        await postgres_outbox_repo.add_event(event)
        deleted = await postgres_outbox_repo.cleanup_published(days=0)
        assert deleted == 0
        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1


class TestPostgreSQLOutboxRepositoryStatistics:
    """Tests for outbox statistics."""

    async def test_get_stats_empty(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        stats = await postgres_outbox_repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.oldest_pending is None
        assert stats.avg_retries == 0.0

    async def test_get_stats_with_entries(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        for i in range(3):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Pending {i}",
                quantity=i,
            )
            outbox_id = await postgres_outbox_repo.add_event(event)
            if i == 0:
                await postgres_outbox_repo.increment_retry(outbox_id, "Error")
                await postgres_outbox_repo.increment_retry(outbox_id, "Error")

        published_event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Published",
            quantity=99,
        )
        published_id = await postgres_outbox_repo.add_event(published_event)
        await postgres_outbox_repo.mark_published(published_id)

        failed_event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Failed",
            quantity=0,
        )
        failed_id = await postgres_outbox_repo.add_event(failed_event)
        await postgres_outbox_repo.mark_failed(failed_id, "Permanent failure")

        stats = await postgres_outbox_repo.get_stats()
        assert stats.pending_count == 3
        assert stats.published_count == 1
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None
        assert stats.avg_retries > 0
