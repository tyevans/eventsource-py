"""Conformance tests for InMemoryOutboxRepository against the port suites."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryOutboxRepository
from eventsource.testing.conformance_ports import OutboxRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import make_event


class TestMemoryOutboxRepository(OutboxRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryOutboxRepository]:
        yield InMemoryOutboxRepository()

    async def test_memory_cleanup_published_cutoff_is_a_python_timedelta(
        self, store: InMemoryOutboxRepository
    ) -> None:
        # Unlike `dlq.py`'s in-memory adapter (which truncates its cutoff
        # to midnight), the outbox adapter computes
        # `datetime.now(UTC) - timedelta(days=days)` at call time -- an
        # entry published moments ago is *already* past a `days=0` cutoff
        # by the time cleanup runs, so it does not survive.
        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)

        deleted = await store.cleanup_published(days=0)

        assert deleted == 1
        stats = await store.get_stats()
        assert stats.published_count == 0

    async def test_memory_cleanup_published_keeps_entries_within_the_retention_window(
        self, store: InMemoryOutboxRepository
    ) -> None:
        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)

        deleted = await store.cleanup_published(days=7)

        assert deleted == 0
        stats = await store.get_stats()
        assert stats.published_count == 1

    async def test_memory_cleanup_published_removes_entries_past_the_cutoff(
        self, store: InMemoryOutboxRepository
    ) -> None:
        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)
        store._entries[outbox_id].published_at = datetime.now(UTC) - timedelta(days=10)

        deleted = await store.cleanup_published(days=7)

        assert deleted == 1
        stats = await store.get_stats()
        assert stats.published_count == 0

    async def test_clear_empties_the_repository(self, store: InMemoryOutboxRepository) -> None:
        await store.add_event(make_event(aggregate_id=uuid4()))

        await store.clear()

        assert await store.get_pending_events() == []
