"""Conformance suite for the `OutboxRepository` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance.
Cleanup cutoff semantics differ per backend (PostgreSQL computes
`NOW() - INTERVAL`, SQLite `datetime('now', ...)`, and the in-memory
adapter a Python `timedelta`), so only the status partition is asserted
here -- exact cutoff boundaries belong in the backend-specific modules,
following the pattern `dlq.py` established.
"""

import json
from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.outbox import OutboxRepository, outbox_event_data
from eventsource.testing.conformance_ports._fixtures import make_event


class OutboxRepositoryConformance(ABC):
    """Conformance suite for `OutboxRepository` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `OutboxRepository`."""
        raise NotImplementedError

    async def test_add_event_then_get_pending_events_round_trips_fields(
        self, store: OutboxRepository
    ) -> None:
        event = make_event(aggregate_id=uuid4())

        await store.add_event(event)

        (entry,) = await store.get_pending_events()
        assert entry.event_id == event.event_id
        assert entry.event_type == event.event_type
        assert entry.aggregate_id == event.aggregate_id
        assert entry.aggregate_type == event.aggregate_type
        assert entry.tenant_id == event.tenant_id
        assert entry.status == "pending"
        assert entry.retry_count == 0

    async def test_event_data_round_trips_the_outbox_event_data_payload(
        self, store: OutboxRepository
    ) -> None:
        event = make_event(aggregate_id=uuid4())

        await store.add_event(event)

        (entry,) = await store.get_pending_events()
        data = entry.event_data
        parsed = json.loads(data) if isinstance(data, str) else data

        assert set(parsed.keys()) == set(outbox_event_data(event).keys())
        assert parsed["payload"] == event.model_dump(mode="json")

    async def test_get_pending_events_returns_oldest_first(self, store: OutboxRepository) -> None:
        event1 = make_event(aggregate_id=uuid4())
        event2 = make_event(aggregate_id=uuid4())
        event3 = make_event(aggregate_id=uuid4())

        await store.add_event(event1)
        await store.add_event(event2)
        await store.add_event(event3)

        pending = await store.get_pending_events(limit=1000)

        assert [e.event_id for e in pending] == [
            event1.event_id,
            event2.event_id,
            event3.event_id,
        ]

    async def test_get_pending_events_honors_limit(self, store: OutboxRepository) -> None:
        for _ in range(3):
            await store.add_event(make_event(aggregate_id=uuid4()))

        pending = await store.get_pending_events(limit=2)

        assert len(pending) == 2

    async def test_mark_published_removes_entry_from_pending_and_stamps_published_at(
        self, store: OutboxRepository
    ) -> None:
        event = make_event(aggregate_id=uuid4())
        outbox_id = await store.add_event(event)

        await store.mark_published(outbox_id)

        assert await store.get_pending_events() == []
        stats = await store.get_stats()
        assert stats.published_count == 1

    async def test_mark_failed_sets_status_and_records_last_error(
        self, store: OutboxRepository
    ) -> None:
        event = make_event(aggregate_id=uuid4())
        outbox_id = await store.add_event(event)

        await store.mark_failed(outbox_id, "boom")

        assert await store.get_pending_events() == []
        stats = await store.get_stats()
        assert stats.failed_count == 1

    async def test_increment_retry_raises_retry_count_and_records_error_while_staying_pending(
        self, store: OutboxRepository
    ) -> None:
        event = make_event(aggregate_id=uuid4())
        outbox_id = await store.add_event(event)

        await store.increment_retry(outbox_id, "first error")
        await store.increment_retry(outbox_id, "second error")

        (entry,) = await store.get_pending_events()
        assert entry.status == "pending"
        assert entry.retry_count == 2

    async def test_cleanup_published_deletes_only_published_entries(
        self, store: OutboxRepository
    ) -> None:
        published_event = make_event(aggregate_id=uuid4())
        published_id = await store.add_event(published_event)
        await store.mark_published(published_id)

        failed_event = make_event(aggregate_id=uuid4())
        failed_id = await store.add_event(failed_event)
        await store.mark_failed(failed_id, "boom")

        await store.add_event(make_event(aggregate_id=uuid4()))

        deleted = await store.cleanup_published(days=0)

        assert deleted == 1
        stats = await store.get_stats()
        assert stats.pending_count == 1
        assert stats.failed_count == 1

    async def test_get_stats_counts_statuses_and_averages_pending_retries(
        self, store: OutboxRepository
    ) -> None:
        pending_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.increment_retry(pending_id, "err")
        await store.increment_retry(pending_id, "err")
        await store.add_event(make_event(aggregate_id=uuid4()))

        published_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(published_id)

        failed_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_failed(failed_id, "boom")

        stats = await store.get_stats()

        assert stats.pending_count == 2
        assert stats.published_count == 1
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None
        assert stats.avg_retries == 1.0

    async def test_unknown_outbox_id_is_a_no_op_for_mutating_methods(
        self, store: OutboxRepository
    ) -> None:
        unknown_id = uuid4()

        await store.mark_published(unknown_id)
        await store.mark_failed(unknown_id, "boom")
        await store.increment_retry(unknown_id, "boom")

        assert await store.get_pending_events() == []
        stats = await store.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
