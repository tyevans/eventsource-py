"""Concurrent transaction and atomic consistency tests for SQLite transactional outbox."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest

from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.adapters.sqlite.store import SQLiteEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import ExpectedVersion
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    pass

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class OrderCreated(DomainEvent):
    """Test domain event for transactional outbox tests."""

    aggregate_type: str = "Order"
    order_number: str = "ORD-001"
    amount: float = 99.99


registry = EventRegistry()
registry.register(OrderCreated)


@pytest.fixture
async def temp_db_path() -> AsyncIterator[str]:
    """Provide a unique temporary file path for a SQLite database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield str(Path(tmpdir) / "test_outbox.db")


@pytest.mark.asyncio
async def test_outbox_disabled_by_default(temp_db_path: str) -> None:
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=False)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")
    event = OrderCreated(aggregate_id=order_id, order_number="ORD-1")
    await store.append(stream, [event], ExpectedVersion.no_stream())

    assert await store.event_exists(event.event_id)
    assert len(await outbox_repo.get_pending_events()) == 0
    await store.close()


@pytest.mark.asyncio
async def test_outbox_enabled_stores_events_and_outbox_atomically(temp_db_path: str) -> None:
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=True)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id, tenant_id = uuid4(), uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")
    events = [
        OrderCreated(aggregate_id=order_id, tenant_id=tenant_id, order_number="ORD-1"),
        OrderCreated(aggregate_id=order_id, tenant_id=tenant_id, order_number="ORD-2"),
    ]
    await store.append(stream, events, ExpectedVersion.no_stream())

    assert await store.event_exists(events[0].event_id)
    assert await store.event_exists(events[1].event_id)

    pending = await outbox_repo.get_pending_events()
    assert len(pending) == 2
    assert [p.event_id for p in pending] == [events[0].event_id, events[1].event_id]
    assert pending[0].tenant_id == tenant_id
    assert pending[0].aggregate_type == "Order"
    assert pending[0].aggregate_id == order_id
    assert pending[0].status == "pending"
    await store.close()


@pytest.mark.asyncio
async def test_outbox_rolls_back_on_optimistic_concurrency_conflict(temp_db_path: str) -> None:
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=True)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")
    initial_event = OrderCreated(aggregate_id=order_id, order_number="ORD-INITIAL")
    await store.append(stream, [initial_event], ExpectedVersion.no_stream())

    conflict_event = OrderCreated(aggregate_id=order_id, order_number="ORD-CONFLICT")
    with pytest.raises(OptimisticLockError):
        await store.append(stream, [conflict_event], ExpectedVersion.no_stream())

    assert not await store.event_exists(conflict_event.event_id)
    pending = await outbox_repo.get_pending_events()
    assert len(pending) == 1
    assert pending[0].event_id == initial_event.event_id
    await store.close()


@pytest.mark.asyncio
async def test_outbox_rolls_back_on_duplicate_event_id(temp_db_path: str) -> None:
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=True)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id, dup_id = uuid4(), uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")
    events = [
        OrderCreated(event_id=dup_id, aggregate_id=order_id, order_number="ORD-A"),
        OrderCreated(event_id=dup_id, aggregate_id=order_id, order_number="ORD-B"),
    ]

    with pytest.raises(DuplicateEventError):
        await store.append(stream, events, ExpectedVersion.no_stream())

    assert len(await outbox_repo.get_pending_events()) == 0
    await store.close()


@pytest.mark.asyncio
async def test_concurrent_transactions_wal_mode(temp_db_path: str) -> None:
    store = SQLiteEventStore(
        temp_db_path, event_registry=registry, wal_mode=True, outbox_enabled=True
    )
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    num_workers, events_per_worker = 5, 10
    total_expected = num_workers * events_per_worker

    async def worker(worker_idx: int) -> None:
        order_id = uuid4()
        stream = StreamId(aggregate_id=order_id, category="Order")
        for i in range(events_per_worker):
            evt = OrderCreated(aggregate_id=order_id, order_number=f"W{worker_idx}-ORD-{i}")
            expected = ExpectedVersion.no_stream() if i == 0 else ExpectedVersion.stream_exists()
            await store.append(stream, [evt], expected)

    await asyncio.gather(*(worker(w) for w in range(num_workers)))

    pending = await outbox_repo.get_pending_events(limit=1000)
    assert len(pending) == total_expected

    stats = await outbox_repo.get_stats()
    assert stats.pending_count == total_expected
    assert stats.published_count == 0
    assert stats.failed_count == 0

    for entry in pending[:25]:
        await outbox_repo.mark_published(entry.id)

    updated_stats = await outbox_repo.get_stats()
    assert updated_stats.pending_count == 25
    assert updated_stats.published_count == 25
    await store.close()
