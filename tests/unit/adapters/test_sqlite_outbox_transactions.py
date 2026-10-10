"""Concurrent transaction and atomic consistency tests for SQLite transactional outbox.

Verifies that:
1. When outbox is disabled, appends do not write to event_outbox.
2. When outbox is enabled, appends write to event_outbox in the same transaction.
3. Rolled back appends (concurrency conflict, duplicate event) never commit outbox rows.
4. Concurrent multi-worker appends in SQLite WAL mode maintain strict atomicity
   and outbox consistency.
"""

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
        db_path = str(Path(tmpdir) / "test_outbox.db")
        yield db_path


@pytest.mark.asyncio
async def test_outbox_disabled_by_default(temp_db_path: str) -> None:
    """When outbox_enabled=False, events table is updated but outbox table remains empty."""
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=False)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")
    event = OrderCreated(aggregate_id=order_id, order_number="ORD-1")

    await store.append(stream, [event], ExpectedVersion.no_stream())

    # Event exists in store
    assert await store.event_exists(event.event_id)

    # Outbox is empty
    pending = await outbox_repo.get_pending_events()
    assert len(pending) == 0

    await store.close()


@pytest.mark.asyncio
async def test_outbox_enabled_stores_events_and_outbox_atomically(
    temp_db_path: str,
) -> None:
    """When outbox_enabled=True, store.append atomically inserts outbox rows."""
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=True)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id = uuid4()
    tenant_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")
    events = [
        OrderCreated(aggregate_id=order_id, tenant_id=tenant_id, order_number="ORD-1"),
        OrderCreated(aggregate_id=order_id, tenant_id=tenant_id, order_number="ORD-2"),
    ]

    await store.append(stream, events, ExpectedVersion.no_stream())

    # Both events exist in store
    assert await store.event_exists(events[0].event_id)
    assert await store.event_exists(events[1].event_id)

    # Both events are pending in outbox
    pending = await outbox_repo.get_pending_events()
    assert len(pending) == 2
    assert [p.event_id for p in pending] == [events[0].event_id, events[1].event_id]
    assert pending[0].tenant_id == tenant_id
    assert pending[0].aggregate_type == "Order"
    assert pending[0].aggregate_id == order_id
    assert pending[0].status == "pending"

    await store.close()


@pytest.mark.asyncio
async def test_outbox_rolls_back_on_optimistic_concurrency_conflict(
    temp_db_path: str,
) -> None:
    """When append encounters OptimisticLockError, outbox insert is rolled back."""
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=True)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")

    # Initial append succeeds
    initial_event = OrderCreated(aggregate_id=order_id, order_number="ORD-INITIAL")
    await store.append(stream, [initial_event], ExpectedVersion.no_stream())

    # Conflicting append expects no_stream but stream already exists
    conflict_event = OrderCreated(aggregate_id=order_id, order_number="ORD-CONFLICT")
    with pytest.raises(OptimisticLockError):
        await store.append(stream, [conflict_event], ExpectedVersion.no_stream())

    # Conflict event was not stored
    assert not await store.event_exists(conflict_event.event_id)

    # Outbox only contains the initial event, conflict event was rolled back
    pending = await outbox_repo.get_pending_events()
    assert len(pending) == 1
    assert pending[0].event_id == initial_event.event_id

    await store.close()


@pytest.mark.asyncio
async def test_outbox_rolls_back_on_duplicate_event_id(
    temp_db_path: str,
) -> None:
    """When a batch contains duplicate event_id, entire batch and outbox roll back."""
    store = SQLiteEventStore(temp_db_path, event_registry=registry, outbox_enabled=True)
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    order_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")

    dup_id = uuid4()
    events = [
        OrderCreated(event_id=dup_id, aggregate_id=order_id, order_number="ORD-A"),
        OrderCreated(event_id=dup_id, aggregate_id=order_id, order_number="ORD-B"),
    ]

    with pytest.raises(DuplicateEventError):
        await store.append(stream, events, ExpectedVersion.no_stream())

    # Outbox remains empty
    pending = await outbox_repo.get_pending_events()
    assert len(pending) == 0

    await store.close()


@pytest.mark.asyncio
async def test_concurrent_transactions_wal_mode(temp_db_path: str) -> None:
    """Concurrent appends in WAL mode maintain atomic append-and-outbox consistency."""
    store = SQLiteEventStore(
        temp_db_path,
        event_registry=registry,
        wal_mode=True,
        outbox_enabled=True,
    )
    conn = await store._conn()
    outbox_repo = SQLiteOutboxRepository(conn)

    num_workers = 5
    events_per_worker = 10
    total_expected = num_workers * events_per_worker

    async def worker(worker_idx: int) -> None:
        order_id = uuid4()
        stream = StreamId(aggregate_id=order_id, category="Order")
        for i in range(events_per_worker):
            evt = OrderCreated(
                aggregate_id=order_id,
                order_number=f"W{worker_idx}-ORD-{i}",
            )
            expected = ExpectedVersion.no_stream() if i == 0 else ExpectedVersion.stream_exists()
            await store.append(stream, [evt], expected)

    # Run workers concurrently
    await asyncio.gather(*(worker(w) for w in range(num_workers)))

    # Verify outbox repository has all events
    pending = await outbox_repo.get_pending_events(limit=1000)
    assert len(pending) == total_expected

    # Verify stats reflect exact counts
    stats = await outbox_repo.get_stats()
    assert stats.pending_count == total_expected
    assert stats.published_count == 0
    assert stats.failed_count == 0

    # Mark half as published
    for entry in pending[:25]:
        await outbox_repo.mark_published(entry.id)

    updated_stats = await outbox_repo.get_stats()
    assert updated_stats.pending_count == 25
    assert updated_stats.published_count == 25

    await store.close()
