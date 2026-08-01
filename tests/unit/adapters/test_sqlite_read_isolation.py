"""SQLite reads run under the same lock as appends.

All statements share one aiosqlite connection (required for `":memory:"`
databases, whose contents live only as long as the creating connection).
`append` is multi-statement and each INSERT is separately awaited, so
before this fix a read scheduled between two INSERTs ran inside the
append's open transaction and observed a torn batch -- or minted a
`Position` for a row that was then rolled back.

The interception below wraps the connection's `execute` beneath the
adapter, not around `_lock`, so the lock discipline under test stays
exactly what production runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.sqlite.store import SQLiteEventStore
from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import DuplicateEventError
from eventsource.ports import ExpectedVersion, collect
from eventsource.testing.conformance_ports._fixtures import (
    ConformanceEvent,
    make_event,
    make_stream,
)

pytestmark = pytest.mark.asyncio


def _make_registry() -> EventRegistry:
    """Fresh registry with `ConformanceEvent` registered.

    `ConformanceEvent` is never registered into `default_registry` --
    registration is explicit. SQLite round-trips events through JSON, so
    it must be able to look the class up again on read.
    """
    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


class _PausingConnection:
    """Wraps the store's connection, pausing after the first event INSERT.

    Interception sits *beneath* the adapter -- the store still calls
    `execute` on what it believes is its connection, and `self._lock` is
    untouched -- so the lock discipline under test is exactly what
    production runs.
    """

    def __init__(self, conn) -> None:  # type: ignore[no-untyped-def]
        self._conn = conn
        self.first_insert_landed = asyncio.Event()
        self.release = asyncio.Event()
        self._paused = False

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        return getattr(self._conn, name)

    async def execute(self, sql, parameters=None):  # type: ignore[no-untyped-def]
        cursor = await (
            self._conn.execute(sql, parameters)
            if parameters is not None
            else self._conn.execute(sql)
        )
        if not self._paused and "INSERT INTO events" in str(sql):
            self._paused = True
            self.first_insert_landed.set()
            await self.release.wait()
        return cursor


@pytest.fixture
async def paused_store() -> AsyncIterator[tuple[SQLiteEventStore, _PausingConnection]]:
    store = SQLiteEventStore(":memory:", event_registry=_make_registry())
    conn = await store._conn()
    pausing = _PausingConnection(conn)
    store._connection = pausing  # type: ignore[assignment]
    yield store, pausing
    # Unblock any append still paused by a failed test, then close --
    # an unclosed aiosqlite connection leaves its non-daemon thread
    # alive and hangs interpreter shutdown.
    pausing.release.set()
    await store.close()


class TestReadsDoNotObserveAnOpenAppend:
    async def test_read_all_sees_zero_or_two_events_never_one(
        self, paused_store: tuple[SQLiteEventStore, _PausingConnection]
    ) -> None:
        store, pausing = paused_store
        stream = make_stream()
        events = [make_event(stream.aggregate_id), make_event(stream.aggregate_id)]

        append_task = asyncio.create_task(store.append(stream, events, ExpectedVersion.no_stream()))
        await asyncio.wait_for(pausing.first_insert_landed.wait(), 1.0)

        read_task = asyncio.create_task(collect(store.read_all()))
        position_task = asyncio.create_task(store.current_position())

        # Both must block on the write lock while the append's
        # transaction is open.
        done, _ = await asyncio.wait({read_task, position_task}, timeout=0.1)
        assert done == set()

        pausing.release.set()
        await append_task
        envelopes = await read_task
        position = await position_task

        assert len(envelopes) in (0, 2)
        if envelopes:
            assert position is not None

    async def test_a_rolled_back_append_is_never_observed(
        self, paused_store: tuple[SQLiteEventStore, _PausingConnection]
    ) -> None:
        store, pausing = paused_store
        stream = make_stream()
        duplicate = make_event(stream.aggregate_id)

        # Land the duplicate first so the second INSERT of the next batch
        # violates the event_id unique constraint and rolls the whole
        # batch back.
        pausing.release.set()
        await store.append(stream, [duplicate], ExpectedVersion.no_stream())

        pausing.release.clear()
        pausing.first_insert_landed.clear()
        pausing._paused = False

        append_task = asyncio.create_task(
            store.append(
                stream,
                [make_event(stream.aggregate_id), duplicate],
                ExpectedVersion.exact(1),
            )
        )
        await asyncio.wait_for(pausing.first_insert_landed.wait(), 1.0)

        read_task = asyncio.create_task(collect(store.read_all()))
        done, _ = await asyncio.wait({read_task}, timeout=0.1)
        assert done == set()

        pausing.release.set()
        with pytest.raises(DuplicateEventError):
            await append_task

        # Only the original event survives: the reader never saw the
        # first row of the rolled-back batch.
        assert len(await read_task) == 1
