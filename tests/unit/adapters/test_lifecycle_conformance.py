"""Conformance tests for every `SupportsClose` implementer in the tree.

Three bindings: `SQLiteEventStore`, `SQLiteSnapshotStore` (both own the
`aiosqlite` connection they open from a path), and `PostgreSQLEventStore`
(engine always caller-supplied, ownership opt-in via `owns_engine`).

The PostgreSQL binding needs a real `AsyncEngine` -- not a mock, because
the property under test is "the caller's engine is *still usable* after
close()", which a mock cannot answer. It does not need a live PostgreSQL
server: `PostgreSQLEventStore.__init__` reads only `engine.url.database`
and `close()` calls only `engine.dispose()`, so a real aiosqlite-backed
`AsyncEngine` exercises the identical code path while remaining
connectable in-process. Query behaviour against real PostgreSQL is
covered by the integration suite, not here.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.snapshots import Snapshot
from eventsource.testing.conformance_ports import (
    CallerOwnedResourceCase,
    SupportsCloseConformance,
)
from tests.conftest import skip_if_no_aiosqlite

pytestmark = [skip_if_no_aiosqlite]


def _registry():
    from eventsource.domain.event_registry import EventRegistry
    from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


class TestSQLiteEventStoreLifecycle(SupportsCloseConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[object]:
        from eventsource.adapters.sqlite import SQLiteEventStore

        instance = SQLiteEventStore(":memory:", event_registry=_registry())
        try:
            yield instance
        finally:
            await instance.close()

    async def use(self, store: object) -> None:
        from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

        aggregate_id = uuid4()
        await store.append(  # type: ignore[attr-defined]
            StreamId(aggregate_id=aggregate_id, category="Conformance"),
            [ConformanceEvent(aggregate_id=aggregate_id)],
            ExpectedVersion.no_stream(),
        )

    @pytest.fixture
    def caller_owned_case(self) -> CallerOwnedResourceCase | None:
        return None  # opens its own connection from a path


class TestSQLiteSnapshotStoreLifecycle(SupportsCloseConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[object]:
        from eventsource.adapters.sqlite import SQLiteSnapshotStore

        instance = SQLiteSnapshotStore(":memory:")
        try:
            yield instance
        finally:
            await instance.close()

    async def use(self, store: object) -> None:
        await store.save_snapshot(  # type: ignore[attr-defined]
            Snapshot(
                aggregate_id=uuid4(),
                aggregate_type="Conformance",
                version=1,
                state={"value": 1},
                schema_version=1,
                created_at=datetime.now(UTC),
            )
        )

    @pytest.fixture
    def caller_owned_case(self) -> CallerOwnedResourceCase | None:
        return None  # opens its own connection from a path


class TestPostgreSQLEventStoreLifecycle(SupportsCloseConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[object]:
        from eventsource import create_async_engine
        from eventsource.adapters.postgresql import PostgreSQLEventStore

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        try:
            yield PostgreSQLEventStore(engine, event_registry=_registry())
        finally:
            await engine.dispose()

    async def use(self, store: object) -> None:
        # The engine is caller-supplied and already live; there is no lazy
        # resource for this adapter to acquire.
        return None

    @pytest.fixture
    async def caller_owned_case(self) -> AsyncIterator[CallerOwnedResourceCase | None]:
        from eventsource import create_async_engine
        from eventsource.adapters.postgresql import PostgreSQLEventStore

        borrowed = create_async_engine("sqlite+aiosqlite:///:memory:")
        owned = create_async_engine("sqlite+aiosqlite:///:memory:")
        # Prime both engines so their pools actually hold a connection --
        # otherwise "was it released?" would be trivially true, and
        # `AsyncEngine.dispose()` on a cold engine is unobservable.
        for engine in (borrowed, owned):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        borrowed_pool_before = borrowed.pool
        pool_before_close = owned.pool

        async def is_resource_usable() -> bool:
            # Two things must hold: the caller's pooled connections were not
            # torn down (`dispose()` swaps in a fresh pool, so pool identity
            # is the observable), and the engine still serves queries. The
            # identity check is load-bearing: a disposed `AsyncEngine` will
            # happily open a *new* connection, so "can it query?" alone
            # cannot distinguish disposed from untouched.
            if borrowed.pool is not borrowed_pool_before:
                return False
            async with borrowed.connect() as conn:
                return (await conn.execute(text("SELECT 1"))).scalar_one() == 1

        async def was_resource_released() -> bool:
            # `AsyncEngine.dispose()` closes the pooled connections and
            # installs a fresh pool; identity change is the observable.
            return owned.pool is not pool_before_close

        try:
            yield CallerOwnedResourceCase(
                store=PostgreSQLEventStore(borrowed, event_registry=_registry()),
                is_resource_usable=is_resource_usable,
                handed_over=PostgreSQLEventStore(
                    owned, event_registry=_registry(), owns_engine=True
                ),
                was_resource_released=was_resource_released,
            )
        finally:
            await borrowed.dispose()
            await owned.dispose()
