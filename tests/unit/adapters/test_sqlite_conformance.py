"""Conformance tests for SQLiteEventStore against the port suites."""

import threading
import time
from collections.abc import AsyncIterator

import pytest
from hypothesis import settings

from tests.conftest import skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


def _make_registry():
    """Fresh registry with `ConformanceEvent` registered.

    `ConformanceEvent` (from the shared conformance fixtures) is never
    registered into `default_registry` -- registration is explicit
    (`@register_event` / `EventRegistry.register`), unlike the memory
    adapter, which stores Python objects directly and never needs a
    registry. SQLite round-trips events through JSON, so it must be able
    to look the class up again on read.
    """
    from eventsource.events.registry import EventRegistry
    from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


from eventsource.adapters.sqlite import SQLiteEventStore  # noqa: E402
from eventsource.ports import ExpectedVersion  # noqa: E402
from eventsource.testing.conformance_ports import (  # noqa: E402
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    StreamReaderConformance,
)
from eventsource.testing.conformance_ports._fixtures import (  # noqa: E402
    ConformanceEvent,
    make_stream,
)
from eventsource.testing.conformance_ports.stateful import StoreStateMachine  # noqa: E402
from eventsource.testing.sync_facade import SyncStoreFacade  # noqa: E402


class TestSQLiteAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=_make_registry())
        yield store
        await store.close()


class TestSQLiteStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=_make_registry())
        yield store
        await store.close()


class TestSQLiteEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=_make_registry())
        yield store
        await store.close()


class TestSQLiteGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=_make_registry())
        yield store
        await store.close()


class TestSQLiteCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=_make_registry())
        yield store
        await store.close()


class SQLiteStateMachine(StoreStateMachine):
    def make_store(self) -> SyncStoreFacade:
        return SyncStoreFacade(SQLiteEventStore(":memory:", event_registry=_make_registry()))


TestSQLiteStateful = SQLiteStateMachine.TestCase
# derandomize=True: pytest-randomly reseeds hypothesis's global random source
# per test, which otherwise makes this state machine's example generation
# nondeterministic across runs (project-known gotcha). max_examples is lower
# than the memory adapter's (25) because SQLite round-trips through real I/O
# and JSON (de)serialization per event, making each example slower.
TestSQLiteStateful.settings = settings(max_examples=10, deadline=None, derandomize=True)


def test_sync_facade_close_closes_underlying_store() -> None:
    """`SyncStoreFacade.close()` must close the wrapped store, not just the loop.

    Regression test: aiosqlite's connection is backed by a non-daemon
    `threading.Thread`; leaving it open leaks a thread per facade and
    prevents clean interpreter shutdown.
    """
    store = SQLiteEventStore(":memory:", event_registry=_make_registry())
    facade = SyncStoreFacade(store)
    stream = make_stream()
    facade.append(
        stream,
        [ConformanceEvent(aggregate_id=stream.aggregate_id)],
        ExpectedVersion.any_(),
    )
    del stream

    before = threading.active_count()

    facade.close()

    deadline = time.monotonic() + 2.0
    after = threading.active_count()
    while after > before - 1 and time.monotonic() < deadline:
        time.sleep(0.01)
        after = threading.active_count()

    assert after == before - 1
