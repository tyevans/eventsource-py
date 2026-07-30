"""Conformance tests for PostgreSQLEventStore against the port suites.

Subclasses the five store-port conformance suites (appender, stream reader,
event lookup, global feed, category query). The hypothesis stateful machine
(`StoreStateMachine`) is exercised for the memory and sqlite adapters only
(see Task 12 brief) -- it is not required here.

Runs against the private `ports_conformance` database (see local
`conftest.py`'s `ports_postgres_connection_url`) rather than the shared
database the legacy `tests/integration/stores/test_postgresql.py` suite
owns, since this module's `events` table follows the canonical migrations
schema (`tenant_id UUID`) while the legacy fixtures provision `tenant_id
VARCHAR(255)`.
"""

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from ..conftest import skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]


def _make_registry():
    """Fresh registry with `ConformanceEvent` registered.

    Mirrors the SQLite adapter's conformance test module: `ConformanceEvent`
    is never registered into `default_registry`, and PostgreSQL round-trips
    events through JSONB, so it must be able to look the class up again on
    read.
    """
    from eventsource.events.registry import EventRegistry
    from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


from eventsource.adapters.postgresql import PostgreSQLEventStore  # noqa: E402
from eventsource.testing.conformance_ports import (  # noqa: E402
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    StreamReaderConformance,
)


async def _fresh_store(connection_url: str) -> PostgreSQLEventStore:
    """Create a store on a fresh engine, with the `events` table dropped first.

    Each test gets its own `AsyncEngine` (disposed by `store.close()`), and
    the `events` table is dropped before the store lazily recreates it on
    first use -- this isolates each test from the others within the
    session-scoped private `ports_conformance` database.
    """
    engine = create_async_engine(connection_url)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))
    return PostgreSQLEventStore(engine, event_registry=_make_registry(), create_schema=True)


class TestPostgreSQLAppender(AppenderConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        store = await _fresh_store(ports_postgres_connection_url)
        yield store
        await store.close()


class TestPostgreSQLStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        store = await _fresh_store(ports_postgres_connection_url)
        yield store
        await store.close()


class TestPostgreSQLEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        store = await _fresh_store(ports_postgres_connection_url)
        yield store
        await store.close()


class TestPostgreSQLGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        store = await _fresh_store(ports_postgres_connection_url)
        yield store
        await store.close()


class TestPostgreSQLCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        store = await _fresh_store(ports_postgres_connection_url)
        yield store
        await store.close()


async def test_store_id_stable_across_restarts(ports_postgres_connection_url: str) -> None:
    """`store_id` derives from the engine URL's database name -- stable, not random."""
    engine_a: AsyncEngine = create_async_engine(ports_postgres_connection_url)
    engine_b: AsyncEngine = create_async_engine(ports_postgres_connection_url)
    try:
        store_a = PostgreSQLEventStore(engine_a, event_registry=_make_registry())
        store_b = PostgreSQLEventStore(engine_b, event_registry=_make_registry())
        assert store_a.store_id == store_b.store_id
        assert store_a.store_id == f"pg:{engine_a.url.database}"
    finally:
        await engine_a.dispose()
        await engine_b.dispose()
