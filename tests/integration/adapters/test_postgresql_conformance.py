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
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

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


from eventsource.adapters.postgresql import (  # noqa: E402
    PostgreSQLEventStore,
    PostgreSQLSnapshotStore,
)
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository  # noqa: E402
from eventsource.adapters.sql.dlq import SQLDLQRepository  # noqa: E402
from eventsource.migrations import get_schema  # noqa: E402
from eventsource.testing.conformance_ports import (  # noqa: E402
    AppenderConformance,
    CategoryQueryConformance,
    CheckpointRepositoryConformance,
    DLQRepositoryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    SnapshotConformance,
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


class TestPostgreSQLSnapshotStore(SnapshotConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLSnapshotStore]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS snapshots CASCADE"))
            # get_schema returns a multi-statement script; asyncpg's prepared-statement
            # path rejects those, so run it via the raw driver connection (simple query
            # protocol), same as PostgreSQLEventStore._ensure_schema.
            raw = await conn.get_raw_connection()
            driver_connection = raw.driver_connection
            assert driver_connection is not None
            await driver_connection.execute(get_schema("snapshots", "postgresql"))
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        yield PostgreSQLSnapshotStore(session_factory)
        await engine.dispose()


# `get_schema("checkpoints"/"dlq", "postgresql")` -- unlike "events" and
# "snapshots" -- ships PL/pgSQL helper functions (dollar-quoted bodies with
# `GET DIAGNOSTICS`) alongside the DDL. Neither `SQLCheckpointRepository`
# nor `SQLDLQRepository` calls those functions (both issue plain
# parameterized SQL from Python), but asyncpg's raw-connection `execute()` --
# the simple query protocol used elsewhere in this module to run
# `events.sql`/`snapshots.sql` as one script -- mis-splits the dollar-quoted
# bodies and fails with `PostgresSyntaxError: unrecognized GET DIAGNOSTICS
# item`. So these two tables are provisioned as bare DDL, one statement at a
# time via `text()`, mirroring `tests/integration/conftest.py`'s
# `CHECKPOINTS_SCHEMA_STATEMENTS`/`DLQ_SCHEMA_STATEMENTS`.
_CHECKPOINTS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS projection_checkpoints (
        projection_name VARCHAR(255) PRIMARY KEY,
        last_event_id UUID,
        last_event_type VARCHAR(255),
        last_processed_at TIMESTAMPTZ,
        events_processed BIGINT NOT NULL DEFAULT 0,
        global_position BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
]

_DLQ_DDL = [
    """
    CREATE TABLE IF NOT EXISTS dead_letter_queue (
        id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL,
        projection_name VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        event_data JSONB NOT NULL,
        error_message TEXT NOT NULL,
        error_stacktrace TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status VARCHAR(20) NOT NULL DEFAULT 'failed',
        resolved_at TIMESTAMPTZ,
        resolved_by VARCHAR(255),
        CONSTRAINT chk_dlq_status_ports CHECK (status IN ('failed', 'retrying', 'resolved')),
        CONSTRAINT uq_dlq_event_projection_ports UNIQUE (event_id, projection_name)
    )
    """,
]


class TestPostgreSQLCheckpointRepository(CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[SQLCheckpointRepository]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS projection_checkpoints CASCADE"))
            await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))
            for statement in _CHECKPOINTS_DDL:
                await conn.execute(text(statement))
            raw = await conn.get_raw_connection()
            driver_connection = raw.driver_connection
            assert driver_connection is not None
            await driver_connection.execute(get_schema("events", "postgresql"))
        yield SQLCheckpointRepository(engine)
        await engine.dispose()


class TestPostgreSQLDLQRepository(DLQRepositoryConformance):
    @pytest.fixture
    async def store(self, ports_postgres_connection_url: str) -> AsyncIterator[SQLDLQRepository]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS dead_letter_queue CASCADE"))
            for statement in _DLQ_DDL:
                await conn.execute(text(statement))
        yield SQLDLQRepository(engine)
        await engine.dispose()

    async def test_postgres_delete_resolved_events_removes_past_cutoff_entries(
        self, store: SQLDLQRepository
    ) -> None:
        # Same cutoff semantics as the sqlite adapter (both use the shared
        # `SQLDLQRepository`): `now` minus `older_than_days`, not
        # truncated to midnight like the memory adapter.
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()
        await store.mark_resolved(entry.id, "alice")

        deleted = await store.delete_resolved_events(older_than_days=0)

        assert deleted == 1
        assert await store.get_failed_event_by_id(entry.id) is None


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
