"""Conformance tests for SQLiteEventStore against the port suites."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import aiosqlite
import pytest
from hypothesis import settings
from sqlalchemy import text

from tests.conftest import skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]
from eventsource import create_async_engine  # noqa: E402
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository  # noqa: E402
from eventsource.adapters.sql.dlq import SQLDLQRepository  # noqa: E402
from eventsource.adapters.sql.schemas import get_schema  # noqa: E402
from eventsource.adapters.sqlite import SQLiteEventStore, SQLiteSnapshotStore  # noqa: E402
from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository  # noqa: E402
from eventsource.testing.conformance_ports import (  # noqa: E402
    AppenderConformance,
    CategoryQueryConformance,
    CheckpointRepositoryConformance,
    DLQRepositoryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    OutboxRepositoryConformance,
    SnapshotDeserializationConformance,
    SnapshotStoreConformance,
    StreamReaderConformance,
)
from eventsource.testing.conformance_ports._fixtures import (  # noqa: E402
    make_conformance_registry,
)
from eventsource.testing.conformance_ports.dlq_tenant import DLQTenantScopeConformance  # noqa: E402
from eventsource.testing.conformance_ports.stateful import StoreStateMachine  # noqa: E402
from eventsource.testing.sync_facade import SyncStoreFacade  # noqa: E402


class _SQLiteEventStoreFixture:
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        yield store
        await store.close()


class TestSQLiteAppender(_SQLiteEventStoreFixture, AppenderConformance):
    pass


class TestSQLiteStreamReader(_SQLiteEventStoreFixture, StreamReaderConformance):
    pass


class TestSQLiteEventLookup(_SQLiteEventStoreFixture, EventLookupConformance):
    pass


class TestSQLiteGlobalFeed(_SQLiteEventStoreFixture, GlobalFeedConformance):
    pass


class TestSQLiteCategoryQuery(_SQLiteEventStoreFixture, CategoryQueryConformance):
    pass


class TestSQLiteSnapshotStore(SnapshotStoreConformance, SnapshotDeserializationConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteSnapshotStore]:
        store = SQLiteSnapshotStore(":memory:")
        yield store
        await store.close()

    async def _write_raw_state(
        self, store: SQLiteSnapshotStore, *, aggregate_id: UUID, aggregate_type: str, raw_state: str
    ) -> None:  # type: ignore[override]
        conn = await store._conn()
        async with store._lock:
            sql = (
                "INSERT INTO snapshots (aggregate_id, aggregate_type, version, "
                "schema_version, state, created_at) VALUES (?, ?, ?, ?, ?, ?)"
            )
            now = datetime.now(UTC).isoformat()
            await conn.execute(sql, (str(aggregate_id), aggregate_type, 1, 1, raw_state, now))
            await conn.commit()


class TestSQLiteCheckpointRepository(CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(self, tmp_path) -> AsyncIterator[SQLCheckpointRepository]:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/checkpoint_repo.db")
        async with engine.begin() as conn:
            raw = (await conn.get_raw_connection()).driver_connection
            for tbl in ("checkpoints", "events"):
                await raw.executescript(get_schema(tbl, backend="sqlite"))
        yield SQLCheckpointRepository(engine)
        await engine.dispose()

    async def write_legacy_int_row(self, store, subscription_id: str, value: int) -> None:
        sql = (
            "INSERT INTO projection_checkpoints "
            "(projection_name, global_position, events_processed) VALUES (:name, :value, 1)"
        )
        async with store._conn.begin() as conn:
            await conn.execute(text(sql), {"name": subscription_id, "value": value})


class TestSQLiteDLQRepository(DLQRepositoryConformance, DLQTenantScopeConformance):
    @pytest.fixture
    async def store(self, tmp_path) -> AsyncIterator[SQLDLQRepository]:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq_repo.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))
        yield SQLDLQRepository(engine)
        await engine.dispose()


class TestSQLiteOutboxRepository(OutboxRepositoryConformance):
    @pytest.fixture
    async def store(self, tmp_path) -> AsyncIterator[SQLiteOutboxRepository]:
        async with aiosqlite.connect(f"{tmp_path}/outbox_repo.db") as conn:
            await conn.executescript(get_schema("outbox", backend="sqlite"))
            await conn.commit()
            yield SQLiteOutboxRepository(conn)

    async def test_sqlite_cleanup_published_removes_entries_past_the_cutoff(self, store) -> None:
        from eventsource.testing.conformance_ports._fixtures import make_event

        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)
        sql = "UPDATE event_outbox SET published_at = datetime('now', '-10 days') WHERE id = ?"
        await store._connection.execute(sql, (str(outbox_id),))
        await store._connection.commit()
        assert await store.cleanup_published(days=7) == 1


class SQLiteStateMachine(StoreStateMachine):
    def make_store(self) -> SyncStoreFacade:
        return SyncStoreFacade(
            SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        )


TestSQLiteStateful = SQLiteStateMachine.TestCase
TestSQLiteStateful.settings = settings(max_examples=10, deadline=None, derandomize=True)
