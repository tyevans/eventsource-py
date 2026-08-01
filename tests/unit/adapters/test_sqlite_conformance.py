"""Conformance tests for SQLiteEventStore against the port suites."""

import tempfile
import threading
import time
from collections.abc import AsyncIterator
from uuid import uuid4

import aiosqlite
import pytest
from hypothesis import settings
from sqlalchemy import text

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
    from eventsource.domain.event_registry import EventRegistry
    from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


from eventsource import create_async_engine  # noqa: E402
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository  # noqa: E402
from eventsource.adapters.sql.dlq import SQLDLQRepository  # noqa: E402
from eventsource.adapters.sql.schemas import get_schema  # noqa: E402
from eventsource.adapters.sqlite import SQLiteEventStore, SQLiteSnapshotStore  # noqa: E402
from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository  # noqa: E402
from eventsource.ports import ExpectedVersion  # noqa: E402
from eventsource.testing.conformance_ports import (  # noqa: E402
    AppenderConformance,
    CategoryQueryConformance,
    CheckpointRepositoryConformance,
    DLQRepositoryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    OutboxRepositoryConformance,
    SnapshotStoreConformance,
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


class TestSQLiteSnapshotStore(SnapshotStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteSnapshotStore]:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/snapshots.db"
            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("snapshots", "sqlite")
                await conn.executescript(schema)
                await conn.commit()
            yield SQLiteSnapshotStore(db_path)


class TestSQLiteCheckpointRepository(CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(self, tmp_path) -> AsyncIterator[SQLCheckpointRepository]:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/checkpoint_repo.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
            await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))
        yield SQLCheckpointRepository(engine)
        await engine.dispose()

    async def write_legacy_int_row(
        self,
        store: SQLCheckpointRepository,
        subscription_id: str,
        value: int,
    ) -> None:
        # A row as written before position tokens existed: an integer
        # global_position and no token. The adapter must read it as None.
        async with store._conn.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO projection_checkpoints "
                    "(projection_name, global_position, events_processed) "
                    "VALUES (:name, :value, 1)"
                ),
                {"name": subscription_id, "value": value},
            )


class TestSQLiteDLQRepository(DLQRepositoryConformance):
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
        db_path = f"{tmp_path}/outbox_repo.db"
        async with aiosqlite.connect(db_path) as conn:
            await conn.executescript(get_schema("outbox", backend="sqlite"))
            await conn.commit()
            yield SQLiteOutboxRepository(conn)

    async def test_sqlite_cleanup_published_removes_entries_past_the_cutoff(
        self, store: SQLiteOutboxRepository
    ) -> None:
        from eventsource.testing.conformance_ports._fixtures import make_event

        outbox_id = await store.add_event(make_event(aggregate_id=uuid4()))
        await store.mark_published(outbox_id)
        await store._connection.execute(
            "UPDATE event_outbox SET published_at = datetime('now', '-10 days') WHERE id = ?",
            (str(outbox_id),),
        )
        await store._connection.commit()

        deleted = await store.cleanup_published(days=7)

        assert deleted == 1


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


async def test_append_rolls_back_on_non_integrity_error(monkeypatch) -> None:
    """A non-`IntegrityError` failure mid-batch must still roll back.

    Only `aiosqlite.IntegrityError` was rolled back previously; any other
    exception (e.g. a serialization failure) left a dirty open transaction
    on the shared connection, which the *next* `append()`'s `commit()`
    would silently fold in.
    """
    from eventsource.ports import CategoryReadOptions

    store = SQLiteEventStore(":memory:", event_registry=_make_registry())
    stream = make_stream()

    import eventsource.adapters.sqlite.store as sqlite_store_module

    original_json_dumps = sqlite_store_module.json_dumps
    call_count = 0

    def flaky_json_dumps(obj):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("boom: simulated mid-batch serialization failure")
        return original_json_dumps(obj)

    monkeypatch.setattr(sqlite_store_module, "json_dumps", flaky_json_dumps)

    with pytest.raises(RuntimeError, match="boom"):
        await store.append(
            stream,
            [
                ConformanceEvent(aggregate_id=stream.aggregate_id, payload="1"),
                ConformanceEvent(aggregate_id=stream.aggregate_id, payload="2"),
            ],
            ExpectedVersion.any_(),
        )

    monkeypatch.setattr(sqlite_store_module, "json_dumps", original_json_dumps)

    envelopes = [e async for e in store.read_category(stream.category, CategoryReadOptions())]
    assert envelopes == []

    # A subsequent append must succeed cleanly (no leftover dirty transaction).
    result = await store.append(
        stream,
        [ConformanceEvent(aggregate_id=stream.aggregate_id, payload="3")],
        ExpectedVersion.any_(),
    )
    assert result.new_version == 1

    await store.close()


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


async def test_reopening_same_file_backed_db_does_not_fail_on_additive_column(
    tmp_path,
) -> None:
    """Regression: a second process opening the same file must not raise on
    the additive `position_token` column that the first process already added.

    Schema is applied on every first connection (`_conn`), including to a
    file that already carries the column -- the naive `ALTER TABLE ADD
    COLUMN` (without the `PRAGMA table_info` guard) raises
    `sqlite3.OperationalError: duplicate column name` here.
    """
    db_path = str(tmp_path / "reopen.db")

    store1 = SQLiteEventStore(db_path, event_registry=_make_registry())
    conn1 = await store1._conn()
    async with conn1.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
        columns1 = {row[1] for row in await cursor.fetchall()}
    assert "position_token" in columns1
    await store1.close()

    store2 = SQLiteEventStore(db_path, event_registry=_make_registry())
    conn2 = await store2._conn()
    async with conn2.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
        columns2 = {row[1] for row in await cursor.fetchall()}
    assert "position_token" in columns2
    await store2.close()
