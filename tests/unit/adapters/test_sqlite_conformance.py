"""Conformance tests for SQLiteEventStore against the port suites."""

import time
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
from eventsource.ports import ExpectedVersion  # noqa: E402
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
    ConformanceEvent,
    make_conformance_registry,
    make_stream,
)
from eventsource.testing.conformance_ports.stateful import StoreStateMachine  # noqa: E402
from eventsource.testing.sync_facade import SyncStoreFacade  # noqa: E402


class TestSQLiteAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        yield store
        await store.close()


class TestSQLiteStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        yield store
        await store.close()


class TestSQLiteEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        yield store
        await store.close()


class TestSQLiteGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        yield store
        await store.close()


class TestSQLiteCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteEventStore]:
        store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        yield store
        await store.close()


class TestSQLiteSnapshotStore(SnapshotStoreConformance, SnapshotDeserializationConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteSnapshotStore]:
        # ":memory:" works because the store keeps one connection open for
        # its lifetime; before it did, every operation opened its own
        # connection and saw a different, empty database.
        store = SQLiteSnapshotStore(":memory:")
        yield store
        await store.close()

    async def _write_raw_state(
        self,
        store: SQLiteSnapshotStore,  # type: ignore[override]
        *,
        aggregate_id: UUID,
        aggregate_type: str,
        raw_state: str,
    ) -> None:
        # Writes the same columns save_snapshot() does, except `state` is
        # the literal `raw_state` string instead of `json.dumps(...)` --
        # exercising get_snapshot()'s deserialization on genuinely
        # malformed content, which save_snapshot() itself can never produce.
        conn = await store._conn()
        async with store._lock:
            await conn.execute(
                """
                INSERT INTO snapshots
                    (aggregate_id, aggregate_type, version, schema_version, state, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(aggregate_id),
                    aggregate_type,
                    1,
                    1,
                    raw_state,
                    datetime.now(UTC).isoformat(),
                ),
            )
            await conn.commit()


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
        return SyncStoreFacade(
            SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
        )


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

    store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
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
    store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
    facade = SyncStoreFacade(store)
    stream = make_stream()
    facade.append(
        stream,
        [ConformanceEvent(aggregate_id=stream.aggregate_id)],
        ExpectedVersion.any_(),
    )
    del stream

    # aiosqlite.Connection *is* a Thread, so the store's own worker can be
    # identified rather than inferred from a global count. An earlier version
    # asserted `threading.active_count() == before - 1`, which reads as
    # precision and is not: the count is process-wide, so any other test's
    # connection thread exiting during this window makes the delta larger than
    # one and fails a passing implementation. That is exactly what happened
    # when the suite ran in a single process rather than under xdist.
    worker = store._connection
    assert worker is not None, "the append above should have opened the connection"
    assert worker.is_alive()

    facade.close()

    deadline = time.monotonic() + 2.0
    while worker.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)

    assert not worker.is_alive(), "closing the facade must release the store's aiosqlite thread"
    assert store._connection is None


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

    store1 = SQLiteEventStore(db_path, event_registry=make_conformance_registry())
    conn1 = await store1._conn()
    async with conn1.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
        columns1 = {row[1] for row in await cursor.fetchall()}
    assert "position_token" in columns1
    await store1.close()

    store2 = SQLiteEventStore(db_path, event_registry=make_conformance_registry())
    conn2 = await store2._conn()
    async with conn2.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
        columns2 = {row[1] for row in await cursor.fetchall()}
    assert "position_token" in columns2
    await store2.close()
