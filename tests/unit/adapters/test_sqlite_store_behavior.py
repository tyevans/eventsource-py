"""SQLite store behavior tests that are not port conformance cases.

Split out of `test_sqlite_conformance.py` to keep both modules under the
test-file line ceiling.
"""

import threading
import time

import pytest

from eventsource import ExpectedVersion
from eventsource.adapters.sqlite import SQLiteEventStore
from eventsource.ports import CategoryReadOptions
from eventsource.testing.conformance_ports._fixtures import (
    ConformanceEvent,
    make_conformance_registry,
    make_stream,
)
from eventsource.testing.sync_facade import SyncStoreFacade
from tests.conftest import skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


async def test_append_rolls_back_on_non_integrity_error(monkeypatch) -> None:
    import eventsource.adapters.sqlite.store as sqlite_store_module

    store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
    stream, original_json_dumps, call_count = make_stream(), sqlite_store_module.json_dumps, 0

    def flaky_json_dumps(obj):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("boom: simulated mid-batch serialization failure")
        return original_json_dumps(obj)

    monkeypatch.setattr(sqlite_store_module, "json_dumps", flaky_json_dumps)
    events = [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=p) for p in ("1", "2")]
    with pytest.raises(RuntimeError, match="boom"):
        await store.append(stream, events, ExpectedVersion.any_())
    monkeypatch.setattr(sqlite_store_module, "json_dumps", original_json_dumps)
    assert [e async for e in store.read_category(stream.category, CategoryReadOptions())] == []
    ev3 = [ConformanceEvent(aggregate_id=stream.aggregate_id, payload="3")]
    assert (await store.append(stream, ev3, ExpectedVersion.any_())).new_version == 1
    await store.close()


def test_sync_facade_close_closes_underlying_store() -> None:
    before = threading.active_count()
    store = SQLiteEventStore(":memory:", event_registry=make_conformance_registry())
    facade, stream = SyncStoreFacade(store), make_stream()
    ev = [ConformanceEvent(aggregate_id=stream.aggregate_id)]
    facade.append(stream, ev, ExpectedVersion.any_())
    assert threading.active_count() > before
    facade.close()
    deadline, alive = time.monotonic() + 2.0, True
    while (alive := threading.active_count() > before) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not alive and store._connection is None


async def test_reopening_same_file_backed_db_does_not_fail_on_additive_column(tmp_path) -> None:
    db_path = str(tmp_path / "reopen.db")
    for _ in range(2):
        store = SQLiteEventStore(db_path, event_registry=make_conformance_registry())
        conn = await store._conn()
        async with conn.execute("PRAGMA table_info(projection_checkpoints)") as cur:
            assert "position_token" in {row[1] for row in await cur.fetchall()}
        await store.close()
