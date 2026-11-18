"""SQLite snapshot store against a database created before the tenant_id column (ADR-0113)."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiosqlite
import pytest

from tests.conftest import skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]

import eventsource.adapters.sql.schemas as schemas  # noqa: E402
from eventsource.adapters.sqlite import SQLiteSnapshotStore  # noqa: E402
from eventsource.ports.snapshots import Snapshot  # noqa: E402

# The snapshots table as shipped before ADR-0113: no tenant_id column.
_LEGACY_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (aggregate_id, aggregate_type)
);
"""
_UPDATE_SCRIPT = Path(schemas.__file__).parent / "updates" / "005_add_snapshot_tenant_sqlite.sql"


async def _create_legacy_db(path: Path, aggregate_id) -> None:
    async with aiosqlite.connect(path) as conn:
        await conn.executescript(_LEGACY_SNAPSHOTS_DDL)
        await conn.execute(
            "INSERT INTO snapshots (aggregate_id, aggregate_type, version, schema_version, "
            "state, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                str(aggregate_id),
                "Order",
                3,
                1,
                '{"status": "open"}',
                datetime.now(UTC).isoformat(),
            ),
        )
        await conn.commit()


async def _assert_tenant_round_trip(db_path: str) -> None:
    aggregate_id, tenant_id = uuid4(), uuid4()
    store = SQLiteSnapshotStore(db_path)
    try:
        await store.save_snapshot(
            Snapshot(aggregate_id, "Order", 1, {"n": 1}, 1, datetime.now(UTC), tenant_id)
        )
        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded is not None
        assert loaded.tenant_id == tenant_id
        assert loaded.state == {"n": 1}
    finally:
        await store.close()


async def test_legacy_snapshot_db_reads_and_writes_after_upgrade(tmp_path) -> None:
    db_path = tmp_path / "legacy_snapshots.db"
    legacy_id = uuid4()
    await _create_legacy_db(db_path, legacy_id)

    store = SQLiteSnapshotStore(str(db_path))
    try:
        legacy = await store.get_snapshot(legacy_id, "Order")
        assert legacy is not None
        assert legacy.version == 3
        assert legacy.state == {"status": "open"}
        assert legacy.tenant_id is None

        tenant_id = uuid4()
        await store.save_snapshot(
            Snapshot(legacy_id, "Order", 4, {"status": "closed"}, 1, datetime.now(UTC), tenant_id)
        )
        upgraded = await store.get_snapshot(legacy_id, "Order")
        assert upgraded is not None
        assert upgraded.version == 4
        assert upgraded.tenant_id == tenant_id
    finally:
        await store.close()

    # A second open must not fail on the column that now exists.
    await _assert_tenant_round_trip(str(db_path))


async def test_update_script_then_store_open_is_safe_to_repeat(tmp_path) -> None:
    db_path = tmp_path / "scripted_snapshots.db"
    await _create_legacy_db(db_path, uuid4())

    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(_UPDATE_SCRIPT.read_text())
        await conn.commit()

    # The store's open-time guard sees the column the script added and skips it.
    await _assert_tenant_round_trip(str(db_path))
    await _assert_tenant_round_trip(str(db_path))
