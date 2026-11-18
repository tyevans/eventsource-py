"""Operator script updates/007 adds dead_letter_queue.tenant_id to an existing SQLite database.

The database is built from the unchanged SQLite DLQ template, as an existing
deployment would have it. The script runs against it, and a tenant-owned row
then reads back with its owner.
"""

import sqlite3
from uuid import uuid4

import pytest

from eventsource.adapters.sql.schemas import _PACKAGE_DIR, get_schema

_UPDATE = _PACKAGE_DIR / "updates" / "007_add_dlq_tenant_sqlite.sql"


def _pre_tenancy_database() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(get_schema("dlq", backend="sqlite", additive=False))
    return conn


def _insert(conn: sqlite3.Connection, event_id: str, tenant_id: str | None) -> None:
    conn.execute(
        "INSERT INTO dead_letter_queue "
        "(event_id, projection_name, event_type, event_data, error_message, tenant_id) "
        "VALUES (?, 'P', 'Created', '{}', 'boom', ?)",
        (event_id, tenant_id),
    )


def test_update_script_adds_the_column_and_a_tenant_row_reads_back() -> None:
    conn = _pre_tenancy_database()
    conn.executescript(_UPDATE.read_text())
    tenant, event = str(uuid4()), str(uuid4())

    _insert(conn, event, tenant)

    row = conn.execute(
        "SELECT tenant_id FROM dead_letter_queue WHERE event_id = ?", (event,)
    ).fetchone()
    assert row == (tenant,)


def test_rows_that_predate_the_column_read_back_with_null_tenant() -> None:
    conn = _pre_tenancy_database()
    old_event = str(uuid4())
    conn.execute(
        "INSERT INTO dead_letter_queue "
        "(event_id, projection_name, event_type, event_data, error_message) "
        "VALUES (?, 'P', 'Created', '{}', 'boom')",
        (old_event,),
    )

    conn.executescript(_UPDATE.read_text())

    row = conn.execute(
        "SELECT tenant_id FROM dead_letter_queue WHERE event_id = ?", (old_event,)
    ).fetchone()
    assert row == (None,)


def test_update_script_is_not_rerunnable() -> None:
    """Pins the documented limit: SQLite has no conditional ADD COLUMN."""
    conn = _pre_tenancy_database()
    conn.executescript(_UPDATE.read_text())

    with pytest.raises(sqlite3.OperationalError, match="duplicate column name"):
        conn.executescript(_UPDATE.read_text())
