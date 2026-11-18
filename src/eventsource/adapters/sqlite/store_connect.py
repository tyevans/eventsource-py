"""Open a SQLite connection for `SQLiteEventStore`: file mode, pragmas, and schema setup."""

from __future__ import annotations

import os

from eventsource import ports as p
from eventsource.adapters.sql.schemas import get_schema
from eventsource.adapters.sqlite import queries as q
from eventsource.observability.error_summary import error_summary

try:
    import aiosqlite
except ImportError:  # pragma: no cover - exercised only without the optional dep
    aiosqlite = None  # type: ignore[assignment]


def _create_database_file(database: str) -> None:
    """Create a missing database file with owner-only permissions (0600).

    An existing file keeps its mode. In-memory databases and shared-memory
    URIs have no file on disk, so they are skipped. `O_EXCL` makes a file
    that appears between the check and the create a no-op, not an error.
    """
    if database in ("", ":memory:") or (database.startswith("file:") and "mode=memory" in database):
        return
    try:
        fd = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return
    os.close(fd)


async def open_connection(
    database: str, store_name: str, wal_mode: bool, busy_timeout: int
) -> aiosqlite.Connection:
    """Open `database`, apply pragmas and the schema, and return the ready connection."""
    try:
        _create_database_file(database)
        conn = await aiosqlite.connect(database)
    except (aiosqlite.Error, OSError) as e:
        msg = f"could not open the SQLite database at {database!r}: {error_summary(e)}"
        raise p.EventStoreConnectionError(msg, store=store_name) from e
    w = ("journal_mode = WAL",) if wal_mode else ()
    for pragma in ("foreign_keys = ON", f"busy_timeout = {busy_timeout}", *w):
        await conn.execute(f"PRAGMA {pragma}")
    conn.row_factory = aiosqlite.Row
    await conn.executescript(get_schema("all", backend="sqlite", additive=False))
    await q.apply_additive_updates(conn)
    await conn.commit()
    return conn


__all__ = ["open_connection"]
