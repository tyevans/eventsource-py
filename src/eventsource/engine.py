"""
Shared async engine factory.

All SQLAlchemy engines used by this library -- and by its tests -- must come
from here rather than from ``sqlalchemy.ext.asyncio.create_async_engine``
directly. The factory applies the SQLite transaction control and PRAGMA setup
that the library's transactional guarantees depend on.

Why this is not optional for SQLite:

The stdlib ``sqlite3`` driver's legacy transaction control does not emit BEGIN
before a SELECT, so reads are not repeatable inside what the application
believes is a transaction, and it may commit implicitly at points the caller
did not choose. Projections rely on a read-then-write-then-commit sequence
being atomic, so the driver must be put under explicit transaction control.

See https://docs.sqlalchemy.org/en/20/dialects/sqlite.html
"""

import logging
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine as _sa_create_async_engine
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)

# Applied to every SQLite connection as it is checked out of the pool.
SQLITE_PRAGMAS: dict[str, str | int] = {
    "foreign_keys": "ON",
    "journal_mode": "WAL",
    "busy_timeout": 5000,
}


def _configure_sqlite(engine: AsyncEngine, *, is_memory: bool) -> None:
    """Attach PRAGMA and transaction-control hooks to a SQLite engine.

    This is SQLAlchemy's documented "driver-level autocommit" recipe for the
    pysqlite/aiosqlite drivers: disable the driver's own implicit-BEGIN
    behavior by setting ``isolation_level = None`` on connect, then have
    SQLAlchemy emit ``BEGIN`` explicitly on every transaction start. It works
    uniformly across supported Python versions -- the alternative,
    driver-level ``autocommit=True`` passed via ``connect_args``, is
    unreliable because the pysqlite/aiosqlite dialect resets
    ``isolation_level`` itself on connect regardless of that setting.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _record: Any) -> None:
        # Take explicit control of transactions: no implicit BEGIN before
        # DML/SELECT, and no implicit COMMIT. See _emit_begin below.
        dbapi_connection.isolation_level = None
        cursor = dbapi_connection.cursor()
        try:
            for pragma, value in SQLITE_PRAGMAS.items():
                # :memory: databases cannot use WAL; skip it rather than error.
                if is_memory and pragma == "journal_mode":
                    continue
                cursor.execute(f"PRAGMA {pragma} = {value}")
        finally:
            cursor.close()

    @event.listens_for(engine.sync_engine, "begin")
    def _emit_begin(conn: Any) -> None:
        conn.exec_driver_sql("BEGIN")


def create_async_engine(url: str, **kwargs: Any) -> AsyncEngine:
    """
    Create an AsyncEngine configured for this library's guarantees.

    For SQLite URLs this enables explicit transaction control, WAL mode,
    foreign keys, and a busy timeout, and uses StaticPool for ``:memory:``
    databases so that every checkout sees the same database.

    For all other dialects this is a thin passthrough to SQLAlchemy.

    Args:
        url: SQLAlchemy database URL.
        **kwargs: Passed through to ``create_async_engine``. Caller-supplied
                  ``connect_args`` are merged with, and take precedence over,
                  the SQLite defaults.

    Returns:
        A configured AsyncEngine.
    """
    is_sqlite = url.startswith("sqlite")
    if not is_sqlite:
        return _sa_create_async_engine(url, **kwargs)

    is_memory = ":memory:" in url
    connect_args: dict[str, Any] = dict(kwargs.pop("connect_args", {}))

    if is_memory:
        kwargs.setdefault("poolclass", StaticPool)

    engine = _sa_create_async_engine(url, connect_args=connect_args, **kwargs)
    _configure_sqlite(engine, is_memory=is_memory)
    logger.debug("Created SQLite engine (memory=%s)", is_memory)
    return engine


__all__ = ["SQLITE_PRAGMAS", "create_async_engine"]
