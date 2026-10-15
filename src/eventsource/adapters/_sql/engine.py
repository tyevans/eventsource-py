"""Shared async engine factory applying SQLite transaction control and PRAGMAs (ADR 0029)."""

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


def _driver_is_autocommit(conn: Any) -> bool:
    """Return True if the underlying sqlite3 DBAPI connection is in autocommit mode."""
    return bool(conn.dialect.detect_autocommit_setting(conn.connection.dbapi_connection))


def _apply_pragmas(dbapi_connection: Any, *, is_memory: bool) -> None:
    """Apply ``SQLITE_PRAGMAS`` to a raw DBAPI connection."""
    cursor = dbapi_connection.cursor()
    try:
        for pragma, value in SQLITE_PRAGMAS.items():
            # :memory: databases cannot use WAL; skip it rather than error.
            if is_memory and pragma == "journal_mode":
                continue
            cursor.execute(f"PRAGMA {pragma} = {value}")
    finally:
        cursor.close()


def _begin_unless_autocommit(conn: Any) -> None:
    """Issue ``BEGIN`` on ``conn`` unless it is in AUTOCOMMIT mode."""
    if _driver_is_autocommit(conn):
        return
    conn.exec_driver_sql("BEGIN")


def _configure_sqlite(engine: AsyncEngine, *, is_memory: bool) -> None:
    """Attach PRAGMA and transaction-control hooks to a SQLite engine."""

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _record: Any) -> None:
        _apply_pragmas(dbapi_connection, is_memory=is_memory)

    @event.listens_for(engine.sync_engine, "begin")
    def _emit_begin(conn: Any) -> None:
        _begin_unless_autocommit(conn)


def create_async_engine(url: str, **kwargs: Any) -> AsyncEngine:
    """Create an AsyncEngine configured for this library's guarantees."""
    is_sqlite = url.startswith("sqlite")
    if not is_sqlite:
        return _sa_create_async_engine(url, **kwargs)

    is_memory = ":memory:" in url
    if is_memory:
        kwargs.setdefault("poolclass", StaticPool)

    engine = _sa_create_async_engine(url, **kwargs)
    _configure_sqlite(engine, is_memory=is_memory)
    logger.debug("Created SQLite engine (memory=%s)", is_memory)
    return engine


__all__ = ["SQLITE_PRAGMAS", "create_async_engine"]
