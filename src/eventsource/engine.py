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

    Transaction control is entirely the ``begin`` listener below: it issues
    ``BEGIN`` explicitly at the start of every SQLAlchemy transaction (both
    explicit ``Connection.begin()`` and SQLAlchemy's autobegin-on-first-
    statement), before any application statement runs. That is what makes a
    SELECT (not just a DML statement) the point at which the transaction --
    and therefore the read snapshot -- opens; see
    ``test_sqlite_engine_holds_read_write_in_one_transaction``.
    """

    def _driver_is_autocommit(conn: Any) -> bool:
        """Is the underlying sqlite3 connection in autocommit mode?

        SQLAlchemy implements ``isolation_level="AUTOCOMMIT"`` for SQLite by
        setting the driver connection's ``isolation_level`` attribute to
        ``None`` (``SQLiteDialect_pysqlite.set_isolation_level``), and ``""``
        for every other level. That attribute is therefore the ground truth
        for whether the driver will open transactions on its own, and it is
        the same regardless of *how* AUTOCOMMIT was requested -- as a
        ``create_async_engine(isolation_level=...)`` argument, via
        ``Engine.execution_options()``, or via ``Connection
        .execution_options()``. Inspecting the execution options instead
        would miss the engine-argument route, which does not surface there.
        """
        return conn.connection.dbapi_connection.isolation_level is None

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _record: Any) -> None:
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
        # A connection asking for AUTOCOMMIT must stay in autocommit: this
        # event still fires for it even though SQLAlchemy itself emits no
        # transaction. Issuing BEGIN anyway would wrap the caller's
        # statements in a real transaction that nothing ever commits,
        # silently discarding writes when the connection closes.
        if _driver_is_autocommit(conn):
            return
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
        **kwargs: Passed through to ``create_async_engine`` unchanged. For
                  SQLite URLs, ``poolclass`` defaults to ``StaticPool`` for
                  ``:memory:`` databases unless the caller overrides it.

    Returns:
        A configured AsyncEngine.
    """
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
