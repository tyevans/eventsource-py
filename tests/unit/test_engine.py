"""Unit tests for the shared async engine factory."""

from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine as sa_create_async_engine

from eventsource import create_async_engine


def _factory_engine(path: str, **kwargs: Any) -> AsyncEngine:
    """Our engine, addressed by filesystem path."""
    return create_async_engine(f"sqlite+aiosqlite:///{path}", **kwargs)


def _stock_async_engine(path: str, **kwargs: Any) -> AsyncEngine:
    """An unconfigured SQLAlchemy engine, used as a behavioral baseline."""
    return sa_create_async_engine(f"sqlite+aiosqlite:///{path}", **kwargs)


async def test_sqlite_engine_holds_read_write_in_one_transaction(tmp_path):
    """A connection's transaction must begin at its first SELECT, not later.

    Two separate connections on a file-backed (WAL) database. Connection A
    starts a transaction and reads. Connection B then inserts a row and
    commits. Connection A reads again -- it must still see the count from
    before B's commit, because A's snapshot was fixed when its transaction
    began at the first SELECT.

    Under sqlite3's legacy isolation this fails: no BEGIN is emitted before
    the SELECT, so connection A has no open transaction yet when it runs its
    first read, and its second read observes B's commit.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))

    conn_a = await engine.connect()
    try:
        await conn_a.begin()
        count_before = (await conn_a.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()

        async with engine.begin() as conn_b:
            await conn_b.execute(text("INSERT INTO t (id) VALUES (1)"))

        count_after = (await conn_a.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()
        await conn_a.rollback()
    finally:
        await conn_a.close()

    assert count_before == 0
    assert count_after == count_before, (
        "connection A observed connection B's commit: A's transaction did "
        "not begin at its first SELECT"
    )
    await engine.dispose()


async def test_sqlite_engine_applies_pragmas(tmp_path):
    """WAL mode, foreign keys, and busy_timeout must be on for every pooled
    connection to a file-backed database.

    ``busy_timeout`` is asserted against the literal ``5000``, not against
    ``SQLITE_PRAGMAS["busy_timeout"]`` -- the latter is tautological (it
    reads the same module constant the code under test also reads, so it
    can't tell "applied correctly" from "applied some other, wrong value"
    apart; a mutation testing run against this file found exactly that: the
    literal ``5000`` mutated to ``5001`` with all tests still green). This
    also pins the documented default from the module docstring, so a
    silent change to it would be caught here rather than only in behavior.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.connect() as conn:
        journal = (await conn.execute(text("PRAGMA journal_mode"))).scalar_one()
        fk = (await conn.execute(text("PRAGMA foreign_keys"))).scalar_one()
        busy_timeout = (await conn.execute(text("PRAGMA busy_timeout"))).scalar_one()
    assert journal.lower() == "wal"
    assert fk == 1
    assert busy_timeout == 5000
    await engine.dispose()


async def test_memory_sqlite_applies_pragmas_except_journal_mode():
    """:memory: databases must get every pragma except journal_mode -- WAL
    is skipped there deliberately (see ``_apply_pragmas``), but that must
    not skip the *rest* of the loop too.

    ``busy_timeout`` is asserted against the literal ``5000`` for the same
    tautology reason as ``test_sqlite_engine_applies_pragmas`` above --
    comparing against ``SQLITE_PRAGMAS["busy_timeout"]`` can never fail.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.connect() as conn:
        journal = (await conn.execute(text("PRAGMA journal_mode"))).scalar_one()
        fk = (await conn.execute(text("PRAGMA foreign_keys"))).scalar_one()
        busy_timeout = (await conn.execute(text("PRAGMA busy_timeout"))).scalar_one()
    assert journal.lower() != "wal", ":memory: databases cannot use WAL"
    assert fk == 1
    assert busy_timeout == 5000
    await engine.dispose()


async def test_memory_sqlite_applies_busy_timeout_after_journal_mode_skip(monkeypatch):
    """``busy_timeout`` must still be applied after the ``journal_mode``
    pragma is skipped for :memory: -- i.e. the skip is a ``continue``, not a
    ``break`` that also drops every pragma after it in iteration order.

    ``busy_timeout`` defaults to 5000 (SQLite's own compiled default) as it
    happens, so asserting the pragma equals ``SQLITE_PRAGMAS["busy_timeout"]``
    (also 5000) can't tell "applied" from "never touched" apart. Monkeypatch
    a value that could never be a coincidental default to make the
    assertion actually discriminate.
    """
    import eventsource.adapters._sql.engine as engine_module

    monkeypatch.setitem(engine_module.SQLITE_PRAGMAS, "busy_timeout", 4321)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.connect() as conn:
        busy_timeout = (await conn.execute(text("PRAGMA busy_timeout"))).scalar_one()
    assert busy_timeout == 4321
    await engine.dispose()


async def test_memory_sqlite_shares_one_connection():
    """:memory: must use StaticPool or each pooled connection gets its own DB."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
    async with engine.connect() as conn:
        # Would raise OperationalError: no such table, on a fresh connection.
        await conn.execute(text("SELECT COUNT(*) FROM t"))
    await engine.dispose()


def test_postgres_url_passes_through_without_sqlite_config():
    """Non-SQLite URLs must not get SQLite connect_args."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    assert engine.dialect.name == "postgresql"


def test_postgres_url_forwards_kwargs():
    """Non-SQLite URLs must still forward **kwargs to SQLAlchemy, not just
    the bare url -- the passthrough branch must not drop them."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db", echo=True)
    assert engine.echo is True


async def test_sqlite_engine_rolls_back_a_transaction(tmp_path):
    """An explicit rollback must discard the writes made inside it."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))

    conn = await engine.connect()
    try:
        await conn.begin()
        await conn.execute(text("INSERT INTO t (id) VALUES (5)"))
        await conn.rollback()
    finally:
        await conn.close()

    async with engine.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()
    assert count == 0, "INSERT survived rollback"
    await engine.dispose()


async def _autocommit_write_survives(factory, url: str, route: str) -> int:
    """Write one row through an AUTOCOMMIT connection, no explicit commit.

    ``route`` selects which of SQLAlchemy's three documented ways of asking
    for AUTOCOMMIT is used. Returns the row count observed afterwards by a
    fresh engine.
    """
    engine = factory(url)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
    await engine.dispose()

    if route == "engine_argument":
        writer = factory(url, isolation_level="AUTOCOMMIT")
        async with writer.connect() as conn:
            await conn.execute(text("INSERT INTO t (id) VALUES (99)"))
    elif route == "engine_execution_options":
        writer = factory(url).execution_options(isolation_level="AUTOCOMMIT")
        async with writer.connect() as conn:
            await conn.execute(text("INSERT INTO t (id) VALUES (99)"))
    elif route == "connection_execution_options":
        writer = factory(url)
        async with writer.connect() as conn:
            autocommit_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await autocommit_conn.execute(text("INSERT INTO t (id) VALUES (99)"))
    else:  # pragma: no cover - guards against a typo in the parametrization
        raise AssertionError(f"unknown route {route!r}")
    await writer.dispose()

    reader = factory(url)
    async with reader.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()
    await reader.dispose()
    return count


@pytest.mark.parametrize(
    "route",
    ["engine_argument", "engine_execution_options", "connection_execution_options"],
)
async def test_sqlite_autocommit_write_persists_without_explicit_commit(tmp_path, route):
    """AUTOCOMMIT writes must persist, however AUTOCOMMIT was requested.

    Our "begin" listener fires even for AUTOCOMMIT connections, where
    SQLAlchemy itself emits no transaction. If it issued BEGIN anyway, the
    write below would run inside a real transaction that nothing ever
    commits, and closing the connection would silently roll it back.

    Each route sets AUTOCOMMIT up differently and must be checked
    separately: notably, an engine-level ``isolation_level`` argument does
    not appear in ``Connection.get_execution_options()``.

    The result is compared against a stock SQLAlchemy engine, so this asserts
    that our factory does not differ from SQLAlchemy on persistence rather
    than just asserting a hardcoded count.
    """
    stock_count = await _autocommit_write_survives(
        _stock_async_engine, f"{tmp_path}/stock-{route}.db", route
    )
    assert stock_count == 1, "baseline assumption broken: stock engine lost the write"

    count = await _autocommit_write_survives(_factory_engine, f"{tmp_path}/ours-{route}.db", route)
    assert count == stock_count, (
        f"AUTOCOMMIT write was lost via {route}: the begin listener wrapped "
        f"it in a transaction that was never committed"
    )


def test_module_lives_under_sql_adapters():
    """`create_async_engine` is adapter-ring content (ADR 0029).

    It constructs a SQLAlchemy AsyncEngine and registers dialect-specific
    driver listeners, so it belongs under `adapters/`. `_sql/` rather than
    `sql/` because `adapters/_sql/__init__.py` is import-free while
    `adapters/sql/__init__.py` eagerly imports three modules, one of which
    reaches into `application/projections/` -- the front-door import chain
    must not widen while relocating the module that chain names.
    """
    assert create_async_engine.__module__ == "eventsource.adapters._sql.engine"
