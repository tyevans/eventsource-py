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
    """A connection's transaction must begin at its first SELECT, not later."""
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
    """WAL mode, foreign keys, and busy_timeout (5000) must be applied on file-backed DBs."""
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
    """:memory: databases must get every pragma except journal_mode."""
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
    """busy_timeout must still be applied after journal_mode is skipped for :memory:."""
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
        await conn.execute(text("SELECT COUNT(*) FROM t"))
    await engine.dispose()


def test_postgres_url_passes_through_without_sqlite_config():
    """Non-SQLite URLs must not get SQLite connect_args."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    assert engine.dialect.name == "postgresql"


def test_postgres_url_forwards_kwargs():
    """Non-SQLite URLs must still forward **kwargs to SQLAlchemy."""
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
    """Write one row through an AUTOCOMMIT connection and return observed row count."""
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
    """AUTOCOMMIT writes must persist across all three SQLAlchemy AUTOCOMMIT routes."""
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
    """`create_async_engine` is adapter-ring content under `_sql/` (ADR 0029)."""
    assert create_async_engine.__module__ == "eventsource.adapters._sql.engine"
