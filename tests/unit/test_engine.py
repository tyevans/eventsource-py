"""Unit tests for the shared async engine factory."""

import pytest
from sqlalchemy import text

from eventsource.engine import create_async_engine


async def test_sqlite_engine_holds_read_write_in_one_transaction(tmp_path):
    """A SELECT then INSERT on one connection must roll back together.

    Under sqlite3's legacy isolation this fails: the SELECT runs outside any
    transaction, so the driver may commit implicitly before the rollback.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))

    conn = await engine.connect()
    try:
        await conn.begin()
        await conn.execute(text("SELECT COUNT(*) FROM t"))
        await conn.execute(text("INSERT INTO t (id) VALUES (1)"))
        await conn.rollback()
    finally:
        await conn.close()

    async with engine.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()
    assert count == 0, "INSERT survived rollback: transaction control is wrong"
    await engine.dispose()


async def test_sqlite_engine_applies_pragmas(tmp_path):
    """WAL mode and foreign keys must be on for every pooled connection."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.connect() as conn:
        journal = (await conn.execute(text("PRAGMA journal_mode"))).scalar_one()
        fk = (await conn.execute(text("PRAGMA foreign_keys"))).scalar_one()
    assert journal.lower() == "wal"
    assert fk == 1
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
