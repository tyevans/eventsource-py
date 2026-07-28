"""Unit tests for the shared async engine factory."""

import pytest
from sqlalchemy import text

from eventsource.engine import create_async_engine


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
        count_before = (
            await conn_a.execute(text("SELECT COUNT(*) FROM t"))
        ).scalar_one()

        async with engine.begin() as conn_b:
            await conn_b.execute(text("INSERT INTO t (id) VALUES (1)"))

        count_after = (
            await conn_a.execute(text("SELECT COUNT(*) FROM t"))
        ).scalar_one()
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


async def test_sqlite_autocommit_write_persists_without_explicit_commit(tmp_path):
    """A connection using isolation_level="AUTOCOMMIT" must not lose writes.

    SQLAlchemy's own do_begin() is a no-op for AUTOCOMMIT connections, but
    our "begin" listener still fires. If it unconditionally issues BEGIN, the
    statement below runs inside a real transaction that nothing ever
    commits, and closing the connection silently rolls it back.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))

    autocommit_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    async with autocommit_engine.connect() as conn:
        await conn.execute(text("INSERT INTO t (id) VALUES (99)"))
        # No explicit commit -- AUTOCOMMIT means the write should already be
        # durable as soon as the statement executes.

    async with engine.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()
    assert count == 1, (
        "AUTOCOMMIT write was lost: the begin listener wrapped it in a "
        "transaction that was never committed"
    )
    await engine.dispose()
