"""SnapshotStore adapters. SQL backends are added by later tasks."""

import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from bench.adapters._postgres import asyncpg_dsn, ensure_schema, ping, postgres_url, truncate
from bench.adapters.base import BenchAdapter
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.ports.snapshots import SnapshotStore


class MemorySnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "memory"

    async def create(self) -> SnapshotStore:
        return InMemorySnapshotStore(enable_tracing=False)


class PostgresSnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "postgresql"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or postgres_url()
        self._engine: Any = None
        self._session_factory: Any = None

    async def available(self) -> str | None:
        return await ping(asyncpg_dsn(self._url))

    async def setup(self) -> None:
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        await ensure_schema(asyncpg_dsn(self._url))
        self._engine = create_async_engine(self._url, echo=False, pool_size=10)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    async def teardown(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    async def create(self) -> SnapshotStore:
        from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore

        await truncate(asyncpg_dsn(self._url))
        return PostgreSQLSnapshotStore(self._session_factory, enable_tracing=False)


class SQLiteSnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "sqlite"

    def __init__(self) -> None:
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None

    async def available(self) -> str | None:
        try:
            import aiosqlite  # noqa: F401
        except ImportError:
            return "sqlite extra not installed (aiosqlite missing)"
        return None

    async def setup(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="bench-sqlite-snap-")

    async def teardown(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()

    async def create(self) -> SnapshotStore:
        import aiosqlite

        from eventsource.adapters.sqlite.snapshots import SQLiteSnapshotStore
        from eventsource.migrations import get_schema

        assert self._tmpdir is not None
        database = str(Path(self._tmpdir.name) / f"{uuid4().hex}.db")
        async with aiosqlite.connect(database) as conn:
            await conn.executescript(get_schema("snapshots", "sqlite"))
        return SQLiteSnapshotStore(database, enable_tracing=False)


SNAPSHOT_ADAPTERS: dict[str, type[BenchAdapter[SnapshotStore]]] = {
    MemorySnapshotAdapter.name: MemorySnapshotAdapter,
    PostgresSnapshotAdapter.name: PostgresSnapshotAdapter,
    SQLiteSnapshotAdapter.name: SQLiteSnapshotAdapter,
}
