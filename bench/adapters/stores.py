"""EventStore adapters. SQL backends are added by later tasks."""

import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from bench.adapters._postgres import asyncpg_dsn, ensure_schema, ping, postgres_url, truncate
from bench.adapters.base import BenchAdapter
from bench.core.domain import make_registry
from eventsource import InMemoryEventStore
from eventsource.stores.interface import EventStore


class MemoryStoreAdapter(BenchAdapter[EventStore]):
    name = "memory"

    async def create(self) -> EventStore:
        return InMemoryEventStore(enable_tracing=False)


class PostgresStoreAdapter(BenchAdapter[EventStore]):
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
        self._engine = create_async_engine(self._url, echo=False, pool_size=10, max_overflow=20)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    async def teardown(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    async def create(self) -> EventStore:
        from eventsource import PostgreSQLEventStore

        await truncate(asyncpg_dsn(self._url))
        return PostgreSQLEventStore(
            self._session_factory,
            event_registry=make_registry(),
            outbox_enabled=False,
            enable_tracing=False,
        )


class SQLiteStoreAdapter(BenchAdapter[EventStore]):
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
        self._tmpdir = tempfile.TemporaryDirectory(prefix="bench-sqlite-")

    async def teardown(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()

    async def create(self) -> EventStore:
        from eventsource.stores.sqlite import SQLiteEventStore

        assert self._tmpdir is not None
        database = str(Path(self._tmpdir.name) / f"{uuid4().hex}.db")
        store = SQLiteEventStore(
            database, event_registry=make_registry(), wal_mode=True, enable_tracing=False
        )
        await store.initialize()
        return store

    async def destroy(self, resource: EventStore) -> None:
        close = getattr(resource, "close", None)
        if close is not None:
            await close()


STORE_ADAPTERS: dict[str, type[BenchAdapter[EventStore]]] = {
    MemoryStoreAdapter.name: MemoryStoreAdapter,
    PostgresStoreAdapter.name: PostgresStoreAdapter,
    SQLiteStoreAdapter.name: SQLiteStoreAdapter,
}
