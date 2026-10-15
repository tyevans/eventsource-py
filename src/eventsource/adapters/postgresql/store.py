"""PostgreSQL adapter implementing the five store ports."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.postgresql.store_append import PostgreSQLEventStoreAppendMixin
from eventsource.adapters.postgresql.store_read import (
    _HORIZON_PREDICATE,
    _HORIZON_QUERY,
    _SELECT_COLUMNS,
    PostgreSQLEventStoreReadMixin,
)
from eventsource.adapters.sql.schemas import get_schema
from eventsource.domain.event_registry import EventRegistry, default_registry
from eventsource.ports import EventStoreConnectionError

try:
    import asyncpg  # noqa: F401

    ASYNCPG_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    ASYNCPG_AVAILABLE = False

logger = logging.getLogger(__name__)


class PostgreSQLEventStore(
    PostgreSQLEventStoreAppendMixin,
    PostgreSQLEventStoreReadMixin,
):
    """PostgreSQL implementation of `FullEventStore` using async SQLAlchemy and asyncpg."""

    def __init__(
        self,
        engine: AsyncEngine,
        event_registry: EventRegistry | None = None,
        *,
        store_id: str | None = None,
        create_schema: bool = False,
        outbox_enabled: bool = False,
        owns_engine: bool = False,
    ) -> None:
        if not ASYNCPG_AVAILABLE:
            raise ImportError(
                "asyncpg is required for PostgreSQLEventStore. "
                "Install with: pip install eventsource[postgresql]"
            )
        self._engine = engine
        self._event_registry = event_registry or default_registry
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        database = engine.url.database or "postgres"
        self._store_id = store_id or f"pg:{database}"
        self._codec = IntPositionCodec(self._store_id)
        self._create_schema = create_schema
        self._schema_ready = False
        self._owns_engine = owns_engine
        self._schema_lock = asyncio.Lock()
        self._outbox_enabled = outbox_enabled

    @property
    def store_id(self) -> str:
        return self._store_id

    @property
    def outbox_enabled(self) -> bool:
        """Whether `append` also writes to `event_outbox` in the same transaction."""
        return self._outbox_enabled

    async def _ensure_schema(self) -> None:
        """Lazily create the `events` table when `create_schema=True`."""
        if not self._create_schema or self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            try:
                async with self._engine.connect() as conn:
                    raw = await conn.get_raw_connection()
                    driver_connection = raw.driver_connection
                    assert driver_connection is not None
                    await driver_connection.execute(get_schema("events"))
                    await conn.commit()
            except OperationalError as e:
                raise EventStoreConnectionError(
                    f"could not connect to PostgreSQL to create the schema: {e}",
                    store=type(self).__name__,
                ) from e
            self._schema_ready = True

    async def close(self) -> None:
        """Dispose the underlying engine if this store owns it (`owns_engine=True`)."""
        if self._owns_engine:
            await self._engine.dispose()

    async def __aenter__(self) -> PostgreSQLEventStore:
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        await self.close()


__all__ = [
    "ASYNCPG_AVAILABLE",
    "PostgreSQLEventStore",
    "PostgreSQLEventStoreAppendMixin",
    "PostgreSQLEventStoreReadMixin",
    "_HORIZON_PREDICATE",
    "_HORIZON_QUERY",
    "_SELECT_COLUMNS",
]
