"""PostgreSQL adapter implementing the five store ports.

Targets the `eventsource.ports.store` protocols: rows map to
`EventEnvelope` / `AppendResult` / `Position` value objects, and append
dispatches on `ExpectedVersion.kind` rather than integer sentinels.

Positions are minted from the `events.global_position` BIGSERIAL column via
`IntPositionCodec`.

Safe-horizon global feed: unlike SQLite (a single serialized writer),
PostgreSQL commits can become visible out of order under concurrent
transactions -- a `global_position` allocated first is not guaranteed to
commit first. `read_all` and `current_position` both apply the horizon
predicate documented on `_HORIZON_PREDICATE` in `store_read.py`, filtering on the
`events.txid` column against a per-read horizon, to avoid skipping a
lower position that is still in flight. Operators must have applied
`adapters/sql/schemas/updates/004_add_events_txid.sql` before upgrading -- the
predicate fails loudly with an undefined-column error otherwise.
"""

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
    """PostgreSQL implementation of `FullEventStore`.

    Uses async SQLAlchemy with the asyncpg driver.

    Schema ownership: like the legacy `stores/postgresql.py`, this adapter
    does NOT create the `events` table by default -- production deployments
    apply the canonical `adapters/sql/schemas/schemas/events.sql` (via `adapters/sql/schemas/`
    tooling) out of band, and this store simply queries an existing table.
    Pass `create_schema=True` (tests, local dev only) to opt into lazy
    `CREATE TABLE IF NOT EXISTS` schema creation on first use, guarded by an
    `asyncio.Lock`, using the same canonical schema. Leaving it `False` in
    production also avoids concurrent `CREATE INDEX IF NOT EXISTS` racing
    across processes ("tuple concurrently updated").

    Engine ownership: the `AsyncEngine` is always caller-supplied. This
    store does not own it by default (`owns_engine=False`), so `close()`
    is a no-op -- disposing a pool the caller still holds would be a
    surprising side effect, especially for a caller sharing one engine
    across several stores/consumers. Pass `owns_engine=True` if this store
    should dispose the engine when `close()` is called (see `close()` for
    details). Satisfies `eventsource.ports.lifecycle.SupportsClose`
    structurally.

    Structural conformance only -- no inheritance from the port protocols.
    """

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
        # Defaults to the database name alone, which collides for same-named
        # databases on different servers. Pass store_id explicitly when two
        # such stores could meet -- see Position's docstring for why the
        # default is not made host-specific.
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
        """Whether `append` also writes to `event_outbox` in the same transaction.

        When `True`, the outbox row and the event row commit (or roll back)
        together -- the entire point of the transactional outbox pattern.
        The outbox *reader* implements `eventsource.ports.outbox.OutboxRepository`.
        """
        return self._outbox_enabled

    async def _ensure_schema(self) -> None:
        """Lazily create the `events` table, only when `create_schema=True`.

        No-op otherwise (the default): production deployments manage schema
        via `adapters/sql/schemas/`, and queries against a missing table fail
        naturally.

        Runs the canonical `adapters/sql/schemas/schemas/events.sql` (the same file
        `get_schema("events")` serves to Alembic/manual setup) as a single
        script via the raw asyncpg driver connection. SQLAlchemy's
        `Connection.execute()` cannot run a multi-statement script through
        asyncpg (it uses the extended query protocol, which asyncpg
        rejects for multiple commands); asyncpg's own `Connection.execute()`
        uses the simple query protocol when no arguments are bound, which
        does support multi-statement scripts, so the raw driver connection
        is used here instead of `text()` execution. `events.sql` contains
        only DDL and `COMMENT ON` statements (no functions/dollar-quoting),
        which is exactly what the simple query protocol supports.
        """
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
                # First contact with the database. A bad DSN, an unreachable
                # host, or refused credentials surface here as a SQLAlchemy
                # driver error naming neither the library nor this store.
                raise EventStoreConnectionError(
                    f"could not connect to PostgreSQL to create the schema: {e}",
                    store=type(self).__name__,
                ) from e
            self._schema_ready = True

    async def close(self) -> None:
        """Dispose the underlying engine, but only if this store owns it.

        The engine is always caller-supplied to the constructor; by default
        this store does not own it and `close()` is a safe no-op, since
        disposing a pool the caller still holds (and may share with other
        stores/consumers) would tear it out from under them. Pass
        `owns_engine=True` at construction to opt in -- for example, a
        caller that constructs the engine solely for this store and wants
        `close()` to release it. Idempotent either way.
        """
        if self._owns_engine:
            await self._engine.dispose()


__all__ = [
    "ASYNCPG_AVAILABLE",
    "PostgreSQLEventStore",
    "PostgreSQLEventStoreAppendMixin",
    "PostgreSQLEventStoreReadMixin",
    "_HORIZON_PREDICATE",
    "_HORIZON_QUERY",
    "_SELECT_COLUMNS",
]
