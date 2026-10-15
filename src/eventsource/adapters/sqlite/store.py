"""SQLite adapter implementing the five store ports (`FullEventStore`).

Positions are minted from `events.global_position` via `IntPositionCodec`.
SQLite writers are serialized on a single shared connection under `_lock`,
so no safe-horizon gap handling is required.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from eventsource.adapters._common import check_expected, describe_expected
from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.schemas import get_schema
from eventsource.adapters.sqlite.queries import (
    apply_additive_updates,
    build_read_all_query,
    build_read_category_query,
    build_read_stream_query,
    row_to_envelope,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, default_registry
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    EventStoreConnectionError,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    StreamReadOptions,
)
from eventsource.ports.outbox import outbox_event_data

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    AIOSQLITE_AVAILABLE = False
    aiosqlite = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SQLiteEventStore:
    """SQLite implementation of `FullEventStore` (single shared aiosqlite connection under `_lock`)."""

    def __init__(
        self,
        database: str,
        event_registry: EventRegistry | None = None,
        *,
        store_id: str | None = None,
        wal_mode: bool = True,
        busy_timeout: int = 5000,
        outbox_enabled: bool = False,
    ) -> None:
        if not AIOSQLITE_AVAILABLE:
            raise ImportError(
                "aiosqlite is required for SQLiteEventStore. "
                "Install with: pip install eventsource[sqlite]"
            )
        self._database = database
        self._event_registry = event_registry or default_registry
        self._wal_mode = wal_mode
        self._busy_timeout = busy_timeout
        self._outbox_enabled = outbox_enabled
        self._connection: aiosqlite.Connection | None = None
        self._store_id = store_id or f"sqlite:{database}"
        self._codec = IntPositionCodec(self._store_id)
        self._lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id

    @property
    def outbox_enabled(self) -> bool:
        """Whether `append` also writes to `event_outbox` in the same transaction."""
        return self._outbox_enabled

    async def _conn(self) -> aiosqlite.Connection:
        """Return the live connection, opening and initializing it on first use."""
        if self._connection is not None:
            return self._connection
        async with self._init_lock:
            if self._connection is not None:
                return self._connection
            try:
                conn = await aiosqlite.connect(self._database)
            except (aiosqlite.Error, OSError) as e:
                raise EventStoreConnectionError(
                    f"could not open the SQLite database at {self._database!r}: {e}",
                    store=type(self).__name__,
                ) from e
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout}")
            if self._wal_mode:
                await conn.execute("PRAGMA journal_mode = WAL")
            conn.row_factory = aiosqlite.Row
            await conn.executescript(get_schema("all", backend="sqlite", additive=False))
            await apply_additive_updates(conn)
            await conn.commit()
            self._connection = conn
            return conn

    async def close(self) -> None:
        """Close the underlying connection, if open. Safe to call multiple times."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def __aenter__(self) -> SQLiteEventStore:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    def __del__(self) -> None:
        if getattr(self, "_connection", None) is not None:
            import warnings

            db = getattr(self, "_database", "<unknown>")
            msg = f"Unclosed {type(self).__name__} ({db!r}) {self!r}; call 'await store.close()' or use 'async with'"
            warnings.warn(msg, ResourceWarning, stacklevel=2, source=self)

    async def _max_version(
        self, conn: aiosqlite.Connection, aggregate_id: str, category: str
    ) -> int:
        cursor = await conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = ? AND aggregate_type = ?",
            (aggregate_id, category),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def append(
        self, stream: StreamId, events: Sequence[DomainEvent], expected: ExpectedVersion
    ) -> AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")
        conn = await self._conn()
        aid, category = str(stream.aggregate_id), stream.category

        async with self._lock:
            try:
                current_version = await self._max_version(conn, aid, category)
                check_expected(current_version, expected, stream)
                seen_in_batch: set[UUID] = set()
                for event in events:
                    if event.event_id in seen_in_batch:
                        raise DuplicateEventError(
                            f"event_id {event.event_id} already exists in the store"
                        )
                    seen_in_batch.add(event.event_id)

                version = current_version
                first_position: Position | None = None
                now = datetime.now(UTC).isoformat()
                for event in events:
                    version += 1
                    eid, etype, actor = str(event.event_id), event.event_type, event.actor_id
                    tid = str(event.tenant_id) if event.tenant_id else None
                    occ = event.occurred_at.isoformat()
                    payload = json_dumps(event.model_dump(mode="json"))
                    vals = (eid, etype, category, aid, tid, actor, version, occ, payload, now)
                    cursor = await conn.execute(
                        "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, "
                        "tenant_id, actor_id, version, timestamp, payload, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        vals,
                    )
                    if first_position is None:
                        first_position = self._codec.encode(cursor.lastrowid or 0)
                    if self._outbox_enabled:
                        await self._write_to_outbox(conn, event, category, now)

                await conn.commit()
                return AppendResult(stream=stream, new_version=version, position=first_position)
            except aiosqlite.IntegrityError as e:
                await conn.rollback()
                error_str = str(e).lower()
                if "event_id" in error_str:
                    raise DuplicateEventError(
                        f"an event_id in this batch already exists in the store: {e}"
                    ) from e
                if "unique" in error_str and (
                    "aggregate_id" in error_str or "version" in error_str
                ):
                    actual_version = await self._max_version(conn, aid, category)
                    raise OptimisticLockError(
                        stream.aggregate_id, describe_expected(expected), actual_version
                    ) from e
                raise
            except BaseException:
                await conn.rollback()
                raise

    async def _write_to_outbox(
        self, conn: aiosqlite.Connection, event: DomainEvent, category: str, now: str
    ) -> None:
        """Write one outbox row for `event` on `conn` inside the append transaction."""
        eid, aid = str(event.event_id), str(event.aggregate_id)
        tid = str(event.tenant_id) if event.tenant_id else None
        data = json_dumps(outbox_event_data(event))
        await conn.execute(
            "INSERT INTO event_outbox (id, event_id, event_type, aggregate_id, aggregate_type, "
            "tenant_id, event_data, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
            (str(uuid4()), eid, event.event_type, aid, category, tid, data, now),
        )

    async def _fetch_envelopes(
        self, sql: str, params: Sequence[Any]
    ) -> AsyncIterator[EventEnvelope]:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()
        for row in rows:
            yield self._row_to_envelope(row)

    def read_stream(
        self, stream: StreamId, options: StreamReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        sql, params = build_read_stream_query(stream, options or StreamReadOptions())
        return self._fetch_envelopes(sql, params)

    async def get_stream_version(self, stream: StreamId) -> int:
        conn = await self._conn()
        async with self._lock:
            return await self._max_version(conn, str(stream.aggregate_id), stream.category)

    async def event_exists(self, event_id: UUID) -> bool:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute(
                "SELECT 1 FROM events WHERE event_id = ? LIMIT 1", (str(event_id),)
            )
            return await cursor.fetchone() is not None

    def read_all(
        self, from_position: Position | None = None, options: FeedReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_all(from_position, options or FeedReadOptions())

    async def _do_read_all(
        self, from_position: Position | None, options: FeedReadOptions
    ) -> AsyncIterator[EventEnvelope]:
        pos_val = self._codec.value_of(from_position) if from_position is not None else None
        sql, params = build_read_all_query(pos_val, options)
        async for env in self._fetch_envelopes(sql, params):
            yield env

    async def current_position(self) -> Position | None:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute("SELECT MAX(global_position) FROM events")
            row = await cursor.fetchone()
        return None if (row is None or row[0] is None) else self._codec.encode(row[0])

    def read_category(
        self, category: str, options: CategoryReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        sql, params = build_read_category_query(category, options or CategoryReadOptions())
        return self._fetch_envelopes(sql, params)

    def _row_to_envelope(self, row: Any) -> EventEnvelope:
        return row_to_envelope(row, self._codec, self._event_registry)


__all__ = ["AIOSQLITE_AVAILABLE", "SQLiteEventStore"]
