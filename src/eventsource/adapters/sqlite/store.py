"""SQLite adapter implementing `FullEventStore` (serialized under `_lock`, `IntPositionCodec`)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from eventsource import ports as p
from eventsource.adapters._common import check_expected, describe_expected
from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.schemas import get_schema
from eventsource.adapters.sqlite import queries as q
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, default_registry
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports.outbox import outbox_event_data

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    AIOSQLITE_AVAILABLE = False
    aiosqlite = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SQLiteEventStore:
    """SQLite implementation of `FullEventStore` (single shared connection under `_lock`)."""

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
            msg = "aiosqlite is required for SQLiteEventStore. Install with: pip install eventsource[sqlite]"
            raise ImportError(msg)
        self._database, self._event_registry = database, event_registry or default_registry
        self._wal_mode, self._busy_timeout = wal_mode, busy_timeout
        self._outbox_enabled, self._store_id = outbox_enabled, store_id or f"sqlite:{database}"
        self._connection: aiosqlite.Connection | None = None
        self._codec = IntPositionCodec(self._store_id)
        self._lock, self._init_lock = asyncio.Lock(), asyncio.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id

    @property
    def outbox_enabled(self) -> bool:
        return self._outbox_enabled

    async def _conn(self) -> aiosqlite.Connection:
        if self._connection is not None:
            return self._connection
        async with self._init_lock:
            if self._connection is not None:
                return self._connection
            try:
                conn = await aiosqlite.connect(self._database)
            except (aiosqlite.Error, OSError) as e:
                msg = f"could not open the SQLite database at {self._database!r}: {e}"
                raise p.EventStoreConnectionError(msg, store=type(self).__name__) from e
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout}")
            if self._wal_mode:
                await conn.execute("PRAGMA journal_mode = WAL")
            conn.row_factory = aiosqlite.Row
            await conn.executescript(get_schema("all", backend="sqlite", additive=False))
            await q.apply_additive_updates(conn)
            await conn.commit()
            self._connection = conn
            return conn

    async def close(self) -> None:
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

    async def _max_version(self, conn: aiosqlite.Connection, aid: str, category: str) -> int:
        sql = "SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = ? AND aggregate_type = ?"
        row = await (await conn.execute(sql, (aid, category))).fetchone()
        return row[0] if row else 0

    async def append(
        self, stream: StreamId, events: Sequence[DomainEvent], expected: p.ExpectedVersion
    ) -> p.AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")
        conn, aid, category = await self._conn(), str(stream.aggregate_id), stream.category
        async with self._lock:
            try:
                current_version = await self._max_version(conn, aid, category)
                check_expected(current_version, expected, stream)
                seen: set[UUID] = set()
                for event in events:
                    if event.event_id in seen:
                        msg = f"event_id {event.event_id} already exists in the store"
                        raise DuplicateEventError(msg)
                    seen.add(event.event_id)
                version, first_position, now = current_version, None, datetime.now(UTC).isoformat()
                sql = "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, tenant_id, actor_id, version, timestamp, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                for ev in events:
                    version += 1
                    tid = str(ev.tenant_id) if ev.tenant_id else None
                    v1 = (str(ev.event_id), ev.event_type, category, aid, tid, ev.actor_id)
                    ts, data = ev.occurred_at.isoformat(), json_dumps(ev.model_dump(mode="json"))
                    cursor = await conn.execute(sql, (*v1, version, ts, data, now))
                    if first_position is None:
                        first_position = self._codec.encode(cursor.lastrowid or 0)
                    if self._outbox_enabled:
                        await self._write_to_outbox(conn, ev, category, now)
                await conn.commit()
                return p.AppendResult(stream=stream, new_version=version, position=first_position)
            except aiosqlite.IntegrityError as e:
                await conn.rollback()
                err = str(e).lower()
                if "event_id" in err:
                    msg = f"an event_id in this batch already exists in the store: {e}"
                    raise DuplicateEventError(msg) from e
                if "unique" in err and ("aggregate_id" in err or "version" in err):
                    actual = await self._max_version(conn, aid, category)
                    desc = describe_expected(expected)
                    raise OptimisticLockError(stream.aggregate_id, desc, actual) from e
                raise
            except BaseException:
                await conn.rollback()
                raise

    async def _write_to_outbox(self, conn: Any, event: DomainEvent, cat: str, now: str) -> None:
        eid, aid = str(event.event_id), str(event.aggregate_id)
        tid = str(event.tenant_id) if event.tenant_id else None
        data = json_dumps(outbox_event_data(event))
        sql = "INSERT INTO event_outbox (id, event_id, event_type, aggregate_id, aggregate_type, tenant_id, event_data, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')"
        await conn.execute(sql, (str(uuid4()), eid, event.event_type, aid, cat, tid, data, now))

    async def _fetch_envelopes(self, sql: str, params: Any) -> AsyncIterator[p.EventEnvelope]:
        conn = await self._conn()
        async with self._lock:
            rows = await (await conn.execute(sql, params)).fetchall()
        for row in rows:
            yield q.row_to_envelope(row, self._codec, self._event_registry)

    def read_stream(
        self, stream: StreamId, options: p.StreamReadOptions | None = None
    ) -> AsyncIterator[p.EventEnvelope]:
        opts = options or p.StreamReadOptions()
        return self._fetch_envelopes(*q.build_read_stream_query(stream, opts))

    async def get_stream_version(self, stream: StreamId) -> int:
        conn = await self._conn()
        async with self._lock:
            return await self._max_version(conn, str(stream.aggregate_id), stream.category)

    async def event_exists(self, event_id: UUID) -> bool:
        conn = await self._conn()
        async with self._lock:
            sql = "SELECT 1 FROM events WHERE event_id = ? LIMIT 1"
            return await (await conn.execute(sql, (str(event_id),))).fetchone() is not None

    async def read_all(
        self, from_position: p.Position | None = None, options: p.FeedReadOptions | None = None
    ) -> AsyncIterator[p.EventEnvelope]:
        opts = options or p.FeedReadOptions()
        pos_val = self._codec.value_of(from_position) if from_position is not None else None
        async for env in self._fetch_envelopes(*q.build_read_all_query(pos_val, opts)):
            yield env

    async def current_position(self) -> p.Position | None:
        conn = await self._conn()
        async with self._lock:
            row = await (await conn.execute("SELECT MAX(global_position) FROM events")).fetchone()
        return None if (row is None or row[0] is None) else self._codec.encode(row[0])

    def read_category(
        self, category: str, options: p.CategoryReadOptions | None = None
    ) -> AsyncIterator[p.EventEnvelope]:
        opts = options or p.CategoryReadOptions()
        return self._fetch_envelopes(*q.build_read_category_query(category, opts))

    def _row_to_envelope(self, row: Any) -> p.EventEnvelope:
        return q.row_to_envelope(row, self._codec, self._event_registry)


__all__ = ["AIOSQLITE_AVAILABLE", "SQLiteEventStore"]
