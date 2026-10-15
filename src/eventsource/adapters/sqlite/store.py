"""SQLite adapter implementing `FullEventStore` (serialized under `_lock`, `IntPositionCodec`)."""

from __future__ import annotations

import asyncio
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
from eventsource.domain import DomainEvent, EventRegistry, StreamId, default_registry
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports.outbox import outbox_event_data

try:
    import aiosqlite
except ImportError:  # pragma: no cover - exercised only without the optional dep
    aiosqlite = None  # type: ignore[assignment]
AIOSQLITE_AVAILABLE = aiosqlite is not None


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
        self.store_id, self.outbox_enabled = self._store_id, self._outbox_enabled

    async def _conn(self) -> aiosqlite.Connection:
        if self._connection is None:
            async with self._init_lock:
                if self._connection is None:
                    try:
                        conn = await aiosqlite.connect(self._database)
                    except (aiosqlite.Error, OSError) as e:
                        msg = f"could not open the SQLite database at {self._database!r}: {e}"
                        raise p.EventStoreConnectionError(msg, store=type(self).__name__) from e
                    pragmas = ("foreign_keys = ON", f"busy_timeout = {self._busy_timeout}")
                    for pragma in (*pragmas, *(("journal_mode = WAL",) if self._wal_mode else ())):
                        await conn.execute(f"PRAGMA {pragma}")
                    conn.row_factory = aiosqlite.Row
                    await conn.executescript(get_schema("all", backend="sqlite", additive=False))
                    await q.apply_additive_updates(conn)
                    await conn.commit()
                    self._connection = conn
        return self._connection

    async def close(self) -> None:
        if (conn := self._connection) is not None:
            await conn.close()
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
                version, first_pos, now = current_version, None, datetime.now(UTC).isoformat()
                sql = "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, tenant_id, actor_id, version, timestamp, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                for ev in events:
                    if ev.event_id in seen:
                        raise DuplicateEventError(f"event_id {ev.event_id} already exists in store")
                    seen.add(ev.event_id)
                    version += 1
                    tid = str(ev.tenant_id) if ev.tenant_id else None
                    v1 = (str(ev.event_id), ev.event_type, category, aid, tid, ev.actor_id)
                    ts, data = ev.occurred_at.isoformat(), json_dumps(ev.model_dump(mode="json"))
                    cursor = await conn.execute(sql, (*v1, version, ts, data, now))
                    first_pos = first_pos or self._codec.encode(cursor.lastrowid or 0)
                    if self._outbox_enabled:
                        await self._write_to_outbox(conn, ev, category, now)
                await conn.commit()
                return p.AppendResult(stream=stream, new_version=version, position=first_pos)
            except aiosqlite.IntegrityError as e:
                await conn.rollback()
                err = str(e).lower()
                if "event_id" in err:
                    raise DuplicateEventError(f"event_id already exists in the store: {e}") from e
                if "unique" in err and ("aggregate_id" in err or "version" in err):
                    actual = await self._max_version(conn, aid, category)
                    desc = describe_expected(expected)
                    raise OptimisticLockError(stream.aggregate_id, desc, actual) from e
                raise
            except BaseException:
                await conn.rollback()
                raise

    async def _write_to_outbox(self, conn: Any, ev: DomainEvent, cat: str, now: str) -> None:
        tid, data = str(ev.tenant_id) if ev.tenant_id else None, json_dumps(outbox_event_data(ev))
        sql = "INSERT INTO event_outbox (id, event_id, event_type, aggregate_id, aggregate_type, tenant_id, event_data, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')"
        row = (str(uuid4()), str(ev.event_id), ev.event_type, str(ev.aggregate_id), cat, tid)
        await conn.execute(sql, (*row, data, now))

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
        sql, conn = "SELECT 1 FROM events WHERE event_id = ? LIMIT 1", await self._conn()
        async with self._lock:
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
