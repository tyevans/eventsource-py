"""SQLite adapter implementing the five store ports.

Targets the `eventsource.ports.store` protocols: rows map to
`EventEnvelope` / `AppendResult` / `Position` value objects, and append
dispatches on `ExpectedVersion.kind` rather than integer sentinels.

Positions are minted from the `events.global_position` autoincrement
column via `IntPositionCodec`.

No safe-horizon handling: SQLite writers are serialized (a single
connection, one implicit write transaction at a time -- WAL mode still
allows only one writer), so a reader can never observe a lower
global_position commit after a higher one. The exclusive
`global_position > :from` predicate `read_all` already uses is
sufficient; there is no gap to skip.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from eventsource.adapters._common import check_expected, describe_expected
from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.serialization import json_dumps, json_loads
from eventsource.adapters.sql.schemas import get_schema
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
    ReadDirection,
    StreamReadOptions,
)

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    AIOSQLITE_AVAILABLE = False
    aiosqlite = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


_SELECT_COLUMNS = """
    global_position, event_id, event_type, aggregate_type, aggregate_id,
    tenant_id, actor_id, version, timestamp, payload, created_at
"""


class SQLiteEventStore:
    """SQLite implementation of `FullEventStore`.

    Uses aiosqlite for async database operations. A single connection is
    opened lazily on first use and reused for the store's lifetime --
    required for `":memory:"` databases, whose contents live only as
    long as the connection that created them stays open. A second
    connection would see a different, empty database, which is why reads
    are not given one.

    Connection discipline: *every* statement on that shared connection
    runs under `self._lock` -- reads as well as `append`. `append` is
    multi-statement (per-event INSERTs, then `commit()`), and a
    same-connection read scheduled between two of them would run inside
    its open transaction and observe a torn batch. Readers therefore
    never see an open append transaction. `asyncio.Lock` is
    non-reentrant, so `append`'s internal SELECTs use the connection
    directly and must never be routed through the public read helpers.

    Structural conformance only -- no inheritance from the port protocols.
    """

    def __init__(
        self,
        database: str,
        event_registry: EventRegistry | None = None,
        *,
        store_id: str | None = None,
        wal_mode: bool = True,
        busy_timeout: int = 5000,
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
        self._connection: aiosqlite.Connection | None = None
        # Defaults to the database path, which is unique only within a host
        # -- and is "sqlite::memory:" for every in-memory store. Pass
        # store_id explicitly when two such stores could meet; see
        # Position's docstring.
        self._store_id = store_id or f"sqlite:{database}"
        self._codec = IntPositionCodec(self._store_id)
        self._lock = asyncio.Lock()
        # Dedicated lock for connection setup, distinct from `self._lock`
        # (held around `append`'s read-check-write sequence). `append` calls
        # `_conn()` before acquiring `self._lock`; guarding connection setup
        # with the same lock would be fine for that ordering, but a separate
        # lock keeps `_conn()` safe to call from any code path (readers too)
        # without coupling to append's locking discipline.
        self._init_lock = asyncio.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id

    async def _conn(self) -> aiosqlite.Connection:
        """Return the live connection, opening and initializing it on first use.

        Double-checked locking around `self._init_lock` (mirroring
        `PostgreSQLEventStore._ensure_schema`'s pattern): without it, two
        concurrent first-callers can each open an `aiosqlite.connect()` --
        a non-daemon background thread each -- and the loser's connection
        is simply discarded, leaking its thread for the process lifetime.
        """
        if self._connection is not None:
            return self._connection

        async with self._init_lock:
            if self._connection is not None:
                return self._connection

            try:
                conn = await aiosqlite.connect(self._database)
            except (aiosqlite.Error, OSError) as e:
                # Without this the user sees a bare
                # `sqlite3.OperationalError: unable to open database file`
                # with nothing naming the library, the adapter, or the path.
                raise EventStoreConnectionError(
                    f"could not open the SQLite database at {self._database!r}: {e}",
                    store=type(self).__name__,
                ) from e

            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout}")
            if self._wal_mode:
                await conn.execute("PRAGMA journal_mode = WAL")
            conn.row_factory = aiosqlite.Row

            schema = get_schema("all", backend="sqlite", additive=False)
            await conn.executescript(schema)
            await self._apply_additive_updates(conn)
            await conn.commit()

            self._connection = conn
            return conn

    async def _apply_additive_updates(self, conn: aiosqlite.Connection) -> None:
        """Apply additive schema fragments SQLite cannot express idempotently.

        SQLite has no `ADD COLUMN IF NOT EXISTS`, and this schema is applied
        on every first connection -- including to a file that already carries
        the column from an earlier process.
        """
        async with conn.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}
        if "position_token" not in columns:
            await conn.execute("ALTER TABLE projection_checkpoints ADD COLUMN position_token TEXT")

    async def close(self) -> None:
        """Close the underlying connection, if open. Safe to call multiple times."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")

        conn = await self._conn()
        aggregate_id_str = str(stream.aggregate_id)
        category = stream.category

        async with self._lock:
            try:
                cursor = await conn.execute(
                    """
                    SELECT COALESCE(MAX(version), 0)
                    FROM events
                    WHERE aggregate_id = ? AND aggregate_type = ?
                    """,
                    (aggregate_id_str, category),
                )
                row = await cursor.fetchone()
                current_version = row[0] if row else 0

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
                    cursor = await conn.execute(
                        """
                        INSERT INTO events (
                            event_id, event_type, aggregate_type, aggregate_id,
                            tenant_id, actor_id, version, timestamp, payload, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(event.event_id),
                            event.event_type,
                            category,
                            aggregate_id_str,
                            str(event.tenant_id) if event.tenant_id else None,
                            event.actor_id,
                            version,
                            event.occurred_at.isoformat(),
                            json_dumps(event.model_dump(mode="json")),
                            now,
                        ),
                    )
                    global_position = cursor.lastrowid or 0
                    if first_position is None:
                        first_position = self._codec.encode(global_position)

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
                    cursor = await conn.execute(
                        """
                        SELECT COALESCE(MAX(version), 0)
                        FROM events
                        WHERE aggregate_id = ? AND aggregate_type = ?
                        """,
                        (aggregate_id_str, category),
                    )
                    row = await cursor.fetchone()
                    actual_version = row[0] if row else 0
                    raise OptimisticLockError(
                        stream.aggregate_id, describe_expected(expected), actual_version
                    ) from e
                raise
            except BaseException:
                # Any non-IntegrityError failure mid-batch (e.g. an
                # OperationalError, or a serialization failure from
                # `json_dumps`/`event.model_dump`) still leaves a dirty open
                # transaction on the shared connection -- the *next*
                # `append()`'s `commit()` would silently commit a torn
                # batch. Roll back on every exception, not just the
                # classified IntegrityError path above.
                await conn.rollback()
                raise

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        opts = options or StreamReadOptions()
        return self._do_read_stream(stream, opts)

    async def _do_read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        conn = await self._conn()

        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events"  # nosec B608 -- constant column list
            " WHERE aggregate_id = ? AND aggregate_type = ?"
        ]
        params: list[Any] = [str(stream.aggregate_id), stream.category]

        if options.from_version is not None:
            query_parts.append("AND version >= ?")
            params.append(options.from_version)
        if options.to_version is not None:
            query_parts.append("AND version <= ?")
            params.append(options.to_version)

        if options.direction == ReadDirection.BACKWARD:
            query_parts.append("ORDER BY version DESC")
        else:
            query_parts.append("ORDER BY version ASC")

        if options.limit is not None:
            query_parts.append("LIMIT ?")
            params.append(options.limit)

        async with self._lock:
            cursor = await conn.execute("\n".join(query_parts), params)
            rows = await cursor.fetchall()

        for row in rows:
            yield self._row_to_envelope(row)

    async def get_stream_version(self, stream: StreamId) -> int:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute(
                """
                SELECT COALESCE(MAX(version), 0)
                FROM events
                WHERE aggregate_id = ? AND aggregate_type = ?
                """,
                (str(stream.aggregate_id), stream.category),
            )
            row = await cursor.fetchone()
        return row[0] if row else 0

    async def event_exists(self, event_id: UUID) -> bool:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute(
                "SELECT 1 FROM events WHERE event_id = ? LIMIT 1",
                (str(event_id),),
            )
            return await cursor.fetchone() is not None

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        opts = options or FeedReadOptions()
        return self._do_read_all(from_position, opts)

    async def _do_read_all(
        self,
        from_position: Position | None,
        options: FeedReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        conn = await self._conn()

        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events WHERE 1=1"  # nosec B608 -- constant column list
        ]
        params: list[Any] = []

        if from_position is not None:
            query_parts.append("AND global_position > ?")
            params.append(self._codec.value_of(from_position))

        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = ?")
            params.append(str(options.tenant_id))

        if options.aggregate_type is not None:
            query_parts.append("AND aggregate_type = ?")
            params.append(options.aggregate_type)

        query_parts.append("ORDER BY global_position ASC")

        if options.limit is not None:
            query_parts.append("LIMIT ?")
            params.append(options.limit)

        async with self._lock:
            cursor = await conn.execute("\n".join(query_parts), params)
            rows = await cursor.fetchall()

        for row in rows:
            yield self._row_to_envelope(row)

    async def current_position(self) -> Position | None:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute("SELECT MAX(global_position) FROM events")
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return self._codec.encode(row[0])

    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        opts = options or CategoryReadOptions()
        return self._do_read_category(category, opts)

    async def _do_read_category(
        self,
        category: str,
        options: CategoryReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        conn = await self._conn()

        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events"  # nosec B608 -- constant column list
            " WHERE aggregate_type = ?"
        ]
        params: list[Any] = [category]

        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = ?")
            params.append(str(options.tenant_id))

        # Filtered and ordered by `created_at` (storage time), not `timestamp`
        # (the event's own `occurred_at`) -- this matches the port contract
        # (`EventEnvelope.stored_at`), mirroring the memory adapter's
        # `read_category`, which filters/orders on `stored_at`. `from_timestamp`
        # is inclusive per the port contract, hence `>=`.
        if options.from_timestamp is not None:
            # `created_at` is TEXT, so this is a *lexical* comparison, and the
            # stored values are all `datetime.now(UTC).isoformat()` -- offset
            # `+00:00`. A bound rendered at any other offset sorts by its
            # printed digits rather than the instant it denotes (a `+05:00`
            # bound sorts after every stored row, silently returning nothing),
            # so normalize to UTC before formatting. A naive datetime is read
            # as UTC rather than compared offset-free, which would sort it
            # ahead of every stored row for the same instant.
            bound = options.from_timestamp
            bound = bound.replace(tzinfo=UTC) if bound.tzinfo is None else bound.astimezone(UTC)
            query_parts.append("AND created_at >= ?")
            params.append(bound.isoformat())

        # `created_at` alone ties within a batch (SQLite stamps one `now`
        # per batch), so `global_position` breaks the tie deterministically.
        query_parts.append("ORDER BY created_at ASC, global_position ASC")

        if options.limit is not None:
            query_parts.append("LIMIT ?")
            params.append(options.limit)

        async with self._lock:
            cursor = await conn.execute("\n".join(query_parts), params)
            rows = await cursor.fetchall()

        for row in rows:
            yield self._row_to_envelope(row)

    def _row_to_envelope(self, row: Any) -> EventEnvelope:
        event = self._deserialize_event(row["event_type"], row["payload"])
        stream_id = StreamId(
            aggregate_id=UUID(row["aggregate_id"]),
            category=row["aggregate_type"],
        )
        stored_at = datetime.fromisoformat(row["created_at"]).replace(tzinfo=UTC)
        return EventEnvelope(
            event=event,
            stream_id=stream_id,
            stream_version=row["version"],
            position=self._codec.encode(row["global_position"]),
            stored_at=stored_at,
        )

    def _deserialize_event(self, event_type: str, payload: str) -> DomainEvent:
        event_class = self._event_registry.get(event_type)
        data = json_loads(payload)
        return event_class.model_validate(data)


__all__ = ["AIOSQLITE_AVAILABLE", "SQLiteEventStore"]
