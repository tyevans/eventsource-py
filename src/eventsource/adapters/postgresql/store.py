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
predicate documented on `_HORIZON_PREDICATE` below to avoid skipping a
lower position that is still in flight.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.domain import StreamId
from eventsource.events import DomainEvent
from eventsource.events.registry import EventRegistry, default_registry
from eventsource.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.migrations import get_schema
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    ReadDirection,
    StreamReadOptions,
    outbox_event_data,
)
from eventsource.serialization import json_dumps, json_loads

try:
    import asyncpg  # noqa: F401

    ASYNCPG_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    ASYNCPG_AVAILABLE = False

logger = logging.getLogger(__name__)

# Sentinel ints preserved for OptimisticLockError's expected_version field, which
# predates ExpectedVersion and is still int-typed. Mirrors stores/interface.py's
# ExpectedVersion.ANY / NO_STREAM / STREAM_EXISTS constants for message fidelity.
_ANY_SENTINEL = -1
_NO_STREAM_SENTINEL = 0
_STREAM_EXISTS_SENTINEL = -2

_SELECT_COLUMNS = """
    global_position, event_id, event_type, aggregate_type, aggregate_id,
    tenant_id, actor_id, version, timestamp, payload, created_at
"""

# Rows whose inserting transaction is not yet definitely-committed
# (xmin >= snapshot xmin) are deferred to a later poll -- the global_position
# sequence commits out of order under concurrent writers, and reading past a
# still-uncommitted lower position would skip it forever once the reader
# resumes from higher up. Uses the `xmin` system column: no DDL, schema
# untouched. Caveat: epoch comparison is not wraparound-proof in the
# ancient-xid regime; acceptable for now, revisit if a `xid8` column is ever
# added.
_HORIZON_PREDICATE = "xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint"

# Constraint names from the canonical `migrations/schemas/events.sql` (verified
# against a live PostgreSQL 15 by introspecting `asyncpg.exceptions
# .UniqueViolationError.constraint_name` on both conflict paths -- see
# `_classify_integrity_error`).
_EVENT_ID_UNIQUE_CONSTRAINT = "events_event_id_key"
_AGGREGATE_VERSION_UNIQUE_CONSTRAINT = "uq_events_aggregate_version"


class PostgreSQLEventStore:
    """PostgreSQL implementation of `FullEventStore`.

    Uses async SQLAlchemy with the asyncpg driver.

    Schema ownership: like the legacy `stores/postgresql.py`, this adapter
    does NOT create the `events` table by default -- production deployments
    apply the canonical `migrations/schemas/events.sql` (via `migrations/`
    tooling) out of band, and this store simply queries an existing table.
    Pass `create_schema=True` (tests, local dev only) to opt into lazy
    `CREATE TABLE IF NOT EXISTS` schema creation on first use, guarded by an
    `asyncio.Lock`, using the same canonical schema. Leaving it `False` in
    production also avoids concurrent `CREATE INDEX IF NOT EXISTS` racing
    across processes ("tuple concurrently updated").

    Structural conformance only -- no inheritance from the port protocols.

    Attributes:
        max_append_batch: No batch-size limit is enforced by this adapter.
    """

    max_append_batch: int | None = None

    def __init__(
        self,
        engine: AsyncEngine,
        event_registry: EventRegistry | None = None,
        *,
        store_id: str | None = None,
        create_schema: bool = False,
        outbox_enabled: bool = False,
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
        via `migrations/`, and queries against a missing table fail
        naturally.

        Runs the canonical `migrations/schemas/events.sql` (the same file
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
            async with self._engine.connect() as conn:
                raw = await conn.get_raw_connection()
                driver_connection = raw.driver_connection
                assert driver_connection is not None
                await driver_connection.execute(get_schema("events"))
                await conn.commit()
            self._schema_ready = True

    async def close(self) -> None:
        """Dispose the underlying engine. Safe to call multiple times."""
        await self._engine.dispose()

    def _check_expected(self, current: int, expected: ExpectedVersion, stream: StreamId) -> None:
        if expected.kind == "any":
            return
        if expected.kind == "no_stream":
            if current != 0:
                raise OptimisticLockError(stream.aggregate_id, _NO_STREAM_SENTINEL, current)
            return
        if expected.kind == "stream_exists":
            if current == 0:
                raise OptimisticLockError(stream.aggregate_id, _STREAM_EXISTS_SENTINEL, current)
            return
        if expected.kind == "exact":
            if current != expected.version:
                raise OptimisticLockError(stream.aggregate_id, expected.version or 0, current)
            return
        raise ValueError(f"unknown ExpectedVersion kind: {expected.kind!r}")

    def _expected_sentinel(self, expected: ExpectedVersion) -> int:
        if expected.kind == "any":
            return _ANY_SENTINEL
        if expected.kind == "no_stream":
            return _NO_STREAM_SENTINEL
        if expected.kind == "stream_exists":
            return _STREAM_EXISTS_SENTINEL
        return expected.version or 0

    def _classify_integrity_error(self, e: IntegrityError) -> str | None:
        """Classify an append `IntegrityError` by the real constraint name.

        SQLAlchemy's asyncpg dialect wraps the driver exception in its own
        DBAPI-compat `IntegrityError` (`e.orig`), which does not itself
        carry `constraint_name`; the underlying `asyncpg.exceptions
        .UniqueViolationError` (`e.orig.__cause__`) does. Verified against a
        live PostgreSQL 15 server: `events_event_id_key` for the `event_id`
        unique violation, `uq_events_aggregate_version` for the
        `(aggregate_id, aggregate_type, version)` conflict.

        Falls back to substring-matching the stringified exception only
        when no `constraint_name` attribute is found on either the DBAPI
        exception or its cause (e.g. a future driver/dialect change) --
        this keeps the classification working, just less precisely.
        """
        constraint_name = getattr(e.orig, "constraint_name", None) or getattr(
            getattr(e.orig, "__cause__", None), "constraint_name", None
        )
        if constraint_name == _EVENT_ID_UNIQUE_CONSTRAINT:
            return "event_id"
        if constraint_name == _AGGREGATE_VERSION_UNIQUE_CONSTRAINT:
            return "aggregate_version"
        if constraint_name is not None:
            return None

        # Fallback: no constraint_name available anywhere -- substring match.
        error_str = str(e).lower()
        if "event_id" in error_str:
            return "event_id"
        if "uq_events_aggregate_version" in error_str or (
            "unique" in error_str and "aggregate" in error_str and "version" in error_str
        ):
            return "aggregate_version"
        return None

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")

        await self._ensure_schema()
        category = stream.category

        async with self._session_factory() as session:
            try:
                result = await session.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(version), 0)
                        FROM events
                        WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type
                        """
                    ),
                    {"aggregate_id": stream.aggregate_id, "aggregate_type": category},
                )
                current_version = result.scalar() or 0

                self._check_expected(current_version, expected, stream)

                seen_in_batch: set[UUID] = set()
                for event in events:
                    if event.event_id in seen_in_batch:
                        raise DuplicateEventError(
                            f"event_id {event.event_id} already exists in the store"
                        )
                    seen_in_batch.add(event.event_id)

                version = current_version
                first_position: Position | None = None

                for event in events:
                    version += 1
                    insert_result = await session.execute(
                        text(
                            """
                            INSERT INTO events (
                                event_id, event_type, aggregate_type, aggregate_id,
                                tenant_id, actor_id, version, timestamp, payload, created_at
                            )
                            VALUES (
                                :event_id, :event_type, :aggregate_type, :aggregate_id,
                                :tenant_id, :actor_id, :version, :timestamp, :payload, NOW()
                            )
                            RETURNING global_position
                            """
                        ),
                        {
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "aggregate_type": category,
                            "aggregate_id": stream.aggregate_id,
                            "tenant_id": event.tenant_id,
                            "actor_id": event.actor_id,
                            "version": version,
                            "timestamp": event.occurred_at,
                            "payload": json_dumps(event.model_dump(mode="json")),
                        },
                    )
                    global_position = insert_result.scalar()
                    if first_position is None and global_position is not None:
                        first_position = self._codec.encode(global_position)

                    if self._outbox_enabled:
                        await self._write_to_outbox(session, event, category)

                await session.commit()
                return AppendResult(stream=stream, new_version=version, position=first_position)

            except IntegrityError as e:
                await session.rollback()
                conflict = self._classify_integrity_error(e)
                if conflict == "event_id":
                    raise DuplicateEventError(
                        f"an event_id in this batch already exists in the store: {e}"
                    ) from e
                if conflict == "aggregate_version":
                    result = await session.execute(
                        text(
                            """
                            SELECT COALESCE(MAX(version), 0)
                            FROM events
                            WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type
                            """
                        ),
                        {"aggregate_id": stream.aggregate_id, "aggregate_type": category},
                    )
                    actual_version = result.scalar() or 0
                    raise OptimisticLockError(
                        stream.aggregate_id, self._expected_sentinel(expected), actual_version
                    ) from e
                raise

    async def _write_to_outbox(
        self,
        session: AsyncSession,
        event: DomainEvent,
        aggregate_type: str,
    ) -> None:
        """Write one outbox row for `event`, on `session`, before commit.

        Must run on the same `AsyncSession` as the
        event `INSERT`, before `append`'s single `await session.commit()`
        -- that is the atomicity guarantee the transactional outbox
        pattern exists to provide. The outbox *reader* implements
        `eventsource.ports.outbox.OutboxRepository`; the payload
        shape (six keys, `payload` = `model_dump(mode="json")`) must
        match what it expects exactly.

        Uses stdlib `json.dumps` rather than this module's `json_dumps`
        (orjson-backed): the payload returned from `ports.outbox.outbox_event_data`
        is already reduced to JSON-safe primitives (`str`/`dict`/`None`),
        so the two would serialize identically, but stdlib is used to mirror the
        legacy store byte-for-byte and avoid any doubt.
        """
        outbox_id = uuid4()
        now = datetime.now(UTC)

        event_data = outbox_event_data(event)

        await session.execute(
            text(
                """
                INSERT INTO event_outbox (
                    id, event_id, event_type, aggregate_id, aggregate_type,
                    tenant_id, event_data, created_at, status
                )
                VALUES (
                    :id, :event_id, :event_type, :aggregate_id, :aggregate_type,
                    :tenant_id, :event_data, :created_at, 'pending'
                )
                """
            ),
            {
                "id": outbox_id,
                "event_id": event.event_id,
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": aggregate_type,
                "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                "event_data": json.dumps(event_data),
                "created_at": now,
            },
        )

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
        await self._ensure_schema()

        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events"  # nosec B608 -- constant column list
            " WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
        ]
        params: dict[str, Any] = {
            "aggregate_id": stream.aggregate_id,
            "aggregate_type": stream.category,
        }

        if options.from_version is not None:
            query_parts.append("AND version >= :from_version")
            params["from_version"] = options.from_version
        if options.to_version is not None:
            query_parts.append("AND version <= :to_version")
            params["to_version"] = options.to_version

        if options.direction == ReadDirection.BACKWARD:
            query_parts.append("ORDER BY version DESC")
        else:
            query_parts.append("ORDER BY version ASC")

        if options.limit is not None:
            query_parts.append("LIMIT :limit")
            params["limit"] = options.limit

        async with self._session_factory() as session:
            result = await session.execute(text("\n".join(query_parts)), params)
            rows = result.mappings().all()

        for row in rows:
            yield self._row_to_envelope(row)

    async def get_stream_version(self, stream: StreamId) -> int:
        await self._ensure_schema()
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    """
                    SELECT COALESCE(MAX(version), 0)
                    FROM events
                    WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type
                    """
                ),
                {"aggregate_id": stream.aggregate_id, "aggregate_type": stream.category},
            )
            return result.scalar() or 0

    async def event_exists(self, event_id: UUID) -> bool:
        await self._ensure_schema()
        async with self._session_factory() as session:
            result = await session.execute(
                text("SELECT 1 FROM events WHERE event_id = :event_id LIMIT 1"),
                {"event_id": event_id},
            )
            return result.first() is not None

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
        await self._ensure_schema()

        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events"  # nosec B608 -- constant column list
            f" WHERE {_HORIZON_PREDICATE}"
        ]
        params: dict[str, Any] = {}

        if from_position is not None:
            query_parts.append("AND global_position > :from_position")
            params["from_position"] = self._codec.value_of(from_position)

        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = :tenant_id")
            params["tenant_id"] = options.tenant_id

        query_parts.append("ORDER BY global_position ASC")

        if options.limit is not None:
            query_parts.append("LIMIT :limit")
            params["limit"] = options.limit

        async with self._session_factory() as session:
            result = await session.execute(text("\n".join(query_parts)), params)
            rows = result.mappings().all()

        for row in rows:
            yield self._row_to_envelope(row)

    async def current_position(self) -> Position | None:
        await self._ensure_schema()
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT MAX(global_position) FROM events"  # nosec B608 -- constant predicate
                    f" WHERE {_HORIZON_PREDICATE}"
                )
            )
            value = result.scalar()
        if value is None:
            return None
        return self._codec.encode(value)

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
        await self._ensure_schema()

        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events"  # nosec B608 -- constant column list
            " WHERE aggregate_type = :aggregate_type"
        ]
        params: dict[str, Any] = {"aggregate_type": category}

        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = :tenant_id")
            params["tenant_id"] = options.tenant_id

        # Filtered and ordered by `created_at` (storage time), not `timestamp`
        # (the event's own `occurred_at`) -- this matches the port contract
        # (`EventEnvelope.stored_at`). `from_timestamp` is inclusive per the
        # port contract, hence `>=`.
        if options.from_timestamp is not None:
            query_parts.append("AND created_at >= :from_timestamp")
            params["from_timestamp"] = options.from_timestamp

        # `created_at` alone ties within a batch (NOW() is transaction time,
        # constant across the whole INSERT loop), so `global_position` breaks
        # the tie deterministically.
        query_parts.append("ORDER BY created_at ASC, global_position ASC")

        if options.limit is not None:
            query_parts.append("LIMIT :limit")
            params["limit"] = options.limit

        async with self._session_factory() as session:
            result = await session.execute(text("\n".join(query_parts)), params)
            rows = result.mappings().all()

        for row in rows:
            yield self._row_to_envelope(row)

    def _row_to_envelope(self, row: Any) -> EventEnvelope:
        event = self._deserialize_event(row["event_type"], row["payload"])
        stream_id = StreamId(
            aggregate_id=row["aggregate_id"],
            category=row["aggregate_type"],
        )
        return EventEnvelope(
            event=event,
            stream_id=stream_id,
            stream_version=row["version"],
            position=self._codec.encode(row["global_position"]),
            stored_at=row["created_at"],
        )

    def _deserialize_event(self, event_type: str, payload: Any) -> DomainEvent:
        event_class = self._event_registry.get(event_type)
        data = payload if isinstance(payload, dict) else json_loads(payload)
        return event_class.model_validate(data)


__all__ = ["ASYNCPG_AVAILABLE", "PostgreSQLEventStore"]
