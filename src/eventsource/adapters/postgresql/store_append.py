"""Append operations mixin for PostgreSQLEventStore."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters._common import check_expected, describe_expected
from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.postgresql.locks_helpers import key_to_lock_id
from eventsource.adapters.postgresql.store_append_conflicts import classify_integrity_error
from eventsource.adapters.postgresql.store_append_outbox import write_outbox_row
from eventsource.adapters.serialization import json_dumps
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.observability.error_summary import error_summary
from eventsource.ports import AppendResult, ExpectedVersion, Position

# Per-stream append lock. Taken before the MAX(version) read so that two
# appends to one stream serialize, whatever the table's unique constraints say.
# Transaction-scoped: released at commit or rollback.
_STREAM_APPEND_LOCK_SQL = text("SELECT pg_advisory_xact_lock(:lock_id)")
# Side table present only in the partitioned schema (events_partitioned.sql).
_EVENT_IDS_PROBE_SQL = text("SELECT to_regclass('event_ids') IS NOT NULL")
_INSERT_EVENT_ID_SQL = text("INSERT INTO event_ids (event_id) VALUES (:event_id)")
_MAX_VERSION_SQL = text(
    "SELECT COALESCE(MAX(version), 0) FROM events "
    "WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
)
_INSERT_EVENT_SQL = text(
    "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, tenant_id, "
    "actor_id, version, timestamp, payload, created_at) VALUES (:event_id, :event_type, "
    ":aggregate_type, :aggregate_id, :tenant_id, :actor_id, :version, :timestamp, :payload, NOW()) "
    "RETURNING global_position"
)


class PostgreSQLEventStoreAppendMixin:
    """Mixin providing append and outbox operations for PostgreSQLEventStore."""

    _session_factory: async_sessionmaker[AsyncSession]
    _codec: IntPositionCodec
    _outbox_enabled: bool
    # None until the first append probes the database for the `event_ids` side table.
    _event_ids_table: bool | None = None

    async def _ensure_schema(self) -> None:
        """Ensures schema is initialized (implemented in PostgreSQLEventStore)."""

    async def append(
        self, stream: StreamId, events: Sequence[DomainEvent], expected: ExpectedVersion
    ) -> AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")

        await self._ensure_schema()
        category = stream.category
        ver_params = {"aggregate_id": stream.aggregate_id, "aggregate_type": category}

        async with self._session_factory() as session:
            try:
                lock_id = key_to_lock_id(f"eventsource.append:{category}:{stream.aggregate_id}")
                await session.execute(_STREAM_APPEND_LOCK_SQL, {"lock_id": lock_id})
                use_event_ids = await self._uses_event_ids_table(session)
                result = await session.execute(_MAX_VERSION_SQL, ver_params)
                current_version = result.scalar() or 0
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

                for event in events:
                    version += 1
                    params = {
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "aggregate_type": category,
                        "aggregate_id": stream.aggregate_id,
                        "tenant_id": event.tenant_id,
                        "actor_id": event.actor_id,
                        "version": version,
                        "timestamp": event.occurred_at,
                        "payload": json_dumps(event.model_dump(mode="json")),
                    }
                    if use_event_ids:
                        await session.execute(_INSERT_EVENT_ID_SQL, {"event_id": event.event_id})
                    insert_result = await session.execute(_INSERT_EVENT_SQL, params)
                    global_position = insert_result.scalar()
                    if first_position is None and global_position is not None:
                        first_position = self._codec.encode(global_position)
                    if self._outbox_enabled:
                        await write_outbox_row(session, event, category)

                await session.commit()
                return AppendResult(stream=stream, new_version=version, position=first_position)

            except IntegrityError as e:
                await session.rollback()
                conflict = classify_integrity_error(e)
                if conflict == "event_id":
                    raise DuplicateEventError(
                        f"an event_id in this batch already exists in the store: {error_summary(e)}"
                    ) from e
                if conflict == "aggregate_version":
                    result = await session.execute(_MAX_VERSION_SQL, ver_params)
                    actual_version = result.scalar() or 0
                    raise OptimisticLockError(
                        stream.aggregate_id, describe_expected(expected), actual_version
                    ) from e
                raise

    async def _uses_event_ids_table(self, session: AsyncSession) -> bool:
        """Detect the partitioned schema's `event_ids` side table once per store."""
        if self._event_ids_table is None:
            result = await session.execute(_EVENT_IDS_PROBE_SQL)
            self._event_ids_table = bool(result.scalar())
        return self._event_ids_table
