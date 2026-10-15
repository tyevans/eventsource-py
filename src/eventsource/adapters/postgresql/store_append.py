"""Append operations mixin for PostgreSQLEventStore."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters._common import check_expected, describe_expected
from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.serialization import json_dumps
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import (
    AppendResult,
    ExpectedVersion,
    Position,
    outbox_event_data,
)

_EVENT_ID_UNIQUE_CONSTRAINT = "events_event_id_key"
_AGGREGATE_VERSION_UNIQUE_CONSTRAINT = "uq_events_aggregate_version"
_MAX_VERSION_SQL = text(
    "SELECT COALESCE(MAX(version), 0) FROM events "
    "WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
)


class PostgreSQLEventStoreAppendMixin:
    """Mixin providing append and outbox operations for PostgreSQLEventStore."""

    _session_factory: async_sessionmaker[AsyncSession]
    _codec: IntPositionCodec
    _outbox_enabled: bool

    async def _ensure_schema(self) -> None:
        """Ensures schema is initialized (implemented in PostgreSQLEventStore)."""

    def _classify_integrity_error(self, e: IntegrityError) -> str | None:
        """Classify an append `IntegrityError` by the real constraint name or substring fallback."""
        constraint_name = getattr(e.orig, "constraint_name", None) or getattr(
            getattr(e.orig, "__cause__", None), "constraint_name", None
        )
        if constraint_name == _EVENT_ID_UNIQUE_CONSTRAINT:
            return "event_id"
        if constraint_name == _AGGREGATE_VERSION_UNIQUE_CONSTRAINT:
            return "aggregate_version"
        if constraint_name is not None:
            return None

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
        ver_params = {"aggregate_id": stream.aggregate_id, "aggregate_type": category}

        async with self._session_factory() as session:
            try:
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
                    insert_result = await session.execute(
                        text(
                            "INSERT INTO events ("
                            "event_id, event_type, aggregate_type, aggregate_id, "
                            "tenant_id, actor_id, version, timestamp, payload, created_at"
                            ") VALUES ("
                            ":event_id, :event_type, :aggregate_type, :aggregate_id, "
                            ":tenant_id, :actor_id, :version, :timestamp, :payload, NOW()"
                            ") RETURNING global_position"
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
                    result = await session.execute(_MAX_VERSION_SQL, ver_params)
                    actual_version = result.scalar() or 0
                    raise OptimisticLockError(
                        stream.aggregate_id, describe_expected(expected), actual_version
                    ) from e
                raise

    async def _write_to_outbox(
        self,
        session: AsyncSession,
        event: DomainEvent,
        aggregate_type: str,
    ) -> None:
        """Write one outbox row for `event`, on `session`, before commit."""
        await session.execute(
            text(
                "INSERT INTO event_outbox ("
                "id, event_id, event_type, aggregate_id, aggregate_type, "
                "tenant_id, event_data, created_at, status"
                ") VALUES ("
                ":id, :event_id, :event_type, :aggregate_id, :aggregate_type, "
                ":tenant_id, :event_data, :created_at, 'pending'"
                ")"
            ),
            {
                "id": uuid4(),
                "event_id": event.event_id,
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": aggregate_type,
                "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                "event_data": json_dumps(outbox_event_data(event)),
                "created_at": datetime.now(UTC),
            },
        )
