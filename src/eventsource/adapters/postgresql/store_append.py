"""
Append operations mixin for PostgreSQLEventStore.

Provides append, outbox writing, and integrity error classification.
"""

from __future__ import annotations

import json
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

# Constraint names from the canonical `adapters/sql/schemas/schemas/events.sql` (verified
# against a live PostgreSQL 15 by introspecting `asyncpg.exceptions
# .UniqueViolationError.constraint_name` on both conflict paths -- see
# `_classify_integrity_error`).
_EVENT_ID_UNIQUE_CONSTRAINT = "events_event_id_key"
_AGGREGATE_VERSION_UNIQUE_CONSTRAINT = "uq_events_aggregate_version"


class PostgreSQLEventStoreAppendMixin:
    """Mixin providing append and outbox operations for PostgreSQLEventStore."""

    _session_factory: async_sessionmaker[AsyncSession]
    _codec: IntPositionCodec
    _outbox_enabled: bool

    async def _ensure_schema(self) -> None:
        """Ensures schema is initialized (implemented in PostgreSQLEventStore)."""
        pass

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
                        stream.aggregate_id, describe_expected(expected), actual_version
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
