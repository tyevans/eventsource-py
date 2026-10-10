"""Query builders and row conversion helpers for SQLiteEventStore.

Extracted from ``store.py`` to keep the main event store module within
the line limits prescribed by ADR-0002.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.adapters.serialization import json_loads
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import (
    CategoryReadOptions,
    EventEnvelope,
    FeedReadOptions,
    ReadDirection,
    StreamReadOptions,
)

if TYPE_CHECKING:
    import aiosqlite

    from eventsource.adapters._sql.positions import IntPositionCodec
    from eventsource.domain.event_registry import EventRegistry

SELECT_COLUMNS = """
    global_position, event_id, event_type, aggregate_type, aggregate_id,
    tenant_id, actor_id, version, timestamp, payload, created_at
"""


def build_read_stream_query(
    stream: StreamId,
    options: StreamReadOptions,
) -> tuple[str, list[Any]]:
    """Build SQL query and parameter list for reading a stream."""
    query_parts = [
        f"SELECT {SELECT_COLUMNS} FROM events"  # nosec B608
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

    return "\n".join(query_parts), params


def build_read_all_query(
    from_position_val: int | None,
    options: FeedReadOptions,
) -> tuple[str, list[Any]]:
    """Build SQL query and parameter list for reading all events."""
    query_parts = [
        f"SELECT {SELECT_COLUMNS} FROM events WHERE 1=1"  # nosec B608
    ]
    params: list[Any] = []

    if from_position_val is not None:
        query_parts.append("AND global_position > ?")
        params.append(from_position_val)

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

    return "\n".join(query_parts), params


def build_read_category_query(
    category: str,
    options: CategoryReadOptions,
) -> tuple[str, list[Any]]:
    """Build SQL query and parameter list for reading a category feed."""
    query_parts = [
        f"SELECT {SELECT_COLUMNS} FROM events"  # nosec B608
        " WHERE aggregate_type = ?"
    ]
    params: list[Any] = [category]

    if options.tenant_id is not None:
        query_parts.append("AND tenant_id = ?")
        params.append(str(options.tenant_id))

    if options.from_timestamp is not None:
        bound = options.from_timestamp
        bound = bound.replace(tzinfo=UTC) if bound.tzinfo is None else bound.astimezone(UTC)
        query_parts.append("AND created_at >= ?")
        params.append(bound.isoformat())

    query_parts.append("ORDER BY created_at ASC, global_position ASC")

    if options.limit is not None:
        query_parts.append("LIMIT ?")
        params.append(options.limit)

    return "\n".join(query_parts), params


def deserialize_event(
    event_type: str,
    payload: str,
    event_registry: EventRegistry,
) -> DomainEvent:
    """Deserialize JSON payload into a registered DomainEvent instance."""
    event_class = event_registry.get(event_type)
    data = json_loads(payload)
    return event_class.model_validate(data)


def row_to_envelope(
    row: Any,
    codec: IntPositionCodec,
    event_registry: EventRegistry,
) -> EventEnvelope:
    """Convert an aiosqlite.Row into an EventEnvelope."""
    event = deserialize_event(row["event_type"], row["payload"], event_registry)
    stream_id = StreamId(
        aggregate_id=UUID(row["aggregate_id"]),
        category=row["aggregate_type"],
    )
    stored_at = datetime.fromisoformat(row["created_at"]).replace(tzinfo=UTC)
    return EventEnvelope(
        event=event,
        stream_id=stream_id,
        stream_version=row["version"],
        position=codec.encode(row["global_position"]),
        stored_at=stored_at,
    )


async def apply_additive_updates(conn: aiosqlite.Connection) -> None:
    """Apply additive schema fragments SQLite cannot express idempotently."""
    async with conn.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    if "position_token" not in columns:
        await conn.execute("ALTER TABLE projection_checkpoints ADD COLUMN position_token TEXT")
