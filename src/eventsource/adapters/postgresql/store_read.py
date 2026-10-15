"""Read operations mixin for PostgreSQLEventStore."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.serialization import json_loads
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports import (
    CategoryReadOptions,
    EventEnvelope,
    FeedReadOptions,
    Position,
    ReadDirection,
    StreamReadOptions,
)

_SELECT_COLUMNS = (
    "global_position, event_id, event_type, aggregate_type, aggregate_id, "
    "tenant_id, actor_id, version, timestamp, payload, created_at"
)
_HORIZON_PREDICATE = "(txid IS NULL OR txid < CAST(:txid_horizon AS text)::xid8)"
_HORIZON_QUERY = "SELECT eventsource_feed_horizon()"


class PostgreSQLEventStoreReadMixin:
    """Mixin providing stream, category, and feed read operations for PostgreSQLEventStore."""

    _session_factory: async_sessionmaker[AsyncSession]
    _codec: IntPositionCodec
    _event_registry: EventRegistry

    async def _ensure_schema(self) -> None:
        """Ensures schema is initialized (implemented in PostgreSQLEventStore)."""

    def read_stream(self, stream: StreamId, options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        return self._do_read_stream(stream, options or StreamReadOptions())

    async def _do_read_stream(self, stream: StreamId, options: StreamReadOptions) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        await self._ensure_schema()
        query_parts = [
            f"SELECT {_SELECT_COLUMNS} FROM events"  # nosec B608 -- constant column list
            " WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
        ]
        params: dict[str, Any] = {"aggregate_id": stream.aggregate_id, "aggregate_type": stream.category}  # fmt: skip
        if options.from_version is not None:
            query_parts.append("AND version >= :from_version")
            params["from_version"] = options.from_version
        if options.to_version is not None:
            query_parts.append("AND version <= :to_version")
            params["to_version"] = options.to_version
        order = "DESC" if options.direction == ReadDirection.BACKWARD else "ASC"
        query_parts.append(f"ORDER BY version {order}")
        if options.limit is not None:
            query_parts.append("LIMIT :limit")
            params["limit"] = options.limit
        async with self._session_factory() as session:
            rows = (await session.execute(text("\n".join(query_parts)), params)).mappings().all()
        for row in rows:
            yield self._row_to_envelope(row)

    async def get_stream_version(self, stream: StreamId) -> int:
        await self._ensure_schema()
        sql = "SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"  # fmt: skip
        params = {"aggregate_id": stream.aggregate_id, "aggregate_type": stream.category}
        async with self._session_factory() as session:
            return (await session.execute(text(sql), params)).scalar() or 0

    async def event_exists(self, event_id: UUID) -> bool:
        await self._ensure_schema()
        sql = "SELECT 1 FROM events WHERE event_id = :event_id LIMIT 1"
        async with self._session_factory() as session:
            return (await session.execute(text(sql), {"event_id": event_id})).first() is not None

    def read_all(self, from_position: Position | None = None, options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        return self._do_read_all(from_position, options or FeedReadOptions())

    async def _do_read_all(self, from_position: Position | None, options: FeedReadOptions) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        await self._ensure_schema()
        query_parts = [f"SELECT {_SELECT_COLUMNS} FROM events WHERE {_HORIZON_PREDICATE}"]  # nosec B608  # fmt: skip
        params: dict[str, Any] = {}
        if from_position is not None:
            query_parts.append("AND global_position > :from_position")
            params["from_position"] = self._codec.value_of(from_position)
        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = :tenant_id")
            params["tenant_id"] = options.tenant_id
        if options.aggregate_type is not None:
            query_parts.append("AND aggregate_type = :aggregate_type")
            params["aggregate_type"] = options.aggregate_type
        query_parts.append("ORDER BY global_position ASC")
        if options.limit is not None:
            query_parts.append("LIMIT :limit")
            params["limit"] = options.limit
        async with self._session_factory() as session:
            params["txid_horizon"] = (await session.execute(text(_HORIZON_QUERY))).scalar_one()
            rows = (await session.execute(text("\n".join(query_parts)), params)).mappings().all()
        for row in rows:
            yield self._row_to_envelope(row)

    async def current_position(self) -> Position | None:
        await self._ensure_schema()
        sql = f"SELECT MAX(global_position) FROM events WHERE {_HORIZON_PREDICATE}"  # nosec B608
        async with self._session_factory() as session:
            horizon = (await session.execute(text(_HORIZON_QUERY))).scalar_one()
            value = (await session.execute(text(sql), {"txid_horizon": horizon})).scalar()
        return None if value is None else self._codec.encode(value)

    def read_category(self, category: str, options: CategoryReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        return self._do_read_category(category, options or CategoryReadOptions())

    async def _do_read_category(self, category: str, options: CategoryReadOptions) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        await self._ensure_schema()
        query_parts = [f"SELECT {_SELECT_COLUMNS} FROM events WHERE aggregate_type = :aggregate_type"]  # nosec B608  # fmt: skip
        params: dict[str, Any] = {"aggregate_type": category}
        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = :tenant_id")
            params["tenant_id"] = options.tenant_id
        if options.from_timestamp is not None:
            query_parts.append("AND created_at >= :from_timestamp")
            params["from_timestamp"] = options.from_timestamp
        query_parts.append("ORDER BY created_at ASC, global_position ASC")
        if options.limit is not None:
            query_parts.append("LIMIT :limit")
            params["limit"] = options.limit
        async with self._session_factory() as session:
            rows = (await session.execute(text("\n".join(query_parts)), params)).mappings().all()
        for row in rows:
            yield self._row_to_envelope(row)

    def _row_to_envelope(self, row: Any) -> EventEnvelope:
        ev = self._deserialize_event(row["event_type"], row["payload"])
        sid = StreamId(aggregate_id=row["aggregate_id"], category=row["aggregate_type"])
        return EventEnvelope(ev, sid, row["version"], self._codec.encode(row["global_position"]), row["created_at"])  # fmt: skip

    def _deserialize_event(self, event_type: str, payload: Any) -> DomainEvent:
        event_class = self._event_registry.get(event_type)
        return event_class.model_validate(payload if isinstance(payload, dict) else json_loads(payload))  # fmt: skip
