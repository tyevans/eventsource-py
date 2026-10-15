"""PostgreSQL snapshot store implementation using async SQLAlchemy sessions."""

from __future__ import annotations

import logging
from typing import cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters.serialization import json_dumps, json_loads
from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)
from eventsource.ports.snapshots import Snapshot

logger = logging.getLogger(__name__)

_UPSERT_SQL = text(
    "INSERT INTO snapshots (aggregate_id, aggregate_type, version, schema_version, state, created_at) "
    "VALUES (:aggregate_id, :aggregate_type, :version, :schema_version, :state, :created_at) "
    "ON CONFLICT (aggregate_id, aggregate_type) DO UPDATE SET "
    "version = EXCLUDED.version, schema_version = EXCLUDED.schema_version, "
    "state = EXCLUDED.state, created_at = EXCLUDED.created_at"
)
_SELECT_SQL = text(
    "SELECT aggregate_id, aggregate_type, version, schema_version, state, created_at "
    "FROM snapshots WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
)
_DELETE_SQL = text(
    "DELETE FROM snapshots WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
)
_EXISTS_SQL = text(
    "SELECT EXISTS (SELECT 1 FROM snapshots "
    "WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type)"
)


class PostgreSQLSnapshotStore:
    """PostgreSQL implementation of `SnapshotStore` using async SQLAlchemy sessions."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._session_factory = session_factory
        logger.debug("PostgreSQLSnapshotStore initialized")

    async def close(self) -> None: return None  # fmt: skip

    async def __aenter__(self) -> PostgreSQLSnapshotStore: return self  # fmt: skip

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None: await self.close()  # fmt: skip

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        attrs = {ATTR_AGGREGATE_ID: str(snapshot.aggregate_id), ATTR_AGGREGATE_TYPE: snapshot.aggregate_type, ATTR_VERSION: snapshot.version}  # fmt: skip
        with self._tracer.span("eventsource.snapshot.save", attrs):
            params = {
                "aggregate_id": snapshot.aggregate_id,
                "aggregate_type": snapshot.aggregate_type,
                "version": snapshot.version,
                "schema_version": snapshot.schema_version,
                "state": json_dumps(snapshot.state),
                "created_at": snapshot.created_at,
            }
            async with self._session_factory() as session, session.begin():
                await session.execute(_UPSERT_SQL, params)
            logger.debug("Saved snapshot for %s/%s at version %d", snapshot.aggregate_type, snapshot.aggregate_id, snapshot.version)  # fmt: skip

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.get", attrs):
            params = {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type}
            async with self._session_factory() as session:
                row = (await session.execute(_SELECT_SQL, params)).fetchone()
            if row is None:
                logger.debug("No snapshot found for %s/%s", aggregate_type, aggregate_id)
                return None
            state = row.state
            if isinstance(state, str):
                try:
                    state = json_loads(state)
                except (ValueError, TypeError) as e:
                    raise SnapshotDeserializationError(row.aggregate_id, row.aggregate_type, e) from e  # fmt: skip
            snapshot = Snapshot(row.aggregate_id, row.aggregate_type, row.version, state, row.schema_version, row.created_at)  # fmt: skip
            logger.debug("Retrieved snapshot for %s/%s at version %d", aggregate_type, aggregate_id, snapshot.version)  # fmt: skip
            return snapshot

    async def delete_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.delete", attrs):
            params = {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type}
            async with self._session_factory() as session, session.begin():
                result = await session.execute(_DELETE_SQL, params)
                deleted = cast(int, result.rowcount) > 0  # type: ignore[attr-defined]
            logger.debug("%s snapshot for %s/%s", "Deleted" if deleted else "No", aggregate_type, aggregate_id)  # fmt: skip
            return deleted

    async def snapshot_exists(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.exists", attrs):
            params = {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type}
            async with self._session_factory() as session:
                result = await session.execute(_EXISTS_SQL, params)
                return bool(result.scalar() or False)

    async def delete_snapshots_by_type(self, aggregate_type: str, schema_version_below: int | None = None) -> int:  # fmt: skip
        attrs = {ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.delete_by_type", attrs):
            sql = "DELETE FROM snapshots WHERE aggregate_type = :aggregate_type"
            params: dict[str, object] = {"aggregate_type": aggregate_type}
            if schema_version_below is not None:
                sql += " AND schema_version < :schema_version_below"
                params["schema_version_below"] = schema_version_below
            async with self._session_factory() as session, session.begin():
                result = await session.execute(text(sql), params)
                count = cast(int, result.rowcount)  # type: ignore[attr-defined]
            if count > 0:
                sfx = f" (schema_version < {schema_version_below})" if schema_version_below else ""
                logger.info("Deleted %d snapshots for aggregate type %s%s", count, aggregate_type, sfx)  # fmt: skip
            return count

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]: return self._session_factory  # fmt: skip

    def __repr__(self) -> str:
        return f"PostgreSQLSnapshotStore(tracing={'enabled' if self._enable_tracing else 'disabled'})"  # fmt: skip
