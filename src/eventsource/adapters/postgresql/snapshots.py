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


class PostgreSQLSnapshotStore:
    """PostgreSQL implementation of `SnapshotStore` using async SQLAlchemy sessions."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the PostgreSQL snapshot store."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._session_factory = session_factory
        logger.debug("PostgreSQLSnapshotStore initialized")

    async def close(self) -> None:
        """No-op resource release: `session_factory` is caller-supplied and owned."""
        return None

    async def __aenter__(self) -> PostgreSQLSnapshotStore:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save or update a snapshot using INSERT ... ON CONFLICT DO UPDATE."""
        with self._tracer.span(
            "eventsource.snapshot.save",
            {
                ATTR_AGGREGATE_ID: str(snapshot.aggregate_id),
                ATTR_AGGREGATE_TYPE: snapshot.aggregate_type,
                ATTR_VERSION: snapshot.version,
            },
        ):
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
            logger.debug(
                "Saved snapshot for %s/%s at version %d",
                snapshot.aggregate_type,
                snapshot.aggregate_id,
                snapshot.version,
            )

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        """Get the snapshot for an aggregate, or None if not found."""
        with self._tracer.span(
            "eventsource.snapshot.get",
            {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type},
        ):
            async with self._session_factory() as session:
                result = await session.execute(
                    text(
                        "SELECT aggregate_id, aggregate_type, version, schema_version, state, created_at "
                        "FROM snapshots WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
                    ),
                    {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
                )
                row = result.fetchone()

            if row is None:
                logger.debug("No snapshot found for %s/%s", aggregate_type, aggregate_id)
                return None

            state = row.state
            if isinstance(state, str):
                try:
                    state = json_loads(state)
                except (ValueError, TypeError) as e:
                    raise SnapshotDeserializationError(
                        row.aggregate_id, row.aggregate_type, e
                    ) from e

            snapshot = Snapshot(
                aggregate_id=row.aggregate_id,
                aggregate_type=row.aggregate_type,
                version=row.version,
                schema_version=row.schema_version,
                state=state,
                created_at=row.created_at,
            )
            logger.debug(
                "Retrieved snapshot for %s/%s at version %d",
                aggregate_type,
                aggregate_id,
                snapshot.version,
            )
            return snapshot

    async def delete_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Delete the snapshot for an aggregate; returns True if deleted."""
        with self._tracer.span(
            "eventsource.snapshot.delete",
            {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type},
        ):
            async with self._session_factory() as session, session.begin():
                result = await session.execute(
                    text(
                        "DELETE FROM snapshots "
                        "WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type"
                    ),
                    {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
                )
                deleted = cast(int, result.rowcount) > 0  # type: ignore[attr-defined]

            logger.debug(
                "%s snapshot for %s/%s",
                "Deleted" if deleted else "No",
                aggregate_type,
                aggregate_id,
            )
            return deleted

    async def snapshot_exists(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Check if a snapshot exists using EXISTS."""
        with self._tracer.span(
            "eventsource.snapshot.exists",
            {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type},
        ):
            async with self._session_factory() as session:
                result = await session.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM snapshots "
                        "WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type)"
                    ),
                    {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
                )
                return bool(result.scalar() or False)

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """Delete all snapshots for a given aggregate type."""
        with self._tracer.span(
            "eventsource.snapshot.delete_by_type", {ATTR_AGGREGATE_TYPE: aggregate_type}
        ):
            sql = "DELETE FROM snapshots WHERE aggregate_type = :aggregate_type"
            params: dict[str, object] = {"aggregate_type": aggregate_type}
            if schema_version_below is not None:
                sql += " AND schema_version < :schema_version_below"
                params["schema_version_below"] = schema_version_below
            async with self._session_factory() as session, session.begin():
                result = await session.execute(text(sql), params)
                count = cast(int, result.rowcount)  # type: ignore[attr-defined]

            if count > 0:
                suffix = (
                    f" (schema_version < {schema_version_below})" if schema_version_below else ""
                )
                logger.info(
                    "Deleted %d snapshots for aggregate type %s%s", count, aggregate_type, suffix
                )
            return count

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Get the session factory for external use (e.g., transactions)."""
        return self._session_factory

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"PostgreSQLSnapshotStore(tracing={'enabled' if self._enable_tracing else 'disabled'})"
        )
