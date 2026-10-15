"""SQLAlchemy checkpoint adapter, serving both PostgreSQL and SQLite."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters._sql.dialect import (
    Dialect,
    dialect_of,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_EVENT_TYPE, ATTR_PROJECTION_NAME
from eventsource.ports.checkpoints import CheckpointData, LagMetrics
from eventsource.ports.positions import Position


class SQLCheckpointRepository:
    """SQLAlchemy-backed checkpoint repository serving both PostgreSQL and SQLite."""

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """Initialize the checkpoint repository."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """Get the last processed event ID for a projection, or None if none exists."""
        with self._tracer.span(
            "eventsource.checkpoint.get_checkpoint", {ATTR_PROJECTION_NAME: projection_name}
        ):
            query = text(
                "SELECT last_event_id FROM projection_checkpoints "
                "WHERE projection_name = :projection_name"
            )
            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"projection_name": projection_name})
                row = result.fetchone()
                return uuid_result(row[0]) if row else None

    async def update_checkpoint(
        self, projection_name: str, event_id: UUID, event_type: str
    ) -> None:
        """Update the checkpoint for a projection using an idempotent UPSERT."""
        with self._tracer.span(
            "eventsource.checkpoint.update_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name, ATTR_EVENT_TYPE: event_type},
        ):
            now = datetime.now(UTC)
            async with sql_connection(self._conn, write=True) as conn:
                dialect = dialect_of(conn)
                query = text("""
                    INSERT INTO projection_checkpoints
                        (projection_name, last_event_id, last_event_type,
                         last_processed_at, events_processed, created_at, updated_at)
                    VALUES (:projection_name, :event_id, :event_type, :now, 1, :now, :now)
                    ON CONFLICT (projection_name) DO UPDATE
                    SET last_event_id = EXCLUDED.last_event_id,
                        last_event_type = EXCLUDED.last_event_type,
                        last_processed_at = EXCLUDED.last_processed_at,
                        events_processed = projection_checkpoints.events_processed + 1,
                        updated_at = EXCLUDED.updated_at
                """)
                await conn.execute(
                    query,
                    {
                        "projection_name": projection_name,
                        "event_id": uuid_param(event_id, dialect),
                        "event_type": event_type,
                        "now": ts_param(now, dialect),
                    },
                )

    async def get_lag_metrics(
        self, projection_name: str, event_types: list[str] | None = None
    ) -> LagMetrics | None:
        """Get lag metrics for a projection, or None if no checkpoint exists."""
        with self._tracer.span(
            "eventsource.checkpoint.get_lag_metrics", {ATTR_PROJECTION_NAME: projection_name}
        ):
            event_types = event_types or []
            async with sql_connection(self._conn, write=False) as conn:
                dialect = dialect_of(conn)
                params: dict[str, object] = {"projection_name": projection_name}
                if dialect is Dialect.POSTGRESQL and event_types:
                    event_filter = "WHERE event_type = ANY(:event_types)"
                    params["event_types"] = event_types
                elif event_types:
                    placeholders = ", ".join(f":et{i}" for i in range(len(event_types)))
                    event_filter = f"WHERE event_type IN ({placeholders})"
                    params.update({f"et{i}": et for i, et in enumerate(event_types)})
                else:
                    event_filter = ""

                query = text(
                    f"""
                    WITH latest_relevant_event AS (
                        SELECT event_id as max_id, timestamp as max_time
                        FROM events {event_filter}
                        ORDER BY timestamp DESC LIMIT 1
                    )
                    SELECT pc.projection_name, pc.last_event_id, le.max_id as latest_event_id,
                           le.max_time as latest_event_time, pc.events_processed, pc.last_processed_at
                    FROM projection_checkpoints pc
                    LEFT JOIN latest_relevant_event le ON 1 = 1
                    WHERE pc.projection_name = :projection_name
                """  # nosec B608 -- only generated placeholders are interpolated
                )
                result = await conn.execute(query, params)
                row = result.fetchone()

            if not row:
                return None

            last_event_id = str(uuid_result(row[1])) if row[1] is not None else None
            latest_event_id = str(uuid_result(row[2])) if row[2] is not None else None
            last_processed_at = ts_result(row[5])
            latest_event_time = ts_result(row[3])
            raw_lag = (
                (latest_event_time - last_processed_at).total_seconds()
                if last_processed_at is not None and latest_event_time is not None
                else 0.0
            )
            lag_seconds = (
                0.0
                if (last_event_id and last_event_id == latest_event_id) or raw_lag < 0
                else round(raw_lag, 1)
            )
            return LagMetrics(
                projection_name=row[0],
                last_event_id=last_event_id,
                latest_event_id=latest_event_id,
                lag_seconds=lag_seconds,
                events_processed=row[4] or 0,
                last_processed_at=last_processed_at.isoformat() if last_processed_at else None,
            )

    async def reset_checkpoint(self, projection_name: str) -> None:
        """Reset the checkpoint for a projection."""
        with self._tracer.span(
            "eventsource.checkpoint.reset_checkpoint", {ATTR_PROJECTION_NAME: projection_name}
        ):
            query = text(
                "DELETE FROM projection_checkpoints WHERE projection_name = :projection_name"
            )
            async with sql_connection(self._conn, write=True) as conn:
                await conn.execute(query, {"projection_name": projection_name})

    async def get_position(self, subscription_id: str) -> Position | None:
        """Get last processed global-feed position for a subscription, or None if unset."""
        with self._tracer.span(
            "eventsource.checkpoint.get_position", {ATTR_PROJECTION_NAME: subscription_id}
        ):
            query = text(
                "SELECT position_token FROM projection_checkpoints "
                "WHERE projection_name = :subscription_id"
            )
            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"subscription_id": subscription_id})
                row = result.fetchone()
                return Position.from_str(row[0]) if row and row[0] is not None else None

    async def save_position(
        self, subscription_id: str, position: Position, event_id: UUID, event_type: str
    ) -> None:
        """Save checkpoint with an opaque global-feed position using an idempotent UPSERT."""
        with self._tracer.span(
            "eventsource.checkpoint.save_position",
            {
                ATTR_PROJECTION_NAME: subscription_id,
                ATTR_EVENT_TYPE: event_type,
                "position_token": position.to_str(),
            },
        ):
            now = datetime.now(UTC)
            async with sql_connection(self._conn, write=True) as conn:
                dialect = dialect_of(conn)
                query = text("""
                    INSERT INTO projection_checkpoints
                        (projection_name, last_event_id, last_event_type,
                         last_processed_at, events_processed, position_token,
                         created_at, updated_at)
                    VALUES
                        (:subscription_id, :event_id, :event_type, :now,
                         1, :position_token, :now, :now)
                    ON CONFLICT (projection_name) DO UPDATE
                    SET last_event_id = EXCLUDED.last_event_id,
                        last_event_type = EXCLUDED.last_event_type,
                        last_processed_at = EXCLUDED.last_processed_at,
                        events_processed = projection_checkpoints.events_processed + 1,
                        position_token = EXCLUDED.position_token,
                        updated_at = EXCLUDED.updated_at
                """)
                await conn.execute(
                    query,
                    {
                        "subscription_id": subscription_id,
                        "event_id": uuid_param(event_id, dialect),
                        "event_type": event_type,
                        "now": ts_param(now, dialect),
                        "position_token": position.to_str(),
                    },
                )

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """Get all projection checkpoints ordered by projection_name."""
        with self._tracer.span("eventsource.checkpoint.get_all_checkpoints", {}):
            query = text("""
                SELECT projection_name, last_event_id, last_event_type,
                       last_processed_at, events_processed, position_token
                FROM projection_checkpoints ORDER BY projection_name
            """)
            async with sql_connection(self._conn, write=False) as conn:
                rows = (await conn.execute(query)).fetchall()

            return [
                CheckpointData(
                    projection_name=row[0],
                    last_event_id=uuid_result(row[1]),
                    last_event_type=row[2],
                    last_processed_at=ts_result(row[3]),
                    events_processed=row[4] or 0,
                    position=Position.from_str(row[5]) if row[5] else None,
                )
                for row in rows
            ]


__all__ = ["SQLCheckpointRepository"]
