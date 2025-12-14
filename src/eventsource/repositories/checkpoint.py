"""
Checkpoint repository for tracking projection positions.

Projections use checkpoints to track which events they have processed,
enabling:
- Resumable processing after restarts
- Lag monitoring and health checks
- Safe rebuilds from specific positions
"""

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.repositories._connection import execute_with_connection

if TYPE_CHECKING:
    import aiosqlite


@dataclass(frozen=True)
class CheckpointData:
    """
    Data structure for checkpoint information.

    Attributes:
        projection_name: Name of the projection
        last_event_id: Last processed event ID
        last_event_type: Type of the last processed event
        last_processed_at: When the last event was processed
        events_processed: Total count of events processed
        global_position: Last processed global position in the event stream
    """

    projection_name: str
    last_event_id: UUID | None = None
    last_event_type: str | None = None
    last_processed_at: datetime | None = None
    events_processed: int = 0
    global_position: int | None = None


@dataclass(frozen=True)
class LagMetrics:
    """
    Data structure for projection lag metrics.

    Attributes:
        projection_name: Name of the projection
        last_event_id: Last event ID processed by the projection
        latest_event_id: Latest relevant event ID in the event store
        lag_seconds: Time lag in seconds (0 if up to date)
        events_processed: Total events processed by this projection
        last_processed_at: When the projection last processed an event
    """

    projection_name: str
    last_event_id: str | None = None
    latest_event_id: str | None = None
    lag_seconds: float = 0.0
    events_processed: int = 0
    last_processed_at: str | None = None


@runtime_checkable
class CheckpointRepository(Protocol):
    """
    Protocol for checkpoint repositories.

    Checkpoint repositories track which events have been processed by
    each projection, enabling resumable processing and lag monitoring.
    """

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        ...

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Update the checkpoint for a projection.

        Uses UPSERT pattern for idempotency - safe to call multiple times.

        Args:
            projection_name: Name of the projection
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        ...

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """
        Get lag metrics for a projection.

        Compares the checkpoint against the latest relevant events to
        determine how far behind the projection is.

        Args:
            projection_name: Name of the projection
            event_types: List of event types this projection handles.
                        Used to filter relevant events for lag calculation.

        Returns:
            LagMetrics if checkpoint exists, None otherwise
        """
        ...

    async def reset_checkpoint(self, projection_name: str) -> None:
        """
        Reset the checkpoint for a projection.

        Used when rebuilding a projection from scratch.

        Args:
            projection_name: Name of the projection
        """
        ...

    async def get_position(self, subscription_id: str) -> int | None:
        """
        Get last processed global position for a subscription.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)

        Returns:
            Last processed global position, or None if no checkpoint exists
            or if checkpoint doesn't have position data.
        """
        ...

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Save checkpoint with global position.

        Updates the position, event_id, and event_type for the checkpoint.
        Uses UPSERT pattern for idempotency.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)
            position: Global position of the event
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        ...

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        ...


class PostgreSQLCheckpointRepository:
    """
    PostgreSQL implementation of checkpoint repository.

    Stores checkpoints in the `projection_checkpoints` table.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLCheckpointRepository(conn)
        ...     await repo.update_checkpoint(
        ...         "MyProjection",
        ...         event.event_id,
        ...         event.event_type,
        ...     )
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the checkpoint repository.

        Args:
            conn: Database connection or engine
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self.conn = conn

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            query = text("""
                SELECT last_event_id
                FROM projection_checkpoints
                WHERE projection_name = :projection_name
            """)
            params = {"projection_name": projection_name}

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()
                return row[0] if row else None

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Update the checkpoint for a projection.

        Uses UPSERT pattern for idempotency.

        Args:
            projection_name: Name of the projection
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.update_checkpoint",
            {
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_EVENT_TYPE: event_type,
            },
        ):
            now = datetime.now(UTC)
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
            params = {
                "projection_name": projection_name,
                "event_id": event_id,
                "event_type": event_type,
                "now": now,
            }

            async with execute_with_connection(self.conn, transactional=True) as conn:
                await conn.execute(query, params)

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """
        Get lag metrics for a projection.

        Args:
            projection_name: Name of the projection
            event_types: List of event types this projection handles

        Returns:
            LagMetrics if checkpoint exists, None otherwise
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_lag_metrics",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            # Default to empty list if no event types provided
            if event_types is None:
                event_types = []

            query = text("""
                WITH latest_relevant_event AS (
                    SELECT event_id as max_id, timestamp as max_time
                    FROM events
                    WHERE event_type = ANY(:event_types)
                    ORDER BY timestamp DESC
                    LIMIT 1
                )
                SELECT
                    pc.projection_name,
                    pc.last_event_id,
                    le.max_id as latest_event_id,
                    EXTRACT(EPOCH FROM (le.max_time - pc.last_processed_at)) as lag_seconds,
                    pc.events_processed,
                    pc.last_processed_at
                FROM projection_checkpoints pc
                LEFT JOIN latest_relevant_event le ON true
                WHERE pc.projection_name = :projection_name
            """)
            params = {"projection_name": projection_name, "event_types": event_types}

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()

            if not row:
                return None

            # Extract values
            last_event_id = str(row[1]) if row[1] else None
            latest_event_id = str(row[2]) if row[2] else None
            raw_lag = float(row[3]) if row[3] else 0.0

            # Calculate actual lag
            # If last_event_id matches latest_event_id, projection is up-to-date
            if (
                last_event_id
                and latest_event_id
                and last_event_id == latest_event_id
                or raw_lag < 0
            ):
                lag_seconds = 0.0
            else:
                lag_seconds = round(raw_lag, 1)

            return LagMetrics(
                projection_name=row[0],
                last_event_id=last_event_id,
                latest_event_id=latest_event_id,
                lag_seconds=lag_seconds,
                events_processed=row[4] or 0,
                last_processed_at=row[5].isoformat() if row[5] else None,
            )

    async def reset_checkpoint(self, projection_name: str) -> None:
        """
        Reset the checkpoint for a projection.

        Args:
            projection_name: Name of the projection
        """
        with self._tracer.span(
            "eventsource.checkpoint.reset_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            query = text("""
                DELETE FROM projection_checkpoints
                WHERE projection_name = :projection_name
            """)
            params = {"projection_name": projection_name}

            async with execute_with_connection(self.conn, transactional=True) as conn:
                await conn.execute(query, params)

    async def get_position(self, subscription_id: str) -> int | None:
        """
        Get last processed global position for a subscription.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)

        Returns:
            Last processed global position, or None if no checkpoint exists
            or if checkpoint doesn't have position data.
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_position",
            {ATTR_PROJECTION_NAME: subscription_id},
        ):
            query = text("""
                SELECT global_position
                FROM projection_checkpoints
                WHERE projection_name = :subscription_id
            """)
            params = {"subscription_id": subscription_id}

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()
                return row[0] if row and row[0] is not None else None

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Save checkpoint with global position.

        Updates the position, event_id, and event_type for the checkpoint.
        Uses UPSERT pattern for idempotency.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)
            position: Global position of the event
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.save_position",
            {
                ATTR_PROJECTION_NAME: subscription_id,
                ATTR_EVENT_TYPE: event_type,
                "global_position": position,
            },
        ):
            now = datetime.now(UTC)
            query = text("""
                INSERT INTO projection_checkpoints
                    (projection_name, last_event_id, last_event_type,
                     last_processed_at, events_processed, global_position,
                     created_at, updated_at)
                VALUES
                    (:subscription_id, :event_id, :event_type, :now,
                     1, :position, :now, :now)
                ON CONFLICT (projection_name) DO UPDATE
                SET last_event_id = EXCLUDED.last_event_id,
                    last_event_type = EXCLUDED.last_event_type,
                    last_processed_at = EXCLUDED.last_processed_at,
                    events_processed = projection_checkpoints.events_processed + 1,
                    global_position = EXCLUDED.global_position,
                    updated_at = EXCLUDED.updated_at
            """)
            params = {
                "subscription_id": subscription_id,
                "event_id": event_id,
                "event_type": event_type,
                "now": now,
                "position": position,
            }

            async with execute_with_connection(self.conn, transactional=True) as conn:
                await conn.execute(query, params)

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_all_checkpoints",
            {},
        ):
            query = text("""
                SELECT projection_name, last_event_id, last_event_type,
                       last_processed_at, events_processed, global_position
                FROM projection_checkpoints
                ORDER BY projection_name
            """)

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query)
                rows = result.fetchall()

            return [
                CheckpointData(
                    projection_name=row[0],
                    last_event_id=row[1],
                    last_event_type=row[2],
                    last_processed_at=row[3],
                    events_processed=row[4] or 0,
                    global_position=row[5],
                )
                for row in rows
            ]


class InMemoryCheckpointRepository:
    """
    In-memory implementation of checkpoint repository for testing.

    Stores checkpoints in memory. All data is lost when the process terminates.

    Example:
        >>> repo = InMemoryCheckpointRepository()
        >>> await repo.update_checkpoint("MyProjection", event_id, "EventType")
        >>> checkpoint = await repo.get_checkpoint("MyProjection")
    """

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize an empty in-memory checkpoint repository.

        Args:
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._checkpoints: dict[str, CheckpointData] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            async with self._lock:
                checkpoint = self._checkpoints.get(projection_name)
                return checkpoint.last_event_id if checkpoint else None

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Update the checkpoint for a projection.

        Args:
            projection_name: Name of the projection
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.update_checkpoint",
            {
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_EVENT_TYPE: event_type,
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                existing = self._checkpoints.get(projection_name)
                events_processed = (existing.events_processed + 1) if existing else 1

                self._checkpoints[projection_name] = CheckpointData(
                    projection_name=projection_name,
                    last_event_id=event_id,
                    last_event_type=event_type,
                    last_processed_at=now,
                    events_processed=events_processed,
                )

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """
        Get lag metrics for a projection.

        Note: In-memory implementation cannot calculate real lag against
        an event store. Returns placeholder metrics based on checkpoint data.

        Args:
            projection_name: Name of the projection
            event_types: List of event types (ignored in in-memory impl)

        Returns:
            LagMetrics if checkpoint exists, None otherwise
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_lag_metrics",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            async with self._lock:
                checkpoint = self._checkpoints.get(projection_name)
                if not checkpoint:
                    return None

                return LagMetrics(
                    projection_name=checkpoint.projection_name,
                    last_event_id=str(checkpoint.last_event_id)
                    if checkpoint.last_event_id
                    else None,
                    latest_event_id=None,  # Cannot determine without event store
                    lag_seconds=0.0,  # Cannot calculate without event store
                    events_processed=checkpoint.events_processed,
                    last_processed_at=(
                        checkpoint.last_processed_at.isoformat()
                        if checkpoint.last_processed_at
                        else None
                    ),
                )

    async def reset_checkpoint(self, projection_name: str) -> None:
        """
        Reset the checkpoint for a projection.

        Args:
            projection_name: Name of the projection
        """
        with self._tracer.span(
            "eventsource.checkpoint.reset_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            async with self._lock:
                self._checkpoints.pop(projection_name, None)

    async def get_position(self, subscription_id: str) -> int | None:
        """
        Get last processed global position for a subscription.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)

        Returns:
            Last processed global position, or None if no checkpoint exists
            or if checkpoint doesn't have position data.
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_position",
            {ATTR_PROJECTION_NAME: subscription_id},
        ):
            async with self._lock:
                checkpoint = self._checkpoints.get(subscription_id)
                return checkpoint.global_position if checkpoint else None

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Save checkpoint with global position.

        Updates the position, event_id, and event_type for the checkpoint.
        Uses UPSERT pattern for idempotency.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)
            position: Global position of the event
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.save_position",
            {
                ATTR_PROJECTION_NAME: subscription_id,
                ATTR_EVENT_TYPE: event_type,
                "global_position": position,
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                existing = self._checkpoints.get(subscription_id)
                events_processed = (existing.events_processed + 1) if existing else 1

                self._checkpoints[subscription_id] = CheckpointData(
                    projection_name=subscription_id,
                    last_event_id=event_id,
                    last_event_type=event_type,
                    last_processed_at=now,
                    events_processed=events_processed,
                    global_position=position,
                )

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_all_checkpoints",
            {},
        ):
            async with self._lock:
                return sorted(
                    self._checkpoints.values(),
                    key=lambda c: c.projection_name,
                )

    async def clear(self) -> None:
        """Clear all checkpoints. Useful for test setup/teardown."""
        with self._tracer.span(
            "eventsource.checkpoint.clear",
            {},
        ):
            async with self._lock:
                self._checkpoints.clear()


class SQLiteCheckpointRepository:
    """
    SQLite implementation of checkpoint repository.

    Stores checkpoints in the `projection_checkpoints` table.

    SQLite-specific adaptations:
    - UUIDs stored as TEXT (36 characters, hyphenated format)
    - Timestamps stored as TEXT in ISO 8601 format
    - Uses UPSERT with ON CONFLICT syntax (SQLite 3.24+)
    - Simplified lag metrics (no FILTER clause or array operators)

    Example:
        >>> async with aiosqlite.connect("events.db") as db:
        ...     repo = SQLiteCheckpointRepository(db)
        ...     await repo.update_checkpoint(
        ...         "MyProjection",
        ...         event.event_id,
        ...         event.event_type,
        ...     )
    """

    def __init__(
        self,
        connection: "aiosqlite.Connection",
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the checkpoint repository.

        Args:
            connection: aiosqlite database connection
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._connection = connection

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            cursor = await self._connection.execute(
                """
                SELECT last_event_id
                FROM projection_checkpoints
                WHERE projection_name = ?
                """,
                (projection_name,),
            )
            row = await cursor.fetchone()
            if row and row[0]:
                return UUID(row[0])
            return None

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Update the checkpoint for a projection.

        Uses UPSERT pattern for idempotency - safe to call multiple times.

        Args:
            projection_name: Name of the projection
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.update_checkpoint",
            {
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_EVENT_TYPE: event_type,
            },
        ):
            now = datetime.now(UTC).isoformat()
            await self._connection.execute(
                """
                INSERT INTO projection_checkpoints
                    (projection_name, last_event_id, last_event_type,
                     last_processed_at, events_processed, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT (projection_name) DO UPDATE
                SET last_event_id = excluded.last_event_id,
                    last_event_type = excluded.last_event_type,
                    last_processed_at = excluded.last_processed_at,
                    events_processed = projection_checkpoints.events_processed + 1,
                    updated_at = excluded.updated_at
                """,
                (
                    projection_name,
                    str(event_id),
                    event_type,
                    now,
                    now,
                    now,
                ),
            )
            await self._connection.commit()

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """
        Get lag metrics for a projection.

        Note: SQLite implementation uses simplified queries since
        SQLite lacks PostgreSQL's FILTER clause and ANY() array operator.

        Args:
            projection_name: Name of the projection
            event_types: List of event types this projection handles

        Returns:
            LagMetrics if checkpoint exists, None otherwise
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_lag_metrics",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            # First, get the checkpoint data
            cursor = await self._connection.execute(
                """
                SELECT
                    projection_name,
                    last_event_id,
                    last_processed_at,
                    events_processed
                FROM projection_checkpoints
                WHERE projection_name = ?
                """,
                (projection_name,),
            )
            checkpoint_row = await cursor.fetchone()

            if not checkpoint_row:
                return None

            # Get the latest relevant event from the event store
            # If event_types is specified, filter by those types
            if event_types:
                # Build query with IN clause for event types
                placeholders = ",".join("?" * len(event_types))
                cursor = await self._connection.execute(
                    f"""
                    SELECT event_id, timestamp
                    FROM events
                    WHERE event_type IN ({placeholders})
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,  # nosec B608 - placeholders only contains "?" chars, values are parameterized
                    tuple(event_types),
                )
            else:
                # Get latest event of any type
                cursor = await self._connection.execute(
                    """
                    SELECT event_id, timestamp
                    FROM events
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """
                )

            latest_event_row = await cursor.fetchone()

            # Extract values
            last_event_id = checkpoint_row[1]
            last_processed_at_str = checkpoint_row[2]
            events_processed = checkpoint_row[3] or 0

            latest_event_id = latest_event_row[0] if latest_event_row else None
            latest_event_time_str = latest_event_row[1] if latest_event_row else None

            # Calculate lag in seconds
            lag_seconds = 0.0
            if last_processed_at_str and latest_event_time_str:
                try:
                    last_processed_at_dt = datetime.fromisoformat(
                        last_processed_at_str.replace("Z", "+00:00")
                    )
                    latest_event_time_dt = datetime.fromisoformat(
                        latest_event_time_str.replace("Z", "+00:00")
                    )
                    raw_lag = (latest_event_time_dt - last_processed_at_dt).total_seconds()
                    # If projection is up-to-date or ahead, lag is 0
                    if raw_lag > 0 and last_event_id != latest_event_id:
                        lag_seconds = round(raw_lag, 1)
                except (ValueError, TypeError):
                    lag_seconds = 0.0

            return LagMetrics(
                projection_name=checkpoint_row[0],
                last_event_id=last_event_id,
                latest_event_id=latest_event_id,
                lag_seconds=lag_seconds,
                events_processed=events_processed,
                last_processed_at=last_processed_at_str,
            )

    async def reset_checkpoint(self, projection_name: str) -> None:
        """
        Reset the checkpoint for a projection.

        Used when rebuilding a projection from scratch.

        Args:
            projection_name: Name of the projection
        """
        with self._tracer.span(
            "eventsource.checkpoint.reset_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            await self._connection.execute(
                """
                DELETE FROM projection_checkpoints
                WHERE projection_name = ?
                """,
                (projection_name,),
            )
            await self._connection.commit()

    async def get_position(self, subscription_id: str) -> int | None:
        """
        Get last processed global position for a subscription.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)

        Returns:
            Last processed global position, or None if no checkpoint exists
            or if checkpoint doesn't have position data.
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_position",
            {ATTR_PROJECTION_NAME: subscription_id},
        ):
            cursor = await self._connection.execute(
                """
                SELECT global_position
                FROM projection_checkpoints
                WHERE projection_name = ?
                """,
                (subscription_id,),
            )
            row = await cursor.fetchone()
            return row[0] if row and row[0] is not None else None

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Save checkpoint with global position.

        Updates the position, event_id, and event_type for the checkpoint.
        Uses UPSERT pattern for idempotency.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)
            position: Global position of the event
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.save_position",
            {
                ATTR_PROJECTION_NAME: subscription_id,
                ATTR_EVENT_TYPE: event_type,
                "global_position": position,
            },
        ):
            now = datetime.now(UTC).isoformat()
            await self._connection.execute(
                """
                INSERT INTO projection_checkpoints
                    (projection_name, last_event_id, last_event_type,
                     last_processed_at, events_processed, global_position,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(projection_name) DO UPDATE SET
                    last_event_id = excluded.last_event_id,
                    last_event_type = excluded.last_event_type,
                    last_processed_at = excluded.last_processed_at,
                    events_processed = events_processed + 1,
                    global_position = excluded.global_position,
                    updated_at = excluded.updated_at
                """,
                (
                    subscription_id,
                    str(event_id),
                    event_type,
                    now,
                    position,
                    now,
                    now,
                ),
            )
            await self._connection.commit()

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_all_checkpoints",
            {},
        ):
            cursor = await self._connection.execute(
                """
                SELECT projection_name, last_event_id, last_event_type,
                       last_processed_at, events_processed, global_position
                FROM projection_checkpoints
                ORDER BY projection_name
                """
            )
            rows = await cursor.fetchall()

            result: list[CheckpointData] = []
            for row in rows:
                # Parse last_event_id from TEXT to UUID
                last_event_id = UUID(row[1]) if row[1] else None

                # Parse last_processed_at from ISO 8601 string
                last_processed_at = None
                if row[3]:
                    with contextlib.suppress(ValueError, TypeError):
                        last_processed_at = datetime.fromisoformat(row[3].replace("Z", "+00:00"))

                result.append(
                    CheckpointData(
                        projection_name=row[0],
                        last_event_id=last_event_id,
                        last_event_type=row[2],
                        last_processed_at=last_processed_at,
                        events_processed=row[4] or 0,
                        global_position=row[5],
                    )
                )

            return result


# Type alias for backwards compatibility
CheckpointRepositoryProtocol = CheckpointRepository
