"""
Checkpoint repository for tracking projection positions.

Projections use checkpoints to track which events they have processed,
enabling:
- Resumable processing after restarts
- Lag monitoring and health checks
- Safe rebuilds from specific positions
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.repositories._dialect import (
    Dialect,
    dialect_of,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)


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


class SQLCheckpointRepository:
    """
    SQLAlchemy-backed checkpoint repository, serving both PostgreSQL and SQLite.

    Stores checkpoints in the `projection_checkpoints` table. Dialect
    differences (UUID/timestamp representation, current-time expression,
    array vs. IN-list filtering) are resolved per call via
    `eventsource.repositories._dialect`.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = SQLCheckpointRepository(conn)
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
        self._conn = conn
        # Kept for backwards-compatible attribute access.
        self.conn = conn

    @asynccontextmanager
    async def _connect(self, *, write: bool) -> AsyncIterator[AsyncConnection]:
        """
        Yield a connection to execute on.

        If this repository was constructed with a live connection, that
        connection is yielded directly and NOT committed -- the caller owns the
        transaction. If constructed with an engine, a connection is opened here
        and, for writes, committed on successful exit.
        """
        if isinstance(self._conn, AsyncEngine):
            if write:
                async with self._conn.begin() as conn:
                    yield conn
            else:
                async with self._conn.connect() as conn:
                    yield conn
        else:
            yield self._conn

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

            async with self._connect(write=False) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()
                return uuid_result(row[0]) if row else None

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

            async with self._connect(write=True) as conn:
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
                params = {
                    "projection_name": projection_name,
                    "event_id": uuid_param(event_id, dialect),
                    "event_type": event_type,
                    "now": ts_param(now, dialect),
                }
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

            async with self._connect(write=False) as conn:
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

                query = text(f"""
                    WITH latest_relevant_event AS (
                        SELECT event_id as max_id, timestamp as max_time
                        FROM events
                        {event_filter}
                        ORDER BY timestamp DESC
                        LIMIT 1
                    )
                    SELECT
                        pc.projection_name,
                        pc.last_event_id,
                        le.max_id as latest_event_id,
                        le.max_time as latest_event_time,
                        pc.events_processed,
                        pc.last_processed_at
                    FROM projection_checkpoints pc
                    LEFT JOIN latest_relevant_event le ON 1 = 1
                    WHERE pc.projection_name = :projection_name
                """)

                result = await conn.execute(query, params)
                row = result.fetchone()

            if not row:
                return None

            # Extract values
            last_event_id = str(uuid_result(row[1])) if row[1] is not None else None
            latest_event_id = str(uuid_result(row[2])) if row[2] is not None else None
            last_processed_at = ts_result(row[5])
            latest_event_time = ts_result(row[3])

            raw_lag = 0.0
            if last_processed_at is not None and latest_event_time is not None:
                raw_lag = (latest_event_time - last_processed_at).total_seconds()

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
                last_processed_at=last_processed_at.isoformat() if last_processed_at else None,
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

            async with self._connect(write=True) as conn:
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

            async with self._connect(write=False) as conn:
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

            async with self._connect(write=True) as conn:
                dialect = dialect_of(conn)
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
                    "event_id": uuid_param(event_id, dialect),
                    "event_type": event_type,
                    "now": ts_param(now, dialect),
                    "position": position,
                }
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

            async with self._connect(write=False) as conn:
                result = await conn.execute(query)
                rows = result.fetchall()

            return [
                CheckpointData(
                    projection_name=row[0],
                    last_event_id=uuid_result(row[1]),
                    last_event_type=row[2],
                    last_processed_at=ts_result(row[3]),
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


# Type alias for backwards compatibility
CheckpointRepositoryProtocol = CheckpointRepository
