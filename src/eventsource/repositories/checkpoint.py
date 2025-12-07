"""
Checkpoint repository for tracking projection positions.

Projections use checkpoints to track which events they have processed,
enabling:
- Resumable processing after restarts
- Lag monitoring and health checks
- Safe rebuilds from specific positions
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from typing import Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


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
    """

    projection_name: str
    last_event_id: UUID | None = None
    last_event_type: str | None = None
    last_processed_at: datetime | None = None
    events_processed: int = 0


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

    def __init__(self, conn: AsyncConnection | AsyncEngine):
        """
        Initialize the checkpoint repository.

        Args:
            conn: Database connection or engine
        """
        self.conn = conn

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        query = text("""
            SELECT last_event_id
            FROM projection_checkpoints
            WHERE projection_name = :projection_name
        """)
        params = {"projection_name": projection_name}

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.connect() as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()
                return row[0] if row else None
        else:
            result = await self.conn.execute(query, params)
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

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                await conn.execute(query, params)
        else:
            await self.conn.execute(query, params)

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

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.connect() as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()
        else:
            result = await self.conn.execute(query, params)
            row = result.fetchone()

        if not row:
            return None

        # Extract values
        last_event_id = str(row[1]) if row[1] else None
        latest_event_id = str(row[2]) if row[2] else None
        raw_lag = float(row[3]) if row[3] else 0.0

        # Calculate actual lag
        # If last_event_id matches latest_event_id, projection is up-to-date
        if last_event_id and latest_event_id and last_event_id == latest_event_id or raw_lag < 0:
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
        query = text("""
            DELETE FROM projection_checkpoints
            WHERE projection_name = :projection_name
        """)
        params = {"projection_name": projection_name}

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                await conn.execute(query, params)
        else:
            await self.conn.execute(query, params)

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        query = text("""
            SELECT projection_name, last_event_id, last_event_type,
                   last_processed_at, events_processed
            FROM projection_checkpoints
            ORDER BY projection_name
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.connect() as conn:
                result = await conn.execute(query)
                rows = result.fetchall()
        else:
            result = await self.conn.execute(query)
            rows = result.fetchall()

        return [
            CheckpointData(
                projection_name=row[0],
                last_event_id=row[1],
                last_event_type=row[2],
                last_processed_at=row[3],
                events_processed=row[4] or 0,
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

    def __init__(self) -> None:
        """Initialize an empty in-memory checkpoint repository."""
        self._checkpoints: dict[str, CheckpointData] = {}
        self._lock = Lock()

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        with self._lock:
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
        now = datetime.now(UTC)
        with self._lock:
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
        with self._lock:
            checkpoint = self._checkpoints.get(projection_name)
            if not checkpoint:
                return None

            return LagMetrics(
                projection_name=checkpoint.projection_name,
                last_event_id=str(checkpoint.last_event_id) if checkpoint.last_event_id else None,
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
        with self._lock:
            self._checkpoints.pop(projection_name, None)

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        with self._lock:
            return sorted(
                self._checkpoints.values(),
                key=lambda c: c.projection_name,
            )

    def clear(self) -> None:
        """Clear all checkpoints. Useful for test setup/teardown."""
        with self._lock:
            self._checkpoints.clear()


# Type alias for backwards compatibility
CheckpointRepositoryProtocol = CheckpointRepository
