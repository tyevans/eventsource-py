"""
SQLite snapshot store implementation.

Provides lightweight, embedded snapshot storage using SQLite
with async aiosqlite driver.

This implementation is suitable for:
- Development and testing environments
- Single-instance deployments
- Embedded applications
- Edge computing scenarios

For high-concurrency production workloads, consider PostgreSQLSnapshotStore.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)
from eventsource.snapshots.interface import Snapshot, SnapshotStore

# Optional dependency handling
try:
    import aiosqlite

    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False
    aiosqlite = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SQLiteNotAvailableError(ImportError):
    """Raised when aiosqlite is not installed."""

    def __init__(self) -> None:
        super().__init__(
            "aiosqlite is required for SQLiteSnapshotStore. "
            "Install it with: pip install eventsource[sqlite]"
        )


class SQLiteSnapshotStore(SnapshotStore):
    """
    SQLite implementation of SnapshotStore.

    Uses aiosqlite for async database operations. Ideal for:
    - Development and testing
    - Single-process applications
    - Embedded systems
    - Desktop applications

    Features:
    - File-based persistence
    - No external database server required
    - Async operations via aiosqlite
    - Upsert using INSERT OR REPLACE
    - Optional OpenTelemetry tracing

    Example:
        >>> from eventsource.snapshots import SQLiteSnapshotStore
        >>>
        >>> store = SQLiteSnapshotStore("snapshots.db")
        >>>
        >>> # Use with repository
        >>> repo = AggregateRepository(
        ...     event_store=event_store,
        ...     aggregate_factory=OrderAggregate,
        ...     aggregate_type="Order",
        ...     snapshot_store=store,
        ... )

    Note:
        - Ensure the snapshots table exists before using this store
        - SQLite is single-writer, so concurrent writes may be slower
        - For production workloads, consider PostgreSQLSnapshotStore
    """

    def __init__(
        self,
        database_path: str,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the SQLite snapshot store.

        Args:
            database_path: Path to the SQLite database file.
                          Use ":memory:" for in-memory database.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
                          Ignored if tracer is explicitly provided.

        Raises:
            SQLiteNotAvailableError: If aiosqlite is not installed.
        """
        if not SQLITE_AVAILABLE:
            raise SQLiteNotAvailableError()

        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._database_path = database_path
        logger.debug("SQLiteSnapshotStore initialized with %s", database_path)

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save or update a snapshot using INSERT OR REPLACE.

        Args:
            snapshot: The snapshot to save
        """
        with self._tracer.span(
            "eventsource.snapshot.save",
            {
                ATTR_AGGREGATE_ID: str(snapshot.aggregate_id),
                ATTR_AGGREGATE_TYPE: snapshot.aggregate_type,
                ATTR_VERSION: snapshot.version,
            },
        ):
            async with aiosqlite.connect(self._database_path) as conn:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO snapshots (
                        aggregate_id,
                        aggregate_type,
                        version,
                        schema_version,
                        state,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(snapshot.aggregate_id),
                        snapshot.aggregate_type,
                        snapshot.version,
                        snapshot.schema_version,
                        json.dumps(snapshot.state),
                        snapshot.created_at.isoformat(),
                    ),
                )
                await conn.commit()

            logger.debug(
                "Saved snapshot for %s/%s at version %d",
                snapshot.aggregate_type,
                snapshot.aggregate_id,
                snapshot.version,
            )

    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None:
        """
        Get the snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            The snapshot if found, None otherwise
        """
        with self._tracer.span(
            "eventsource.snapshot.get",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            async with aiosqlite.connect(self._database_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    """
                    SELECT
                        aggregate_id,
                        aggregate_type,
                        version,
                        schema_version,
                        state,
                        created_at
                    FROM snapshots
                    WHERE aggregate_id = ?
                      AND aggregate_type = ?
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                row = await cursor.fetchone()

            if row is None:
                logger.debug(
                    "No snapshot found for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )
                return None

            snapshot = Snapshot(
                aggregate_id=UUID(row["aggregate_id"]),
                aggregate_type=row["aggregate_type"],
                version=row["version"],
                schema_version=row["schema_version"],
                state=json.loads(row["state"]),
                created_at=datetime.fromisoformat(row["created_at"]),
            )

            logger.debug(
                "Retrieved snapshot for %s/%s at version %d",
                aggregate_type,
                aggregate_id,
                snapshot.version,
            )

            return snapshot

    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Delete the snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if a snapshot was deleted, False otherwise
        """
        with self._tracer.span(
            "eventsource.snapshot.delete",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            async with aiosqlite.connect(self._database_path) as conn:
                cursor = await conn.execute(
                    """
                    DELETE FROM snapshots
                    WHERE aggregate_id = ?
                      AND aggregate_type = ?
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                await conn.commit()
                deleted: bool = cursor.rowcount > 0

            if deleted:
                logger.debug(
                    "Deleted snapshot for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )
            else:
                logger.debug(
                    "No snapshot to delete for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )

            return deleted

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Check if a snapshot exists.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if snapshot exists, False otherwise
        """
        with self._tracer.span(
            "eventsource.snapshot.exists",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            async with aiosqlite.connect(self._database_path) as conn:
                cursor = await conn.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM snapshots
                        WHERE aggregate_id = ?
                          AND aggregate_type = ?
                    )
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                row = await cursor.fetchone()
                return bool(row[0]) if row else False

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """
        Delete all snapshots for a given aggregate type.

        Args:
            aggregate_type: Type name of aggregates
            schema_version_below: If provided, only delete snapshots with
                                 schema_version < this value

        Returns:
            Number of snapshots deleted
        """
        with self._tracer.span(
            "eventsource.snapshot.delete_by_type",
            {
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            async with aiosqlite.connect(self._database_path) as conn:
                if schema_version_below is not None:
                    cursor = await conn.execute(
                        """
                        DELETE FROM snapshots
                        WHERE aggregate_type = ?
                          AND schema_version < ?
                        """,
                        (aggregate_type, schema_version_below),
                    )
                else:
                    cursor = await conn.execute(
                        """
                        DELETE FROM snapshots
                        WHERE aggregate_type = ?
                        """,
                        (aggregate_type,),
                    )
                await conn.commit()
                count: int = cursor.rowcount

            if count > 0:
                logger.info(
                    "Deleted %d snapshots for aggregate type %s%s",
                    count,
                    aggregate_type,
                    f" (schema_version < {schema_version_below})" if schema_version_below else "",
                )

            return count

    @property
    def database_path(self) -> str:
        """Get the database path."""
        return self._database_path
