"""
In-memory snapshot store implementation.

Provides a simple, thread-safe snapshot store for testing and development.
All data is stored in memory and lost when the process ends.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)
from eventsource.snapshots.interface import Snapshot, SnapshotStore

logger = logging.getLogger(__name__)


class InMemorySnapshotStore(SnapshotStore):
    """
    In-memory implementation of SnapshotStore for testing and development.

    Stores snapshots in a dictionary keyed by (aggregate_id, aggregate_type).
    Uses asyncio.Lock for thread-safe concurrent access.

    Features:
    - Fast in-memory operations
    - Thread-safe with asyncio.Lock
    - clear() method for test cleanup
    - Full implementation of delete_snapshots_by_type
    - Optional OpenTelemetry tracing

    Limitations:
    - Data is not persisted (lost on process restart)
    - Not suitable for production use
    - Single-process only (no shared state)

    Example:
        >>> from eventsource.snapshots import InMemorySnapshotStore, Snapshot
        >>> from datetime import datetime, UTC
        >>> from uuid import uuid4
        >>>
        >>> store = InMemorySnapshotStore()
        >>>
        >>> # Save a snapshot
        >>> snapshot = Snapshot(
        ...     aggregate_id=uuid4(),
        ...     aggregate_type="Order",
        ...     version=100,
        ...     state={"status": "shipped"},
        ...     schema_version=1,
        ...     created_at=datetime.now(UTC),
        ... )
        >>> await store.save_snapshot(snapshot)
        >>>
        >>> # Retrieve the snapshot
        >>> loaded = await store.get_snapshot(
        ...     snapshot.aggregate_id, "Order"
        ... )
        >>> assert loaded == snapshot

    See Also:
        PostgreSQLSnapshotStore: For production PostgreSQL deployments
        SQLiteSnapshotStore: For embedded/lightweight deployments
    """

    def __init__(
        self,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the in-memory snapshot store.

        Args:
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
                          Ignored if tracer is explicitly provided.
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._snapshots: dict[tuple[UUID, str], Snapshot] = {}
        self._lock = asyncio.Lock()
        logger.debug("InMemorySnapshotStore initialized")

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save or update a snapshot.

        Uses upsert semantics - replaces any existing snapshot for the
        same aggregate_id and aggregate_type combination.

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
            async with self._lock:
                key = (snapshot.aggregate_id, snapshot.aggregate_type)
                old_snapshot = self._snapshots.get(key)
                self._snapshots[key] = snapshot

                if old_snapshot:
                    logger.debug(
                        "Updated snapshot for %s/%s: v%d -> v%d",
                        snapshot.aggregate_type,
                        snapshot.aggregate_id,
                        old_snapshot.version,
                        snapshot.version,
                    )
                else:
                    logger.debug(
                        "Created snapshot for %s/%s at v%d",
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
            async with self._lock:
                key = (aggregate_id, aggregate_type)
                snapshot = self._snapshots.get(key)

                if snapshot:
                    logger.debug(
                        "Retrieved snapshot for %s/%s at v%d",
                        aggregate_type,
                        aggregate_id,
                        snapshot.version,
                    )
                else:
                    logger.debug(
                        "No snapshot found for %s/%s",
                        aggregate_type,
                        aggregate_id,
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
            True if a snapshot was deleted, False if none existed
        """
        with self._tracer.span(
            "eventsource.snapshot.delete",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            async with self._lock:
                key = (aggregate_id, aggregate_type)
                if key in self._snapshots:
                    del self._snapshots[key]
                    logger.debug(
                        "Deleted snapshot for %s/%s",
                        aggregate_type,
                        aggregate_id,
                    )
                    return True

                logger.debug(
                    "No snapshot to delete for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )
                return False

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Check if a snapshot exists for an aggregate.

        More efficient than default implementation as it doesn't
        need to retrieve the full snapshot.

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
            async with self._lock:
                key = (aggregate_id, aggregate_type)
                return key in self._snapshots

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """
        Delete all snapshots for aggregates of a given type.

        Args:
            aggregate_type: Type name of aggregates
            schema_version_below: If provided, only delete snapshots
                                 with schema_version < this value

        Returns:
            Number of snapshots deleted
        """
        with self._tracer.span(
            "eventsource.snapshot.delete_by_type",
            {
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            async with self._lock:
                keys_to_delete = []

                for key, snapshot in self._snapshots.items():
                    agg_id, agg_type = key
                    if agg_type != aggregate_type:
                        continue

                    if (
                        schema_version_below is not None
                        and snapshot.schema_version >= schema_version_below
                    ):
                        continue

                    keys_to_delete.append(key)

                for key in keys_to_delete:
                    del self._snapshots[key]

                if keys_to_delete:
                    logger.info(
                        "Deleted %d snapshots for aggregate type %s%s",
                        len(keys_to_delete),
                        aggregate_type,
                        f" (schema_version < {schema_version_below})"
                        if schema_version_below
                        else "",
                    )

                return len(keys_to_delete)

    async def clear(self) -> None:
        """
        Clear all snapshots from the store.

        Useful for test cleanup between test cases.

        Example:
            >>> @pytest.fixture
            ... async def snapshot_store():
            ...     store = InMemorySnapshotStore()
            ...     yield store
            ...     await store.clear()  # Cleanup after test
        """
        with self._tracer.span(
            "eventsource.snapshot.clear",
            {},
        ):
            async with self._lock:
                count = len(self._snapshots)
                self._snapshots.clear()
                logger.debug("Cleared %d snapshots from InMemorySnapshotStore", count)

    @property
    def snapshot_count(self) -> int:
        """
        Get the number of snapshots currently stored.

        Useful for testing and debugging. Not thread-safe for reads
        but acceptable for diagnostic purposes.

        Returns:
            Number of snapshots in the store
        """
        return len(self._snapshots)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"InMemorySnapshotStore(snapshots={len(self._snapshots)})"
