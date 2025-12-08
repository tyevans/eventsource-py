"""
Snapshot store interface and core data structures.

The snapshot store provides optimized aggregate loading by capturing
point-in-time state. Instead of replaying all events, aggregates can be
restored from a snapshot and only replay events since the snapshot.

This module provides:
- Snapshot: Immutable data structure representing captured aggregate state
- SnapshotStore: Abstract base class for snapshot storage implementations
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class Snapshot:
    """
    Represents a point-in-time capture of aggregate state.

    Snapshots are optimization artifacts that enable fast aggregate
    loading by skipping event replay for events before the snapshot.
    They are not the source of truth - events are. Snapshots can be
    safely deleted and regenerated from events at any time.

    Attributes:
        aggregate_id: Unique identifier of the aggregate instance
        aggregate_type: Type name of the aggregate (e.g., "Order", "User")
        version: Aggregate version when this snapshot was taken.
                 Events with version <= this have been applied.
        state: Serialized aggregate state as a JSON-compatible dictionary.
               Created via Pydantic's model_dump(mode="json").
        schema_version: Version of the aggregate's state schema.
                       Used to detect incompatible schema changes.
        created_at: Timestamp when this snapshot was created.

    Example:
        >>> snapshot = Snapshot(
        ...     aggregate_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
        ...     aggregate_type="Order",
        ...     version=100,
        ...     state={"order_id": "...", "status": "shipped", "items": [...]},
        ...     schema_version=1,
        ...     created_at=datetime.now(UTC),
        ... )
        >>> print(snapshot)
        Snapshot(Order/550e8400-e29b-41d4-a716-446655440000, v100, schema_v1)

    Note:
        - Snapshots are immutable (frozen=True) for consistency
        - One snapshot per aggregate at any time (latest only)
        - Schema version mismatch triggers full event replay
    """

    aggregate_id: UUID
    aggregate_type: str
    version: int
    state: dict[str, Any]
    schema_version: int
    created_at: datetime

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"Snapshot({self.aggregate_type}/{self.aggregate_id}, "
            f"v{self.version}, schema_v{self.schema_version})"
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"Snapshot("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"version={self.version}, "
            f"schema_version={self.schema_version}, "
            f"state_keys={list(self.state.keys())}, "
            f"created_at={self.created_at!r})"
        )


class SnapshotStore(ABC):
    """
    Abstract base class for snapshot storage.

    Snapshot stores provide persistence for aggregate state snapshots,
    enabling fast aggregate loading by skipping event replay for events
    that occurred before the snapshot.

    Implementations must provide:
    - save_snapshot: Save or update a snapshot (upsert semantics)
    - get_snapshot: Retrieve the latest snapshot for an aggregate
    - delete_snapshot: Remove a snapshot for an aggregate

    Design principles:
    - One snapshot per aggregate (keyed by aggregate_id + aggregate_type)
    - Upsert semantics for save (newer snapshot replaces older)
    - Snapshots are optional - missing snapshots trigger full event replay
    - Thread-safe for concurrent access

    Example:
        >>> class MySnapshotStore(SnapshotStore):
        ...     async def save_snapshot(self, snapshot: Snapshot) -> None:
        ...         # Persist snapshot to storage
        ...         ...
        ...
        ...     async def get_snapshot(
        ...         self, aggregate_id: UUID, aggregate_type: str
        ...     ) -> Snapshot | None:
        ...         # Retrieve snapshot from storage
        ...         ...
        ...
        ...     async def delete_snapshot(
        ...         self, aggregate_id: UUID, aggregate_type: str
        ...     ) -> bool:
        ...         # Delete snapshot from storage
        ...         ...

    See Also:
        - InMemorySnapshotStore: Testing/development implementation
        - PostgreSQLSnapshotStore: Production PostgreSQL implementation
        - SQLiteSnapshotStore: Embedded SQLite implementation
    """

    @abstractmethod
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save or update a snapshot for an aggregate.

        Uses upsert semantics: if a snapshot exists for the aggregate,
        it is replaced with the new snapshot. This ensures only the
        latest snapshot is retained.

        Args:
            snapshot: The snapshot to save. Must have all fields populated.

        Raises:
            Exception: Implementation-specific errors (e.g., database errors)

        Example:
            >>> snapshot = Snapshot(
            ...     aggregate_id=order_id,
            ...     aggregate_type="Order",
            ...     version=100,
            ...     state=order.state.model_dump(mode="json"),
            ...     schema_version=1,
            ...     created_at=datetime.now(UTC),
            ... )
            >>> await snapshot_store.save_snapshot(snapshot)
        """
        pass

    @abstractmethod
    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None:
        """
        Get the latest snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate (e.g., "Order")

        Returns:
            The snapshot if found, None otherwise.
            Never raises for missing snapshots - returns None instead.

        Example:
            >>> snapshot = await snapshot_store.get_snapshot(
            ...     aggregate_id=order_id,
            ...     aggregate_type="Order",
            ... )
            >>> if snapshot:
            ...     print(f"Found snapshot at version {snapshot.version}")
            ... else:
            ...     print("No snapshot, will replay all events")
        """
        pass

    @abstractmethod
    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Delete the snapshot for an aggregate.

        Used for:
        - Manual cache invalidation
        - Schema migration (force full replay)
        - Cleanup operations

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if a snapshot was deleted, False if no snapshot existed.

        Note:
            Deleting a snapshot does NOT affect the aggregate or its events.
            The snapshot will be regenerated on the next save if snapshotting
            is enabled.

        Example:
            >>> deleted = await snapshot_store.delete_snapshot(
            ...     aggregate_id=order_id,
            ...     aggregate_type="Order",
            ... )
            >>> if deleted:
            ...     print("Snapshot deleted, next load will replay all events")
        """
        pass

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Check if a snapshot exists for an aggregate.

        Default implementation uses get_snapshot. Implementations may
        override for efficiency (e.g., using SQL EXISTS query).

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if a snapshot exists, False otherwise.

        Example:
            >>> if await snapshot_store.snapshot_exists(order_id, "Order"):
            ...     print("Snapshot available")
        """
        snapshot = await self.get_snapshot(aggregate_id, aggregate_type)
        return snapshot is not None

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """
        Delete snapshots for all aggregates of a given type.

        Useful for bulk invalidation during schema migrations.
        When schema_version_below is specified, only deletes snapshots
        with schema_version < the specified value.

        Args:
            aggregate_type: Type name of aggregates to delete snapshots for
            schema_version_below: If provided, only delete snapshots with
                                 schema_version less than this value.

        Returns:
            Number of snapshots deleted.

        Note:
            Default implementation raises NotImplementedError.
            Concrete implementations should override for production use.

        Example:
            >>> # Delete all Order snapshots with schema version < 2
            >>> count = await snapshot_store.delete_snapshots_by_type(
            ...     aggregate_type="Order",
            ...     schema_version_below=2,
            ... )
            >>> print(f"Invalidated {count} old snapshots")
        """
        raise NotImplementedError(
            "delete_snapshots_by_type is not implemented by default. "
            "Concrete implementations may provide this functionality."
        )
