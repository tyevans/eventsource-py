"""
Snapshot port: contract for aggregate-state snapshot storage.

This module provides:
- Snapshot: Immutable data structure representing captured aggregate state
- SnapshotStore: Protocol for the core save/get/delete/exists capabilities
- SnapshotTypeInvalidation: Protocol for the optional bulk-invalidation
  capability (bulk delete by aggregate type). A store that does not support
  bulk invalidation simply does not implement this Protocol -- there is no
  default body and no NotImplementedError.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class Snapshot:
    """
    Represents a point-in-time capture of aggregate state.

    Snapshots are optimization artifacts that enable fast aggregate loading by
    skipping event replay for events before the snapshot.

    Attributes:
        aggregate_id: Unique identifier of the aggregate instance
        aggregate_type: Type name of the aggregate (e.g., "Order", "User")
        version: Aggregate version when this snapshot was taken.
        state: Serialized aggregate state as a JSON-compatible dictionary.
        schema_version: Version of the aggregate's state schema.
        created_at: Timestamp when this snapshot was created.
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


@runtime_checkable
class SnapshotStore(Protocol):
    """
    Protocol for snapshot storage.

    Implementations must provide:
    - save_snapshot: Save or update a snapshot (upsert semantics)
    - get_snapshot: Retrieve the latest snapshot for an aggregate
    - delete_snapshot: Remove a snapshot for an aggregate
    - snapshot_exists: Check whether a snapshot exists for an aggregate

    Design principles:
    - One snapshot per aggregate (keyed by aggregate_id + aggregate_type)
    - Upsert semantics for save (newer snapshot replaces older)
    - Snapshots are optional - missing snapshots trigger full event replay
    - Thread-safe for concurrent access
    """

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save or update a snapshot for an aggregate (upsert semantics).

        Args:
            snapshot: The snapshot to save. Must have all fields populated.
        """
        ...

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

        Raises:
            SnapshotDeserializationError: Implementors should raise this
                (``domain.exceptions``, ADR 0017) if the stored state cannot
                be deserialized -- callers going through
                ``read_valid_snapshot()`` never see it (it is caught there
                and treated as a cache miss); it exists so a store can say
                precisely why a snapshot was unusable instead of letting a
                driver-specific exception (e.g. ``json.JSONDecodeError``)
                escape.
        """
        ...

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
            True if a snapshot was deleted, False if no snapshot existed.
        """
        ...

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Check if a snapshot exists for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if a snapshot exists, False otherwise.
        """
        ...


@runtime_checkable
class SnapshotTypeInvalidation(Protocol):
    """
    Optional capability: bulk snapshot invalidation by aggregate type.

    A snapshot store that supports bulk invalidation implements this
    Protocol in addition to `SnapshotStore`; one that doesn't simply omits
    `delete_snapshots_by_type` -- there is no default body and no
    `NotImplementedError` fallback. Callers that need this capability check
    `isinstance(store, SnapshotTypeInvalidation)` before calling it.
    """

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """
        Delete snapshots for all aggregates of a given type.

        Args:
            aggregate_type: Type name of aggregates to delete snapshots for
            schema_version_below: If provided, only delete snapshots with
                                 schema_version less than this value.

        Returns:
            Number of snapshots deleted.
        """
        ...


__all__ = ["Snapshot", "SnapshotStore", "SnapshotTypeInvalidation"]
