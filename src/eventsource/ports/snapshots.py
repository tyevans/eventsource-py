"""Snapshot port: contract for aggregate-state snapshot storage."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class Snapshot:
    """Point-in-time capture of aggregate state enabling fast aggregate loading."""

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
    """Protocol for snapshot storage (save, get, delete, exists)."""

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save or update a snapshot for an aggregate (upsert semantics)."""
        ...

    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None:
        """Get the latest snapshot for an aggregate, or raise SnapshotDeserializationError."""
        ...

    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """Delete the snapshot for an aggregate; return True if deleted."""
        ...

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """Check if a snapshot exists for an aggregate."""
        ...


@runtime_checkable
class SnapshotTypeInvalidation(Protocol):
    """Optional capability: bulk snapshot invalidation by aggregate type."""

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """Delete snapshots for all aggregates of a given type and return count deleted."""
        ...


__all__ = ["Snapshot", "SnapshotStore", "SnapshotTypeInvalidation"]
