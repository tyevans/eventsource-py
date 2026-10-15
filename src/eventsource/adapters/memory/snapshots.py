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
from eventsource.ports.snapshots import Snapshot

logger = logging.getLogger(__name__)


class InMemorySnapshotStore:
    """In-memory implementation of SnapshotStore keyed by (aggregate_id, aggregate_type)."""

    def __init__(self, *, tracer: Tracer | None = None, enable_tracing: bool = True) -> None:
        """Initialize the in-memory snapshot store."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._snapshots: dict[tuple[UUID, str], Snapshot] = {}
        self._lock = asyncio.Lock()
        logger.debug("InMemorySnapshotStore initialized")

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save or update a snapshot using upsert semantics."""
        attrs = {
            ATTR_AGGREGATE_ID: str(snapshot.aggregate_id),
            ATTR_AGGREGATE_TYPE: snapshot.aggregate_type,
            ATTR_VERSION: snapshot.version,
        }
        with self._tracer.span("eventsource.snapshot.save", attrs):
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

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        """Get the snapshot for an aggregate, or None if not found."""
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.get", attrs):
            async with self._lock:
                snapshot = self._snapshots.get((aggregate_id, aggregate_type))
                if snapshot:
                    logger.debug(
                        "Retrieved snapshot for %s/%s at v%d",
                        aggregate_type,
                        aggregate_id,
                        snapshot.version,
                    )
                else:
                    logger.debug("No snapshot found for %s/%s", aggregate_type, aggregate_id)
                return snapshot

    async def delete_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Delete the snapshot for an aggregate. Returns True if deleted."""
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.delete", attrs):
            async with self._lock:
                key = (aggregate_id, aggregate_type)
                if key in self._snapshots:
                    del self._snapshots[key]
                    logger.debug("Deleted snapshot for %s/%s", aggregate_type, aggregate_id)
                    return True
                logger.debug("No snapshot to delete for %s/%s", aggregate_type, aggregate_id)
                return False

    async def snapshot_exists(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Check if a snapshot exists for an aggregate."""
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.exists", attrs):
            async with self._lock:
                return (aggregate_id, aggregate_type) in self._snapshots

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """Delete all snapshots for aggregates of a given type."""
        with self._tracer.span(
            "eventsource.snapshot.delete_by_type",
            {ATTR_AGGREGATE_TYPE: aggregate_type},
        ):
            async with self._lock:
                keys_to_delete = [
                    key
                    for key, snapshot in self._snapshots.items()
                    if key[1] == aggregate_type
                    and (
                        schema_version_below is None
                        or snapshot.schema_version < schema_version_below
                    )
                ]
                for key in keys_to_delete:
                    del self._snapshots[key]
                if keys_to_delete:
                    suffix = (
                        f" (schema_version < {schema_version_below})"
                        if schema_version_below
                        else ""
                    )
                    logger.info(
                        "Deleted %d snapshots for aggregate type %s%s",
                        len(keys_to_delete),
                        aggregate_type,
                        suffix,
                    )
                return len(keys_to_delete)

    async def clear(self) -> None:
        """Clear all snapshots from the store."""
        with self._tracer.span("eventsource.snapshot.clear", {}):
            async with self._lock:
                count = len(self._snapshots)
                self._snapshots.clear()
                logger.debug("Cleared %d snapshots from InMemorySnapshotStore", count)

    @property
    def snapshot_count(self) -> int:
        """Get the number of snapshots currently stored."""
        return len(self._snapshots)

    def __len__(self) -> int:
        """Return total number of snapshots currently stored."""
        return len(self._snapshots)

    def __bool__(self) -> bool:
        """Snapshot store instances are always truthy even when empty."""
        return True

    async def close(self) -> None:
        """No-op resource release for lifecycle parity."""
        return None

    async def __aenter__(self) -> InMemorySnapshotStore:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"InMemorySnapshotStore(snapshots={len(self._snapshots)})"
