"""In-memory snapshot store implementation."""

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
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._snapshots: dict[tuple[UUID, str], Snapshot] = {}
        self._lock = asyncio.Lock()
        logger.debug("InMemorySnapshotStore initialized")

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save or update a snapshot using upsert semantics."""
        aid, atype, ver = snapshot.aggregate_id, snapshot.aggregate_type, snapshot.version
        attrs = {ATTR_AGGREGATE_ID: str(aid), ATTR_AGGREGATE_TYPE: atype, ATTR_VERSION: ver}
        with self._tracer.span("eventsource.snapshot.save", attrs):
            async with self._lock:
                key = (aid, atype)
                old = self._snapshots.get(key)
                self._snapshots[key] = snapshot
                if old:
                    msg = "Updated snapshot for %s/%s: v%d -> v%d"
                    logger.debug(msg, atype, aid, old.version, ver)
                else:
                    logger.debug("Created snapshot for %s/%s at v%d", atype, aid, ver)

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        """Get the snapshot for an aggregate, or None if not found."""
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.get", attrs):
            async with self._lock:
                snap = self._snapshots.get((aggregate_id, aggregate_type))
                if snap:
                    msg = "Retrieved snapshot for %s/%s at v%d"
                    logger.debug(msg, aggregate_type, aggregate_id, snap.version)
                else:
                    logger.debug("No snapshot found for %s/%s", aggregate_type, aggregate_id)
                return snap

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
        attrs = {ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.delete_by_type", attrs):
            async with self._lock:
                keys = [
                    k
                    for k, s in self._snapshots.items()
                    if k[1] == aggregate_type
                    and (schema_version_below is None or s.schema_version < schema_version_below)
                ]
                for k in keys:
                    del self._snapshots[k]
                if keys:
                    sfx = (
                        f" (schema_version < {schema_version_below})"
                        if schema_version_below
                        else ""
                    )
                    msg = "Deleted %d snapshots for aggregate type %s%s"
                    logger.info(msg, len(keys), aggregate_type, sfx)
                return len(keys)

    async def clear(self) -> None:
        """Clear all snapshots from the store."""
        with self._tracer.span("eventsource.snapshot.clear", {}):
            async with self._lock:
                count = len(self._snapshots)
                self._snapshots.clear()
                logger.debug("Cleared %d snapshots from InMemorySnapshotStore", count)

    @property
    def snapshot_count(self) -> int:
        return len(self._snapshots)

    def __len__(self) -> int:
        return len(self._snapshots)

    def __bool__(self) -> bool:
        return True

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> InMemorySnapshotStore:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"InMemorySnapshotStore(snapshots={len(self._snapshots)})"
