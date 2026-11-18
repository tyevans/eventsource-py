"""Snapshot operations mixin for AggregateRepository."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from eventsource.application.aggregates.snapshot_owner import snapshot_owner
from eventsource.application.aggregates.snapshotting import (
    SnapshotScheduler,
    take_snapshot,
)
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)

if TYPE_CHECKING:
    from eventsource.ports.snapshots import Snapshot, SnapshotStore
    from eventsource.ports.store import AggregateStore


class AggregateRepositorySnapshotMixin[TAggregate: AggregateRoot[Any]]:
    """Mixin providing snapshot operations for AggregateRepository."""

    _tracer: Tracer
    _aggregate_type: str
    _snapshot_store: SnapshotStore | None
    _snapshot_threshold: int | None
    _snapshot_mode: Literal["sync", "background", "manual"]
    _snapshot_scheduler: SnapshotScheduler
    _event_store: AggregateStore

    def stream_for(self, aggregate_id: UUID) -> StreamId:
        """Stream identity for one aggregate of this repository's type."""
        raise NotImplementedError

    @property
    def snapshot_store(self) -> SnapshotStore | None:
        """Get the snapshot store, if configured."""
        return self._snapshot_store

    @property
    def snapshot_threshold(self) -> int | None:
        """Get the snapshot threshold (constructor knob; may differ from custom policy)."""
        return self._snapshot_threshold

    @property
    def snapshot_mode(self) -> Literal["sync", "background", "manual"]:
        """Get the snapshot creation mode (constructor knob; may differ from custom scheduler)."""
        return self._snapshot_mode

    @property
    def has_snapshot_support(self) -> bool:
        """Check if snapshot support is enabled."""
        return self._snapshot_store is not None

    async def create_snapshot(self, aggregate: TAggregate) -> Snapshot:
        """Manually create and save a snapshot for the given aggregate."""
        if self._snapshot_store is None:
            raise RuntimeError(
                "Cannot create snapshot: snapshot_store is not configured. "
                "Provide a snapshot_store when creating the repository."
            )

        with self._tracer.span(
            "eventsource.repository.create_snapshot",
            {
                ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
                ATTR_VERSION: aggregate.version,
            },
        ):
            # The aggregate does not keep its events, so read the stream to find its owner.
            # The ambient scope is not used: a foreign scope must not claim the snapshot.
            history = [
                env.event
                async for env in self._event_store.read_stream(
                    self.stream_for(aggregate.aggregate_id)
                )
            ]
            return await take_snapshot(
                aggregate,
                self._aggregate_type,
                self._snapshot_store,
                tenant=snapshot_owner(history),
            )

    async def await_pending_snapshots(self) -> int:
        """Wait for all pending background snapshot tasks to complete."""
        return await self._snapshot_scheduler.await_pending()

    @property
    def pending_snapshot_count(self) -> int:
        """Get the number of pending background snapshot tasks."""
        return self._snapshot_scheduler.pending_count


__all__ = ["AggregateRepositorySnapshotMixin"]
