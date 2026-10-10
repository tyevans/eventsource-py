"""Snapshot operations mixin for AggregateRepository.

Provides snapshot persistence, waiting, and inspection operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from eventsource.application.aggregates.snapshotting import (
    SnapshotScheduler,
    take_snapshot,
)
from eventsource.domain.aggregate import AggregateRoot
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)

if TYPE_CHECKING:
    from eventsource.ports.snapshots import Snapshot, SnapshotStore


class AggregateRepositorySnapshotMixin[TAggregate: AggregateRoot[Any]]:
    """Mixin providing snapshot operations for AggregateRepository."""

    _tracer: Tracer
    _aggregate_type: str
    _snapshot_store: SnapshotStore | None
    _snapshot_threshold: int | None
    _snapshot_mode: Literal["sync", "background", "manual"]
    _snapshot_scheduler: SnapshotScheduler

    @property
    def snapshot_store(self) -> SnapshotStore | None:
        """Get the snapshot store, if configured."""
        return self._snapshot_store

    @property
    def snapshot_threshold(self) -> int | None:
        """Get the snapshot threshold (events between snapshots).

        Caveat: this reflects the constructor knob only. When a custom
        ``snapshot_policy`` is supplied, this property reports the default
        threshold, not the active policy's actual behavior.
        """
        return self._snapshot_threshold

    @property
    def snapshot_mode(self) -> Literal["sync", "background", "manual"]:
        """Get the snapshot creation mode.

        Caveat: this reflects the constructor knob only. When a custom
        ``snapshot_scheduler`` is supplied, this property reports the
        default mode, not the active scheduler's actual behavior.
        """
        return self._snapshot_mode

    @property
    def has_snapshot_support(self) -> bool:
        """Check if snapshot support is enabled."""
        return self._snapshot_store is not None

    async def create_snapshot(self, aggregate: TAggregate) -> Snapshot:
        """
        Manually create a snapshot for the given aggregate.

        Creates a snapshot of the aggregate's current state and saves it
        to the snapshot store. This method can be called regardless of
        the snapshot_mode or snapshot_threshold settings.

        Use cases:
        - Creating snapshots at specific business milestones
        - Forcing snapshot creation before maintenance
        - Pre-warming snapshots for frequently accessed aggregates
        - Testing snapshot functionality

        Args:
            aggregate: The aggregate to create a snapshot for.
                      The aggregate should have its current state loaded
                      (via load() or after applying events).

        Returns:
            The created Snapshot object with all metadata.

        Raises:
            RuntimeError: If snapshot_store is not configured.

        Example:
            >>> # Create snapshot after a major state transition
            >>> order = await repo.load(order_id)
            >>> order.complete_fulfillment()
            >>> await repo.save(order)
            >>> snapshot = await repo.create_snapshot(order)
            >>> print(f"Created snapshot at version {snapshot.version}")

            >>> # Create snapshot for frequently accessed aggregate
            >>> user = await repo.load(user_id)
            >>> await repo.create_snapshot(user)

        Note:
            The snapshot is saved immediately (synchronously) regardless
            of the configured snapshot_mode.

            If a snapshot already exists for the aggregate, it will be
            replaced (upsert semantics).
        """
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
            return await take_snapshot(aggregate, self._aggregate_type, self._snapshot_store)

    async def await_pending_snapshots(self) -> int:
        """
        Wait for all pending background snapshot tasks to complete.

        This method is primarily useful for testing to ensure all
        background snapshots are complete before assertions.

        Returns:
            Number of tasks that were awaited.

        Example:
            >>> # In tests
            >>> await repo.save(aggregate)  # Triggers background snapshot
            >>> count = await repo.await_pending_snapshots()
            >>> print(f"Waited for {count} background snapshots")
            >>> # Now safe to check snapshot store

        Note:
            In production, you typically don't need to call this method.
            Background snapshots complete independently.
        """
        return await self._snapshot_scheduler.await_pending()

    @property
    def pending_snapshot_count(self) -> int:
        """
        Get the number of pending background snapshot tasks.

        Useful for monitoring and debugging.

        Returns:
            Number of background snapshot tasks not yet complete.
        """
        return self._snapshot_scheduler.pending_count


__all__ = [
    "AggregateRepositorySnapshotMixin",
]
