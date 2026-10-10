"""Persistence and saving mixin for AggregateRepository.

Provides event stream append, optimistic locking, and event dispatch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.aggregates.snapshotting import (
    SnapshotPolicy,
    SnapshotScheduler,
    take_snapshot,
)
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_VERSION,
)
from eventsource.ports.bus import EventPublisher
from eventsource.ports.positions import ExpectedVersion
from eventsource.ports.store import AggregateStore

if TYPE_CHECKING:
    from eventsource.ports.snapshots import SnapshotStore


class AggregateRepositorySaveMixin[TAggregate: AggregateRoot[Any]]:
    """Mixin providing save and persistence operations for AggregateRepository."""

    _tracer: Tracer
    _aggregate_type: str
    _event_store: AggregateStore
    _event_publisher: EventPublisher | None
    _snapshot_store: SnapshotStore | None
    _snapshot_policy: SnapshotPolicy
    _snapshot_scheduler: SnapshotScheduler

    def _stream(self, aggregate_id: UUID) -> StreamId:
        """Stream identity for one aggregate of this repository's type."""
        raise NotImplementedError

    async def save(self, aggregate: TAggregate) -> None:
        """
        Save an aggregate by persisting its uncommitted events.

        Appends all uncommitted events to the event store atomically.
        Uses optimistic locking to detect concurrent modifications.

        After successful persistence:
        1. Marks events as committed on the aggregate
        2. Publishes events to event publisher (if configured)
        3. Creates snapshot if threshold is met (if configured)

        Args:
            aggregate: The aggregate to save

        Raises:
            OptimisticLockError: If there's a version conflict

        Note:
            - If there are no uncommitted events, this is a no-op.
            - Snapshot creation failure does not fail the save operation.

        Example:
            >>> order.ship(tracking_number="TRACK123")
            >>> await repo.save(order)
            >>> assert not order.has_uncommitted_events
        """
        uncommitted_events = aggregate.uncommitted_events

        if not uncommitted_events:
            # No changes to persist
            return

        with self._tracer.span(
            "eventsource.repository.save",
            {
                ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
                ATTR_EVENT_COUNT: len(uncommitted_events),
                ATTR_VERSION: aggregate.version,
            },
        ) as span:
            # Calculate expected version
            # Current version minus number of new events = version before changes
            expected_version = aggregate.version - len(uncommitted_events)

            # Append events to event store
            await self._event_store.append(
                self._stream(aggregate.aggregate_id),
                uncommitted_events,
                ExpectedVersion.exact(expected_version),
            )

            # Mark events as committed on the aggregate
            aggregate.mark_events_as_committed()

            if span:
                span.set_attribute("save.success", True)
                span.set_attribute("new_version", aggregate.version)

            # Publish events if publisher is configured
            if self._event_publisher:
                await self._event_publisher.publish(uncommitted_events)

            # Create snapshot if the policy says so
            if self._snapshot_store is not None and self._snapshot_policy.should_snapshot(
                aggregate, len(uncommitted_events)
            ):
                with self._tracer.span(
                    "eventsource.repository.snapshot",
                    {
                        ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                        ATTR_AGGREGATE_TYPE: self._aggregate_type,
                        ATTR_VERSION: aggregate.version,
                    },
                ):
                    await self._snapshot_scheduler.schedule(
                        take_snapshot(aggregate, self._aggregate_type, self._snapshot_store),
                        aggregate_type=self._aggregate_type,
                        aggregate_id=aggregate.aggregate_id,
                    )


__all__ = [
    "AggregateRepositorySaveMixin",
]
