"""Query and loading mixin for AggregateRepository.

Provides aggregate hydration from snapshots and event streams.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.aggregates.snapshotting import (
    SnapshotMissReason,
    read_valid_snapshot,
    record_snapshot_miss,
)
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)
from eventsource.ports.envelopes import StreamReadOptions
from eventsource.ports.store import AggregateStore

if TYPE_CHECKING:
    from eventsource.ports.snapshots import SnapshotStore

logger = logging.getLogger(__name__)


class AggregateRepositoryQueryMixin[TAggregate: AggregateRoot[Any]]:
    """Mixin providing load, query, and factory operations for AggregateRepository."""

    _tracer: Tracer
    _aggregate_type: str
    _aggregate_factory: type[TAggregate]
    _snapshot_store: SnapshotStore | None
    _event_store: AggregateStore

    def _stream(self, aggregate_id: UUID) -> StreamId:
        """Stream identity for one aggregate of this repository's type."""
        raise NotImplementedError

    async def load(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an aggregate from its event history.

        If a snapshot store is configured and a valid snapshot exists,
        the aggregate is restored from the snapshot and only events
        since the snapshot are replayed. This significantly improves
        load time for aggregates with many events.

        Retrieves all events for the aggregate from the event store
        and reconstitutes the aggregate state by replaying them.

        Args:
            aggregate_id: ID of the aggregate to load

        Returns:
            The reconstituted aggregate with current state

        Raises:
            AggregateNotFoundError: If no events exist for the aggregate

        Loading Sequence:
            1. Check for valid snapshot (if snapshot_store configured)
            2. If snapshot valid: restore state, get events from snapshot.version
            3. If no snapshot: get all events from version 0
            4. Apply events to aggregate
            5. Return hydrated aggregate

        Example:
            >>> order = await repo.load(order_id)
            >>> print(f"Order status: {order.state.status}")
        """
        with self._tracer.span(
            "eventsource.repository.load",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
            },
        ) as span:
            from_version = 0
            snapshot = None

            # Try to load from snapshot if configured
            if self._snapshot_store is not None:
                snapshot = await read_valid_snapshot(
                    self._snapshot_store,
                    aggregate_id,
                    self._aggregate_type,
                    self._aggregate_factory,
                )
                if snapshot is not None:
                    from_version = snapshot.version
                    if span:
                        span.set_attribute("snapshot.used", True)
                        span.set_attribute("snapshot.version", from_version)
                    logger.debug(
                        "Using snapshot for %s/%s at version %d",
                        self._aggregate_type,
                        aggregate_id,
                        from_version,
                    )

            # Get events from event store (from snapshot version or 0)
            stream = self._stream(aggregate_id)
            options = StreamReadOptions(from_version=from_version + 1) if from_version > 0 else None
            events = [
                envelope.event async for envelope in self._event_store.read_stream(stream, options)
            ]

            # Handle case: no snapshot and no events
            if snapshot is None and not events:
                raise AggregateNotFoundError(aggregate_id, self._aggregate_type)

            # Create aggregate instance
            aggregate = self._aggregate_factory(aggregate_id)

            # Restore from snapshot if available
            if snapshot is not None:
                try:
                    aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
                except Exception as e:
                    # Deserialization failed - fall back to full replay.
                    # Counted here rather than in read_valid_snapshot because
                    # this is where a corrupt payload actually surfaces for
                    # every in-tree adapter: they return the row intact and
                    # the failure appears when the aggregate rebuilds state.
                    record_snapshot_miss(
                        SnapshotMissReason.STATE_RESTORE_FAILED, self._aggregate_type
                    )
                    logger.warning(
                        "Failed to restore from snapshot for %s/%s: %s. "
                        "Falling back to full event replay.",
                        self._aggregate_type,
                        aggregate_id,
                        e,
                        exc_info=True,
                    )
                    # Re-fetch all events
                    events = [
                        envelope.event async for envelope in self._event_store.read_stream(stream)
                    ]
                    if not events:
                        raise AggregateNotFoundError(aggregate_id, self._aggregate_type) from None
                    # Reset aggregate
                    aggregate = self._aggregate_factory(aggregate_id)

            # Apply events since snapshot (or all events if no snapshot)
            if events:
                aggregate.load_from_history(events)

            if span:
                span.set_attribute("events.replayed", len(events))
                span.set_attribute(ATTR_VERSION, aggregate.version)

            logger.debug(
                "Loaded %s/%s at version %d (snapshot: %s, events replayed: %d)",
                self._aggregate_type,
                aggregate_id,
                aggregate.version,
                "yes" if snapshot else "no",
                len(events),
            )

            return aggregate

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an existing aggregate or create a new one.

        Useful when you want to work with an aggregate regardless of
        whether it already exists.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            Existing aggregate if found, or new empty aggregate

        Example:
            >>> order = await repo.load_or_create(order_id)
            >>> if order.version == 0:
            ...     order.create(customer_id=customer_id)
        """
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return self._aggregate_factory(aggregate_id)

    async def exists(self, aggregate_id: UUID) -> bool:
        """
        Check if an aggregate exists.

        Args:
            aggregate_id: ID of the aggregate to check

        Returns:
            True if aggregate has events, False otherwise
        """
        with self._tracer.span(
            "eventsource.repository.exists",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
            },
        ) as span:
            exists = await self._event_store.get_stream_version(self._stream(aggregate_id)) > 0

            if span:
                span.set_attribute("exists", exists)

            return exists

    async def get_version(self, aggregate_id: UUID) -> int:
        """
        Get the current version of an aggregate.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            Current version (0 if aggregate doesn't exist)
        """
        return await self._event_store.get_stream_version(self._stream(aggregate_id))

    async def get_or_raise(self, aggregate_id: UUID) -> TAggregate:
        """
        Get an aggregate, raising if it doesn't exist.

        This is an alias for load() that makes the intent clearer
        in calling code.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            The loaded aggregate

        Raises:
            AggregateNotFoundError: If aggregate doesn't exist
        """
        return await self.load(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        """
        Create a new, empty aggregate instance.

        This does not persist anything - it just creates an in-memory
        aggregate that can have commands applied and then saved.

        Args:
            aggregate_id: ID for the new aggregate

        Returns:
            New aggregate instance with version 0

        Example:
            >>> order = repo.create_new(uuid4())
            >>> order.create(customer_id=customer_id)
            >>> await repo.save(order)
        """
        return self._aggregate_factory(aggregate_id)


__all__ = [
    "AggregateRepositoryQueryMixin",
]
