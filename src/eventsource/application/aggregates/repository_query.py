"""Query and loading mixin for AggregateRepository."""

from __future__ import annotations

import logging
from collections.abc import Iterable
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
        """Load an aggregate from its snapshot (if valid) and event history."""
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: self._aggregate_type}
        with self._tracer.span("eventsource.repository.load", attrs) as span:
            from_version = 0
            snapshot = None

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

            stream = self._stream(aggregate_id)
            options = StreamReadOptions(from_version=from_version + 1) if from_version > 0 else None
            events = [
                envelope.event async for envelope in self._event_store.read_stream(stream, options)
            ]

            if snapshot is None and not events:
                raise AggregateNotFoundError(aggregate_id, self._aggregate_type)

            aggregate = self._aggregate_factory(aggregate_id)

            if snapshot is not None:
                try:
                    aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
                except Exception as e:
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
                    events = [
                        envelope.event async for envelope in self._event_store.read_stream(stream)
                    ]
                    if not events:
                        raise AggregateNotFoundError(aggregate_id, self._aggregate_type) from None
                    aggregate = self._aggregate_factory(aggregate_id)

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

    async def get_or_none(self, aggregate_id: UUID) -> TAggregate | None:
        """Get an aggregate if it exists, or return None if not found."""
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return None

    async def load_many(
        self, aggregate_ids: Iterable[UUID], *, ignore_missing: bool = False
    ) -> dict[UUID, TAggregate]:
        """Load multiple aggregates by ID into a mapping keyed by aggregate ID."""
        results: dict[UUID, TAggregate] = {}
        for agg_id in aggregate_ids:
            if ignore_missing:
                if (aggregate := await self.get_or_none(agg_id)) is not None:
                    results[agg_id] = aggregate
            else:
                results[agg_id] = await self.load(agg_id)
        return results

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        """Load an existing aggregate or create a new empty one."""
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return self._aggregate_factory(aggregate_id)

    async def exists(self, aggregate_id: UUID) -> bool:
        """Check if an aggregate has events in the store."""
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: self._aggregate_type}
        with self._tracer.span("eventsource.repository.exists", attrs) as span:
            exists = await self._event_store.get_stream_version(self._stream(aggregate_id)) > 0
            if span:
                span.set_attribute("exists", exists)
            return exists

    async def get_version(self, aggregate_id: UUID) -> int:
        """Get the current version of an aggregate (0 if it does not exist)."""
        return await self._event_store.get_stream_version(self._stream(aggregate_id))

    async def get_or_raise(self, aggregate_id: UUID) -> TAggregate:
        """Get an aggregate, raising AggregateNotFoundError if it does not exist."""
        return await self.load(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        """Create a new, empty in-memory aggregate instance with version 0."""
        return self._aggregate_factory(aggregate_id)


__all__ = ["AggregateRepositoryQueryMixin"]
