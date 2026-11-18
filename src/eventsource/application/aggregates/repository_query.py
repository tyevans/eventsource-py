"""Query and loading mixin for AggregateRepository."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.aggregates._load_guards import (
    assert_owned,
    resolve_empty_tail,
    restore_snapshot_or_fall_back,
)
from eventsource.application.aggregates.snapshotting import read_valid_snapshot
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.domain.tenant_context import get_current_tenant
from eventsource.observability import Tracer
from eventsource.observability.attributes import (  # fmt: skip
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

    def stream_for(self, aggregate_id: UUID) -> StreamId:
        """Stream identity for one aggregate of this repository's type."""
        raise NotImplementedError

    async def load(self, aggregate_id: UUID) -> TAggregate:
        """Load an aggregate from its snapshot (if valid) and event history.

        A snapshot is used only when it matches the requested identity and does
        not claim a version beyond the stream head. Otherwise load replays the
        stream from version 0. A stream with no events raises
        AggregateNotFoundError, even when a snapshot exists.

        When a tenant scope is active, the stream must belong to that tenant:
        every event must carry the active tenant, and a snapshot is used only
        when its tenant matches. A stream owned by another tenant (or with
        untenanted events) raises AggregateNotFoundError, the same error as a
        missing stream (ADR-0113). Without a scope, load is unchanged.
        """
        attrs = {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: self._aggregate_type}
        owner = get_current_tenant()
        with self._tracer.span("eventsource.repository.load", attrs) as span:
            snapshot = None
            if self._snapshot_store is not None:
                snapshot = await read_valid_snapshot(self._snapshot_store, aggregate_id, self._aggregate_type, self._aggregate_factory, tenant=owner)  # fmt: skip
            stream = self.stream_for(aggregate_id)
            from_version = snapshot.version if snapshot is not None else 0
            options = StreamReadOptions(from_version=from_version + 1) if from_version > 0 else None
            events = [env.event async for env in self._event_store.read_stream(stream, options)]

            if snapshot is not None and not events:
                snapshot, events = await resolve_empty_tail(self._event_store, stream, aggregate_id, self._aggregate_type, snapshot)  # fmt: skip

            if snapshot is None and not events:
                raise AggregateNotFoundError(aggregate_id, self._aggregate_type)

            if snapshot is not None:
                if span:
                    span.set_attribute("snapshot.used", True)
                    span.set_attribute("snapshot.version", snapshot.version)
                logger.debug("Using snapshot for %s/%s at version %d", self._aggregate_type, aggregate_id, snapshot.version)  # fmt: skip

            aggregate = self._aggregate_factory(aggregate_id)
            if snapshot is not None and not restore_snapshot_or_fall_back(aggregate, snapshot, self._aggregate_type, aggregate_id):  # fmt: skip
                events = [env.event async for env in self._event_store.read_stream(stream)]
                if not events:
                    raise AggregateNotFoundError(aggregate_id, self._aggregate_type) from None
                aggregate = self._aggregate_factory(aggregate_id)

            # Checked once, after every read path (tail, full replay, restore fallback).
            assert_owned(self._aggregate_type, aggregate_id, events, owner)
            if events:
                aggregate.load_from_history(events)
            if span:
                span.set_attribute("events.replayed", len(events))
                span.set_attribute(ATTR_VERSION, aggregate.version)
            logger.debug("Loaded %s/%s at version %d (snapshot: %s, events replayed: %d)", self._aggregate_type, aggregate_id, aggregate.version, "yes" if snapshot else "no", len(events))  # fmt: skip
            return aggregate

    async def get_or_none(self, aggregate_id: UUID) -> TAggregate | None:
        """Get an aggregate if it exists, or return None if not found."""
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return None

    async def load_many(self, aggregate_ids: Iterable[UUID], *, ignore_missing: bool = False) -> dict[UUID, TAggregate]:  # fmt: skip
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
            exists = await self._event_store.get_stream_version(self.stream_for(aggregate_id)) > 0
            if span:
                span.set_attribute("exists", exists)
            return exists

    async def get_version(self, aggregate_id: UUID) -> int:
        """Get the current version of an aggregate (0 if it does not exist)."""
        return await self._event_store.get_stream_version(self.stream_for(aggregate_id))

    async def get_or_raise(self, aggregate_id: UUID) -> TAggregate:
        """Get an aggregate, raising AggregateNotFoundError if it does not exist."""
        return await self.load(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        """Create a new, empty in-memory aggregate instance with version 0."""
        return self._aggregate_factory(aggregate_id)


__all__ = ["AggregateRepositoryQueryMixin"]
