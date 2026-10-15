"""FullEventStore operations implementation for TenantStoreRouter."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_POSITION,
    ATTR_TENANT_ID,
)
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
)

if TYPE_CHECKING:
    from eventsource.observability import Tracer


class RouterStoreMixin:
    """Mixin implementing FullEventStore operations through routed stores."""

    _tracer: Tracer
    _default_store: FullEventStore
    _default_store_id: str
    _stores: dict[str, FullEventStore]

    def _extract_tenant_id(self, events: Sequence[DomainEvent]) -> UUID | None:
        raise NotImplementedError

    async def _wait_if_paused(self, tenant_id: UUID | None) -> None:
        raise NotImplementedError

    async def _get_write_store(self, tenant_id: UUID | None) -> FullEventStore:
        raise NotImplementedError

    async def _get_read_store(self, tenant_id: UUID) -> FullEventStore:
        raise NotImplementedError

    async def append(
        self, stream: StreamId, events: Sequence[DomainEvent], expected: ExpectedVersion
    ) -> AppendResult:
        """Append events, routing to the appropriate store based on tenant_id and migration state."""
        if not events:
            raise ValueError("Cannot append empty event list")
        tenant_id = self._extract_tenant_id(events)
        attrs = {
            ATTR_AGGREGATE_ID: str(stream.aggregate_id),
            ATTR_AGGREGATE_TYPE: stream.category,
            ATTR_TENANT_ID: str(tenant_id) if tenant_id else "none",
            ATTR_EVENT_COUNT: len(events),
            ATTR_EXPECTED_VERSION: expected.kind,
        }
        with self._tracer.span("eventsource.router.append", attrs):
            await self._wait_if_paused(tenant_id)
            store = await self._get_write_store(tenant_id)
            return await store.append(stream, events, expected)

    async def read_category(
        self, category: str, options: CategoryReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        """Read events across all streams in a category, routed by `options.tenant_id` if given."""
        opts = options or CategoryReadOptions()
        attrs = {
            ATTR_AGGREGATE_TYPE: category,
            ATTR_TENANT_ID: str(opts.tenant_id) if opts.tenant_id else "all",
        }
        with self._tracer.span("eventsource.router.read_category", attrs):
            store = (
                await self._get_read_store(opts.tenant_id)
                if opts.tenant_id
                else self._default_store
            )
            async for envelope in store.read_category(category, opts):
                yield envelope

    async def event_exists(self, event_id: UUID) -> bool:
        """Check if an event exists in any registered store (default store checked first)."""
        with self._tracer.span("eventsource.router.event_exists", {}):
            if await self._default_store.event_exists(event_id):
                return True
            for store_id, store in self._stores.items():
                if store_id != self._default_store_id and await store.event_exists(event_id):
                    return True
            return False

    async def get_stream_version(self, stream: StreamId) -> int:
        """Get the current version of a stream in the default store (0 if absent)."""
        return await self._default_store.get_stream_version(stream)

    async def read_stream(
        self, stream: StreamId, options: StreamReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        """Read events from a specific stream in the default store."""
        with self._tracer.span("eventsource.router.read_stream", {"stream_id": stream.render()}):
            async for envelope in self._default_store.read_stream(stream, options):
                yield envelope

    async def read_all(
        self, from_position: Position | None = None, options: FeedReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        """Read the global feed from the tenant's store if `options.tenant_id` is set, else default store."""
        opts = options or FeedReadOptions()
        attrs = {
            ATTR_TENANT_ID: str(opts.tenant_id) if opts.tenant_id else "all",
            ATTR_POSITION: from_position.to_str() if from_position else "start",
        }
        with self._tracer.span("eventsource.router.read_all", attrs):
            store = (
                await self._get_read_store(opts.tenant_id)
                if opts.tenant_id
                else self._default_store
            )
            async for envelope in store.read_all(from_position, opts):
                yield envelope

    async def current_position(self) -> Position | None:
        """Get the current global-feed position of the DEFAULT store, or None if empty."""
        return await self._default_store.current_position()


__all__ = ["RouterStoreMixin"]
