"""
FullEventStore operations implementation for TenantStoreRouter.
"""

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

    # Stubs for methods provided by RouterResolutionMixin
    def _extract_tenant_id(self, events: Sequence[DomainEvent]) -> UUID | None:
        raise NotImplementedError

    async def _wait_if_paused(self, tenant_id: UUID | None) -> None:
        raise NotImplementedError

    async def _get_write_store(self, tenant_id: UUID | None) -> FullEventStore:
        raise NotImplementedError

    async def _get_read_store(self, tenant_id: UUID) -> FullEventStore:
        raise NotImplementedError

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        """
        Append events, routing to the appropriate store.

        Routes based on tenant_id from events and migration state.

        Args:
            stream: Identity of the stream to append to
            events: Events to append
            expected: Optimistic-concurrency expectation

        Returns:
            AppendResult from the routed store

        Raises:
            ValueError: If events list is empty
            OptimisticLockError: If the routed store's version check fails
            WritePausedError: If writes are paused and timeout exceeded
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        tenant_id = self._extract_tenant_id(events)

        with self._tracer.span(
            "eventsource.router.append",
            {
                ATTR_AGGREGATE_ID: str(stream.aggregate_id),
                ATTR_AGGREGATE_TYPE: stream.category,
                ATTR_TENANT_ID: str(tenant_id) if tenant_id else "none",
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected.kind,
            },
        ):
            # Check for write pause
            await self._wait_if_paused(tenant_id)

            # Get routing
            store = await self._get_write_store(tenant_id)

            return await store.append(stream, events, expected)

    async def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read events across all streams in a category.

        Routes to the appropriate store based on `options.tenant_id`
        when one is given, otherwise reads from the default store.

        Args:
            category: The stream category (e.g. 'Order')
            options: Options for reading (tenant, timestamp, limit)

        Yields:
            EventEnvelope instances in the port's category order
            (storage time, position tie-break, `from_timestamp` inclusive)
        """
        opts = options or CategoryReadOptions()

        with self._tracer.span(
            "eventsource.router.read_category",
            {
                ATTR_AGGREGATE_TYPE: category,
                ATTR_TENANT_ID: str(opts.tenant_id) if opts.tenant_id else "all",
            },
        ):
            if opts.tenant_id:
                store = await self._get_read_store(opts.tenant_id)
            else:
                store = self._default_store

            async for envelope in store.read_category(category, opts):
                yield envelope

    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event exists in any registered store.

        Checks default store first (most common case), then other stores.

        Args:
            event_id: ID of the event to check

        Returns:
            True if event exists in any store
        """
        with self._tracer.span(
            "eventsource.router.event_exists",
            {},
        ):
            # Check default store first (most common)
            if await self._default_store.event_exists(event_id):
                return True

            # Check other stores
            for store_id, store in self._stores.items():
                if store_id == self._default_store_id:
                    continue
                if await store.event_exists(event_id):
                    return True

            return False

    async def get_stream_version(self, stream: StreamId) -> int:
        """
        Get the current version of a stream in the default store.

        Args:
            stream: Identity of the stream

        Returns:
            Current version (0 if the stream doesn't exist)
        """
        return await self._default_store.get_stream_version(stream)

    async def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read events from a specific stream in the default store.

        The stream read carries no tenant, so it cannot be routed by
        tenant; it goes to the default store.

        Args:
            stream: Identity of the stream
            options: Options for reading (direction, version range, limit)

        Yields:
            EventEnvelope instances
        """
        with self._tracer.span(
            "eventsource.router.read_stream",
            {"stream_id": stream.render()},
        ):
            async for envelope in self._default_store.read_stream(stream, options):
                yield envelope

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read the global feed from the appropriate store.

        If `options.tenant_id` is provided, routes to that tenant's store.
        Otherwise, reads from the default store. `from_position` must
        belong to whichever store the read resolves to.

        Args:
            from_position: Read strictly after this position; None for the
                start of the feed
            options: Options for reading (tenant, limit)

        Yields:
            EventEnvelope instances in global feed order
        """
        opts = options or FeedReadOptions()

        with self._tracer.span(
            "eventsource.router.read_all",
            {
                ATTR_TENANT_ID: str(opts.tenant_id) if opts.tenant_id else "all",
                ATTR_POSITION: from_position.to_str() if from_position else "start",
            },
        ):
            if opts.tenant_id:
                store = await self._get_read_store(opts.tenant_id)
            else:
                store = self._default_store

            async for envelope in store.read_all(from_position, opts):
                yield envelope

    async def current_position(self) -> Position | None:
        """
        Get the current global-feed position of the DEFAULT store.

        The returned position belongs to the default store and is NOT
        comparable with any other store's positions -- ordering two
        stores' positions raises `PositionForeignError`.

        Returns:
            The default store's latest position, or None if it is empty
        """
        return await self._default_store.current_position()


__all__ = ["RouterStoreMixin"]
