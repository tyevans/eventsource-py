"""Store port protocols (Clean Architecture output ports).

Five narrow, composable protocols describing the capabilities an event
store adapter may offer, plus `FullEventStore` — the union of all five —
and `collect`, a convenience helper for draining an async iterator of
`EventEnvelope` into a list.

None of these protocols is `@runtime_checkable`; add that only when a
consumer needs `isinstance` checks against them.
"""

from collections.abc import AsyncIterator, Sequence
from typing import Protocol
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports.envelopes import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    FeedReadOptions,
    StreamReadOptions,
)
from eventsource.ports.positions import ExpectedVersion, Position


class EventAppender(Protocol):
    """Port for appending events to a stream with optimistic concurrency."""

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult: ...


class StreamReader(Protocol):
    """Port for reading a single stream's events."""

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]: ...

    async def get_stream_version(self, stream: StreamId) -> int: ...


class EventLookup(Protocol):
    """Port for checking whether a specific event has been stored."""

    async def event_exists(self, event_id: UUID) -> bool: ...


class GlobalEventFeed(Protocol):
    """Port for reading the store's global, ordered event feed."""

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]: ...

    async def current_position(self) -> Position | None: ...


class CategoryQuery(Protocol):
    """Port for reading events across all streams in a category."""

    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]: ...


class AggregateStore(EventAppender, StreamReader, Protocol):
    """What an aggregate repository needs: append plus stream read/version.

    Narrower than `FullEventStore` on purpose -- a repository never reads
    the global feed, never queries a category, and never probes for an
    individual event id, so it must not type-require those capabilities
    (ISP; see `.claude/rules/architecture.md`).
    """


class FullEventStore(
    EventAppender,
    StreamReader,
    EventLookup,
    GlobalEventFeed,
    CategoryQuery,
    Protocol,
):
    """Union of all five store capability ports."""


async def collect(it: AsyncIterator[EventEnvelope]) -> list[EventEnvelope]:
    """Drain an async iterator into a list."""
    return [item async for item in it]


__all__ = [
    "AggregateStore",
    "CategoryQuery",
    "EventAppender",
    "EventLookup",
    "FullEventStore",
    "GlobalEventFeed",
    "StreamReader",
    "collect",
]
