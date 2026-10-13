"""Shared fixtures, test doubles, and helper functions for replay tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.domain.stream_id import StreamId
from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions
from eventsource.ports.positions import ExpectedVersion

if TYPE_CHECKING:
    from eventsource.ports.positions import Position

TENANT_A = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
TENANT_B = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"


@register_event(registry=_REGISTRY)
class InvoiceIssued(DomainEvent):
    aggregate_type: str = "Invoice"


class Collecting:
    """A subscriber that records what it was handed."""

    def __init__(self) -> None:
        self.seen: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.seen.append(event)


class RejectsEverything:
    """A subscriber that refuses everything, with a distinguishable error."""

    def __init__(self, message: str) -> None:
        self._message = message

    async def handle(self, event: DomainEvent) -> None:
        raise RuntimeError(self._message)


class RejectsOne:
    """Rejects a single named event id and applies the rest."""

    def __init__(self, poison: UUID) -> None:
        self._poison = poison
        self.seen: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        if event.event_id == self._poison:
            raise ValueError("poison")
        self.seen.append(event)


class RecordingFeed:
    """A `GlobalEventFeed` that remembers how it was asked."""

    def __init__(self, inner: InMemoryEventStore) -> None:
        self._inner = inner
        self.calls: list[tuple[Position | None, FeedReadOptions | None]] = []

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        self.calls.append((from_position, options))
        return self._inner.read_all(from_position, options)

    async def current_position(self) -> Position | None:
        return await self._inner.current_position()


class NonAdvancingFeed:
    """The hang `max_events` exists to turn into a failure.

    An adapter whose cursor never advances re-yields forever. Left alone that
    is an infinite loop, and a hang in CI reads as infrastructure trouble and
    gets retried rather than investigated.
    """

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        while True:
            yield _envelope(OrderPlaced(aggregate_id=uuid4()))

    async def current_position(self) -> Position | None:
        return None


class PositionlessFeed:
    """A feed whose envelopes carry no position.

    `EventEnvelope.position` is `Position | None`, and `ReplayFailure` keeps it
    that way rather than asserting -- a feedless store is a documented case,
    and the rebuild path's whole job is not to crash. Anything deriving a
    count from `position` therefore has to cope with `None` repeating.
    """

    def __init__(self, events: Sequence[DomainEvent]) -> None:
        self._events = events

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        for event in self._events:
            yield _envelope(event)

    async def current_position(self) -> Position | None:
        return None


def _envelope(event: DomainEvent) -> EventEnvelope:
    return EventEnvelope(
        event=event,
        stream_id=StreamId(event.aggregate_id, event.aggregate_type),
        stream_version=1,
        position=None,
        stored_at=datetime.now(UTC),
    )


async def _append(store: InMemoryEventStore, event: DomainEvent) -> None:
    await store.append(
        StreamId(event.aggregate_id, event.aggregate_type),
        [event],
        ExpectedVersion.no_stream(),
    )


async def _store_with(*events: DomainEvent) -> InMemoryEventStore:
    store = InMemoryEventStore(event_registry=_REGISTRY)
    for event in events:
        await _append(store, event)
    return store
