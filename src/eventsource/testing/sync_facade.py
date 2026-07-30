"""Synchronous façade over a port-shaped async event store.

`SyncStoreFacade` owns a private event loop and drives an async
`FullEventStore`-shaped adapter synchronously. It exists for test code
(notably the hypothesis stateful conformance machine in
`conformance_ports/stateful.py`) that wants a plain, blocking call
surface without pulling in the older `sync/adapter.py` ABC-oriented
adapter.

Mirrors the loop-management approach of `eventsource.sync.adapter`
(one dedicated loop, `run_until_complete` per call) but targets the
`ports` protocols instead of the legacy `EventStore` ABC, and drains
async iterators internally rather than exposing them.

Kept sqlalchemy-free: only `ports`, stdlib `asyncio`, and typing.
"""

import asyncio
from collections.abc import Callable, Sequence
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.events import DomainEvent
from eventsource.ports import (
    AppendResult,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
    collect,
)


class SyncStoreFacade:
    """Blocking, port-shaped wrapper around an async event store.

    Owns both the wrapped store's lifecycle and a dedicated event loop
    for the lifetime of the facade; call `close()` when done to release
    them both.
    """

    def __init__(
        self,
        store: FullEventStore,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] = asyncio.new_event_loop,
    ) -> None:
        self._store = store
        self._loop = loop_factory()

    def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        return self._loop.run_until_complete(self._store.append(stream, events, expected))

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> list[EventEnvelope]:
        return self._loop.run_until_complete(collect(self._store.read_stream(stream, options)))

    def get_stream_version(self, stream: StreamId) -> int:
        return self._loop.run_until_complete(self._store.get_stream_version(stream))

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> list[EventEnvelope]:
        return self._loop.run_until_complete(collect(self._store.read_all(from_position, options)))

    def current_position(self) -> Position | None:
        return self._loop.run_until_complete(self._store.current_position())

    def event_exists(self, event_id: UUID) -> bool:
        return self._loop.run_until_complete(self._store.event_exists(event_id))

    def close(self) -> None:
        """Close the underlying store (if it has a close()), then release the private loop. Idempotent."""
        if not self._loop.is_closed():
            close = getattr(self._store, "close", None)
            if close is not None:
                self._loop.run_until_complete(close())
            self._loop.close()


__all__ = ["SyncStoreFacade"]
