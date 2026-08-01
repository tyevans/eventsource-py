"""Synchronous façade over a port-shaped async event store.

`SyncStoreFacade` owns a private event loop and drives an async
`FullEventStore`-shaped adapter synchronously. It exists for test code
(notably the hypothesis stateful conformance machine in
`conformance_ports/stateful.py`) that wants a plain, blocking call
surface.

Both this facade and `eventsource.adapters.sync.adapter.SyncEventStoreAdapter` drive
a port-shaped `FullEventStore` synchronously. The split is lifecycle: this
facade owns one private loop for its lifetime and has no timeouts, which
suits test machinery; the adapter runs `asyncio.run` per call, falls back to
a threadpool when a loop is already running, and enforces a timeout, which
suits production sync callers.

Kept sqlalchemy-free: only `ports`, stdlib `asyncio`, and typing.
"""

import asyncio
from collections.abc import Callable, Sequence
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import (
    AppendResult,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
    SupportsClose,
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
        """Close the underlying store (if it implements SupportsClose), then release the private loop. Idempotent."""
        if not self._loop.is_closed():
            if isinstance(self._store, SupportsClose):
                self._loop.run_until_complete(self._store.close())
            self._loop.close()


__all__ = ["SyncStoreFacade"]
