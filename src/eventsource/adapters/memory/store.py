"""In-process, non-persistent adapter implementing the five store ports.

Suitable for testing and development only. All state lives in process
memory and is lost on restart. Not distributed-safe.
"""

import threading
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.events import DomainEvent
from eventsource.events.registry import EventRegistry
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    ReadDirection,
    StreamReadOptions,
)

# Sentinel ints preserved for OptimisticLockError's expected_version field, which
# predates ExpectedVersion and is still int-typed. The values match the integer
# sentinels the store surface has always reported for message fidelity.
_ANY_SENTINEL = -1
_NO_STREAM_SENTINEL = 0
_STREAM_EXISTS_SENTINEL = -2


class InMemoryEventStore:
    """In-memory implementation of `FullEventStore`.

    Structural conformance only -- no inheritance from the port protocols.

    Attributes:
        max_append_batch: No batch-size limit is enforced by this adapter.
    """

    max_append_batch: int | None = None

    def __init__(
        self,
        store_id: str = "memory",
        *,
        event_registry: EventRegistry | None = None,
    ) -> None:
        self._store_id = store_id
        self._event_registry = event_registry
        self._events: list[EventEnvelope] = []
        self._streams: dict[str, list[int]] = {}
        self._event_ids: set[UUID] = set()
        # threading.Lock, not asyncio.Lock: SyncEventStoreAdapter runs each
        # call in a fresh event loop (`asyncio.run`), so an asyncio.Lock
        # acquired under one loop raises RuntimeError when awaited from
        # another. The critical section below never awaits, so a plain
        # threading.Lock (safe across loops and threads) is sufficient.
        self._lock = threading.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id

    def _position_of(self, index: int) -> Position:
        """1-based global position for the given 0-based `_events` index."""
        return Position(store_id=self._store_id, key=(index + 1,))

    def _check_expected(self, current: int, expected: ExpectedVersion, stream: StreamId) -> None:
        if expected.kind == "any":
            return
        if expected.kind == "no_stream":
            if current != 0:
                raise OptimisticLockError(stream.aggregate_id, _NO_STREAM_SENTINEL, current)
            return
        if expected.kind == "stream_exists":
            if current == 0:
                raise OptimisticLockError(stream.aggregate_id, _STREAM_EXISTS_SENTINEL, current)
            return
        if expected.kind == "exact":
            if current != expected.version:
                raise OptimisticLockError(stream.aggregate_id, expected.version or 0, current)
            return
        raise ValueError(f"unknown ExpectedVersion kind: {expected.kind!r}")

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")

        with self._lock:
            key = stream.render()
            indexes = self._streams.get(key, [])
            current_version = len(indexes)

            self._check_expected(current_version, expected, stream)

            seen_in_batch: set[UUID] = set()
            for event in events:
                if event.event_id in self._event_ids or event.event_id in seen_in_batch:
                    raise DuplicateEventError(
                        f"event_id {event.event_id} already exists in the store"
                    )
                seen_in_batch.add(event.event_id)

            new_indexes: list[int] = []
            first_position: Position | None = None
            version = current_version
            for event in events:
                version += 1
                index = len(self._events)
                envelope = EventEnvelope(
                    event=event,
                    stream_id=stream,
                    stream_version=version,
                    position=self._position_of(index),
                    stored_at=datetime.now(UTC),
                )
                self._events.append(envelope)
                self._event_ids.add(event.event_id)
                new_indexes.append(index)
                if first_position is None:
                    first_position = envelope.position

            self._streams[key] = indexes + new_indexes

            return AppendResult(stream=stream, new_version=version, position=first_position)

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        opts = options or StreamReadOptions()
        return self._do_read_stream(stream, opts)

    async def _do_read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        events_snapshot = self._events
        indexes = list(self._streams.get(stream.render(), []))
        envelopes = [events_snapshot[i] for i in indexes]

        if options.from_version is not None:
            envelopes = [e for e in envelopes if e.stream_version >= options.from_version]
        if options.to_version is not None:
            envelopes = [e for e in envelopes if e.stream_version <= options.to_version]

        if options.direction == ReadDirection.BACKWARD:
            envelopes = list(reversed(envelopes))

        if options.limit is not None:
            envelopes = envelopes[: options.limit]

        for envelope in envelopes:
            yield envelope

    async def get_stream_version(self, stream: StreamId) -> int:
        return len(self._streams.get(stream.render(), []))

    async def event_exists(self, event_id: UUID) -> bool:
        return event_id in self._event_ids

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        opts = options or FeedReadOptions()
        return self._do_read_all(from_position, opts)

    async def _do_read_all(
        self,
        from_position: Position | None,
        options: FeedReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        envelopes = list(self._events)

        if from_position is not None:
            envelopes = [
                e for e in envelopes if e.position is not None and e.position > from_position
            ]

        if options.tenant_id is not None:
            envelopes = [
                e for e in envelopes if getattr(e.event, "tenant_id", None) == options.tenant_id
            ]

        if options.limit is not None:
            envelopes = envelopes[: options.limit]

        for envelope in envelopes:
            yield envelope

    async def current_position(self) -> Position | None:
        if not self._events:
            return None
        return self._events[-1].position

    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        opts = options or CategoryReadOptions()
        return self._do_read_category(category, opts)

    async def _do_read_category(
        self,
        category: str,
        options: CategoryReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        envelopes = [e for e in self._events if e.stream_id.category == category]

        if options.tenant_id is not None:
            envelopes = [
                e for e in envelopes if getattr(e.event, "tenant_id", None) == options.tenant_id
            ]
        if options.from_timestamp is not None:
            envelopes = [e for e in envelopes if e.stored_at >= options.from_timestamp]

        # `stored_at` alone can tie within a batch (all events in one
        # `append()` call share the same `datetime.now(UTC)` snapshot); the
        # position key breaks the tie deterministically, mirroring the
        # sqlite/postgresql adapters' `created_at, global_position` order.
        envelopes = sorted(
            envelopes, key=lambda e: (e.stored_at, e.position.key if e.position else ())
        )

        if options.limit is not None:
            envelopes = envelopes[: options.limit]

        for envelope in envelopes:
            yield envelope
