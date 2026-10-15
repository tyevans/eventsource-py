"""In-process, non-persistent adapter implementing the five store ports.

Suitable for testing and development only. All state lives in process
memory and is lost on restart. Not distributed-safe.
"""

import bisect
import threading
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters._common import check_expected, check_registered
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, default_registry
from eventsource.domain.exceptions import DuplicateEventError
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    PositionForeignError,
    ReadDirection,
    StreamReadOptions,
)


class InMemoryEventStore:
    """In-memory implementation of `FullEventStore` (structural conformance only).

    `store_id` defaults to `"memory"` for every instance; give each instance its
    own `store_id` when testing multiple stores concurrently.
    """

    def __init__(
        self,
        store_id: str = "memory",
        *,
        event_registry: EventRegistry | None = None,
    ) -> None:
        self._store_id = store_id
        self._event_registry = event_registry or default_registry
        self._events: list[EventEnvelope] = []
        self._streams: dict[str, list[int]] = {}
        self._categories: dict[str, list[int]] = {}
        self._by_category: dict[str, list[int]] = self._categories
        self._event_ids: set[UUID] = set()
        # Plain threading.Lock is safe across fresh loops from SyncEventStoreAdapter.
        self._lock = threading.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id

    async def close(self) -> None:
        """No-op resource release for lifecycle parity."""
        return None

    async def __aenter__(self) -> "InMemoryEventStore":
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    def _position_of(self, index: int) -> Position:
        """1-based global position for the given 0-based `_events` index."""
        return Position(store_id=self._store_id, key=(index + 1,))

    def _validate_position(self, position: Position) -> None:
        """Raise `PositionForeignError` if `position` belongs to another store."""
        if position.store_id != self._store_id:
            raise PositionForeignError(
                f"cannot order positions from {self._store_id!r} and {position.store_id!r}"
            )

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        if not events:
            raise ValueError("cannot append an empty batch of events")

        # Validate event registration for parity with SQLite/PostgreSQL stores.
        check_registered(events, self._event_registry)

        with self._lock:
            key = stream.render()
            indexes = self._streams.get(key, [])
            current_version = len(indexes)
            check_expected(current_version, expected, stream)

            seen_in_batch: set[UUID] = set()
            for event in events:
                if event.event_id in self._event_ids or event.event_id in seen_in_batch:
                    raise DuplicateEventError(
                        f"event_id {event.event_id} already exists in the store"
                    )
                seen_in_batch.add(event.event_id)

            now = datetime.now(UTC)
            if self._events and now < self._events[-1].stored_at:
                now = self._events[-1].stored_at

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
                    stored_at=now,
                )
                self._events.append(envelope)
                self._event_ids.add(event.event_id)
                new_indexes.append(index)
                if first_position is None:
                    first_position = envelope.position

            self._streams[key] = indexes + new_indexes
            self._categories[stream.category] = (
                self._categories.get(stream.category, []) + new_indexes
            )
            return AppendResult(stream=stream, new_version=version, position=first_position)

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_stream(stream, options or StreamReadOptions())

    async def _do_read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        if options.limit is not None and options.limit <= 0:
            return

        events_snapshot = self._events
        indexes = self._streams.get(stream.render(), ())
        if not indexes:
            return

        start = (
            bisect.bisect_left(
                indexes,
                options.from_version,
                key=lambda i: events_snapshot[i].stream_version,
            )
            if options.from_version is not None
            else 0
        )
        end = (
            bisect.bisect_right(
                indexes,
                options.to_version,
                key=lambda i: events_snapshot[i].stream_version,
            )
            if options.to_version is not None
            else len(indexes)
        )
        if start >= end:
            return

        if options.direction == ReadDirection.BACKWARD:
            if options.limit is not None:
                start = max(start, end - options.limit)
            for i in reversed(indexes[start:end]):
                yield events_snapshot[i]
        else:
            if options.limit is not None:
                end = min(end, start + options.limit)
            for i in indexes[start:end]:
                yield events_snapshot[i]

    async def get_stream_version(self, stream: StreamId) -> int:
        return len(self._streams.get(stream.render(), ()))

    async def event_exists(self, event_id: UUID) -> bool:
        return event_id in self._event_ids

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_all(from_position, options or FeedReadOptions())

    async def _do_read_all(
        self,
        from_position: Position | None,
        options: FeedReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        if options.limit is not None and options.limit <= 0:
            return

        events_snapshot = self._events
        if from_position is not None:
            self._validate_position(from_position)

        # Filtered on stream category for parity with SQL adapters.
        if options.aggregate_type is not None:
            cat_indices: Sequence[int] = self._categories.get(options.aggregate_type, ())
            start = (
                bisect.bisect_right(cat_indices, from_position.key, key=lambda i: (i + 1,))
                if from_position is not None
                else 0
            )
            candidate_indices: Sequence[int] = cat_indices[start:]
        else:
            all_indices = range(len(events_snapshot))
            start = (
                bisect.bisect_right(all_indices, from_position.key, key=lambda i: (i + 1,))
                if from_position is not None
                else 0
            )
            candidate_indices = range(start, len(events_snapshot))

        yielded = 0
        for i in candidate_indices:
            if options.limit is not None and yielded >= options.limit:
                break
            envelope = events_snapshot[i]
            if (
                options.tenant_id is not None
                and getattr(envelope.event, "tenant_id", None) != options.tenant_id
            ):
                continue
            yielded += 1
            yield envelope

    async def current_position(self) -> Position | None:
        return self._events[-1].position if self._events else None

    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_category(category, options or CategoryReadOptions())

    async def _do_read_category(
        self,
        category: str,
        options: CategoryReadOptions,
    ) -> AsyncIterator[EventEnvelope]:
        if options.limit is not None and options.limit <= 0:
            return

        events_snapshot = self._events
        indices: Sequence[int] = self._categories.get(category, ())
        if not indices:
            return

        start = (
            bisect.bisect_left(
                indices,
                options.from_timestamp,
                key=lambda i: events_snapshot[i].stored_at,
            )
            if options.from_timestamp is not None
            else 0
        )

        yielded = 0
        for i in indices[start:]:
            if options.limit is not None and yielded >= options.limit:
                break
            envelope = events_snapshot[i]
            if (
                options.tenant_id is not None
                and getattr(envelope.event, "tenant_id", None) != options.tenant_id
            ):
                continue
            yielded += 1
            yield envelope
