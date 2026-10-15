"""In-process, non-persistent adapter implementing the five store ports."""

import bisect
import threading
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters._common import check_expected, check_registered
from eventsource.domain import (
    DomainEvent,
    DuplicateEventError,
    EventRegistry,
    StreamId,
    default_registry,
)
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
    """In-memory implementation of `FullEventStore` (structural conformance only)."""

    def __init__(self, store_id: str = "memory", *, event_registry: EventRegistry | None = None) -> None:  # fmt: skip
        self._store_id = store_id
        self._event_registry = event_registry or default_registry
        self._events: list[EventEnvelope] = []
        self._streams: dict[str, list[int]] = {}
        self._categories: dict[str, list[int]] = {}
        self._event_ids: set[UUID] = set()
        self._lock = threading.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id  # fmt: skip

    def __len__(self) -> int:
        return len(self._events)  # fmt: skip

    def __bool__(self) -> bool:
        return True  # fmt: skip

    async def get_event_count(self) -> int:
        return len(self._events)  # fmt: skip

    async def close(self) -> None:
        return None  # fmt: skip

    async def __aenter__(self) -> "InMemoryEventStore":
        return self  # fmt: skip

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()  # fmt: skip

    def clear(self) -> None:
        with self._lock:
            for col in (self._events, self._streams, self._categories, self._event_ids):
                col.clear()

    def _position_of(self, index: int) -> Position:
        return Position(store_id=self._store_id, key=(index + 1,))

    def _validate_position(self, position: Position) -> None:
        if position.store_id != self._store_id:
            raise PositionForeignError(f"cannot order positions from {self._store_id!r} and {position.store_id!r}")  # fmt: skip

    async def append(self, stream: StreamId, events: Sequence[DomainEvent], expected: ExpectedVersion) -> AppendResult:  # fmt: skip
        if not events:
            raise ValueError("cannot append an empty batch of events")
        check_registered(events, self._event_registry)
        with self._lock:
            key = stream.render()
            current_version = len(self._streams.get(key, ()))
            check_expected(current_version, expected, stream)
            seen_in_batch: set[UUID] = set()
            for event in events:
                if event.event_id in self._event_ids or event.event_id in seen_in_batch:
                    raise DuplicateEventError(f"event_id {event.event_id} already exists in the store")  # fmt: skip
                seen_in_batch.add(event.event_id)

            now = datetime.now(UTC)
            if self._events and now < self._events[-1].stored_at:
                now = self._events[-1].stored_at

            new_indexes: list[int] = []
            for version, event in enumerate(events, start=current_version + 1):
                idx = len(self._events)
                self._events.append(EventEnvelope(event, stream, version, self._position_of(idx), now))  # fmt: skip
                self._event_ids.add(event.event_id)
                new_indexes.append(idx)

            self._streams.setdefault(key, []).extend(new_indexes)
            self._categories.setdefault(stream.category, []).extend(new_indexes)
            return AppendResult(stream, current_version + len(events), self._position_of(new_indexes[0]))  # fmt: skip

    async def read_stream(self, stream: StreamId, options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        opts = options or StreamReadOptions()
        if opts.limit is not None and opts.limit <= 0:
            return
        events_snapshot, stream_list = self._events, self._streams.get(stream.render(), ())
        total = len(stream_list)
        start = max(0, opts.from_version - 1) if opts.from_version is not None else 0
        end = min(total, max(0, opts.to_version)) if opts.to_version is not None else total
        if start >= end:
            return
        if opts.direction == ReadDirection.BACKWARD:
            start = max(start, end - opts.limit) if opts.limit is not None else start
            indices: Sequence[int] = list(reversed(stream_list[start:end]))
        else:
            end = min(end, start + opts.limit) if opts.limit is not None else end
            indices = stream_list[start:end]
        for i in indices:
            yield events_snapshot[i]

    async def get_stream_version(self, stream: StreamId) -> int:
        return len(self._streams.get(stream.render(), ()))  # fmt: skip

    async def event_exists(self, event_id: UUID) -> bool:
        return event_id in self._event_ids  # fmt: skip

    async def _iter_filtered(self, indices: Sequence[int], limit: int | None, tenant_id: UUID | None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        if limit is not None and limit <= 0:
            return
        events_snapshot, yielded = self._events, 0
        for i in indices:
            if limit is not None and yielded >= limit:
                break
            envelope = events_snapshot[i]
            if tenant_id is not None and getattr(envelope.event, "tenant_id", None) != tenant_id:
                continue
            yielded += 1
            yield envelope

    async def read_all(self, from_position: Position | None = None, options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        opts = options or FeedReadOptions()
        if opts.limit is not None and opts.limit <= 0:
            return
        if from_position is not None:
            self._validate_position(from_position)
        agg = opts.aggregate_type
        indices: Sequence[int] = self._categories.get(agg, ()) if agg is not None else range(len(self._events))  # fmt: skip
        key_fn = lambda i: (i + 1,)  # noqa: E731
        start = bisect.bisect_right(indices, from_position.key, key=key_fn) if from_position else 0
        async for envelope in self._iter_filtered(indices[start:], opts.limit, opts.tenant_id):
            yield envelope

    async def current_position(self) -> Position | None:
        return self._events[-1].position if self._events else None  # fmt: skip

    async def read_category(self, category: str, options: CategoryReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        opts = options or CategoryReadOptions()
        events_snapshot, indices = self._events, self._categories.get(category, ())
        ts = opts.from_timestamp
        start = bisect.bisect_left(indices, ts, key=lambda i: events_snapshot[i].stored_at) if ts else 0  # fmt: skip
        async for envelope in self._iter_filtered(indices[start:], opts.limit, opts.tenant_id):
            yield envelope
