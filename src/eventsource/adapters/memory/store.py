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

_ANY = ExpectedVersion.any_()


class InMemoryEventStore:
    """In-memory implementation of `FullEventStore` (structural conformance only)."""

    def __init__(self, store_id: str = "memory", *, event_registry: EventRegistry | None = None) -> None:  # fmt: skip
        self._store_id, self._event_registry = store_id, event_registry or default_registry
        self._events: list[EventEnvelope] = []
        self._streams, self._categories = dict[str, list[int]](), dict[str, list[int]]()
        self._event_ids, self._lock = set[UUID](), threading.Lock()

    @property
    def store_id(self) -> str:
        return self._store_id  # fmt: skip

    def __len__(self) -> int:
        return len(self._events)  # fmt: skip

    def __bool__(self) -> bool:
        return True  # fmt: skip

    def __contains__(self, target: object) -> bool:
        uid = target if isinstance(target, UUID) else target.event_id if isinstance(target, DomainEvent) else None  # fmt: skip
        return uid in self._event_ids if uid is not None else False

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

    async def append(self, stream: StreamId, events: DomainEvent | Sequence[DomainEvent], expected: ExpectedVersion = _ANY) -> AppendResult:  # fmt: skip
        batch: Sequence[DomainEvent] = (events,) if isinstance(events, DomainEvent) else events
        if not batch:
            raise ValueError("cannot append an empty batch of events")
        check_registered(batch, self._event_registry)
        with self._lock:
            key = stream.render()
            current_version = len(self._streams.get(key, ()))
            check_expected(current_version, expected, stream)
            seen_in_batch: set[UUID] = set()
            for event in batch:
                if event.event_id in self._event_ids or event.event_id in seen_in_batch:
                    raise DuplicateEventError(f"event_id {event.event_id} already exists in the store")  # fmt: skip
                seen_in_batch.add(event.event_id)
            now = max(datetime.now(UTC), self._events[-1].stored_at) if self._events else datetime.now(UTC)  # fmt: skip
            base = len(self._events)
            for offset, event in enumerate(batch, start=1):
                pos = Position(store_id=self._store_id, key=(base + offset,))
                self._events.append(EventEnvelope(event, stream, current_version + offset, pos, now))  # fmt: skip
                self._event_ids.add(event.event_id)
            self._streams.setdefault(key, []).extend(range(base, len(self._events)))
            self._categories.setdefault(stream.category, []).extend(range(base, len(self._events)))
            return AppendResult(stream, current_version + len(batch), Position(self._store_id, (base + 1,)))  # fmt: skip

    async def read_stream(self, stream: StreamId, options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        opts = options or StreamReadOptions()
        events_snapshot, stream_list = self._events, self._streams.get(stream.render(), ())
        total = len(stream_list)
        start = max(0, opts.from_version - 1) if opts.from_version is not None else 0
        end = min(total, max(0, opts.to_version)) if opts.to_version is not None else total
        if (opts.limit is not None and opts.limit <= 0) or start >= end:
            return
        rev = opts.direction == ReadDirection.BACKWARD
        start = max(start, end - opts.limit) if rev and opts.limit is not None else start
        end = min(end, start + opts.limit) if not rev and opts.limit is not None else end
        for i in reversed(stream_list[start:end]) if rev else stream_list[start:end]:
            yield events_snapshot[i]

    async def get_stream_version(self, stream: StreamId) -> int:
        return len(self._streams.get(stream.render(), ()))  # fmt: skip

    async def stream_exists(self, stream: StreamId) -> bool:
        return bool(self._streams.get(stream.render()))  # fmt: skip

    async def event_exists(self, event_id: UUID) -> bool:
        return event_id in self._event_ids  # fmt: skip

    async def _iter_filtered(self, indices: Sequence[int], limit: int | None, tenant_id: UUID | None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        events_snapshot, yielded = self._events, 0
        for i in indices if limit is None or limit > 0 else ():
            if limit is not None and yielded >= limit:
                break
            if tenant_id is None or getattr(events_snapshot[i].event, "tenant_id", None) == tenant_id:  # fmt: skip
                yielded += 1
                yield events_snapshot[i]

    async def read_all(self, from_position: Position | None = None, options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        opts = options or FeedReadOptions()
        if from_position is not None and from_position.store_id != self._store_id:
            raise PositionForeignError(f"cannot order positions from {self._store_id!r} and {from_position.store_id!r}")  # fmt: skip
        indices: Sequence[int] = self._categories.get(opts.aggregate_type, ()) if opts.aggregate_type is not None else range(len(self._events))  # fmt: skip
        start = bisect.bisect_right(indices, from_position.key, key=lambda i: (i + 1,)) if from_position else 0  # fmt: skip
        async for envelope in self._iter_filtered(indices[start:], opts.limit, opts.tenant_id):
            yield envelope

    async def current_position(self) -> Position | None:
        return self._events[-1].position if self._events else None  # fmt: skip

    async def read_category(self, category: str, options: CategoryReadOptions | None = None) -> AsyncIterator[EventEnvelope]:  # fmt: skip
        opts, events_snapshot, indices = options or CategoryReadOptions(), self._events, self._categories.get(category, ())  # fmt: skip
        start = bisect.bisect_left(indices, opts.from_timestamp, key=lambda i: events_snapshot[i].stored_at) if opts.from_timestamp else 0  # fmt: skip
        async for envelope in self._iter_filtered(indices[start:], opts.limit, opts.tenant_id):
            yield envelope
