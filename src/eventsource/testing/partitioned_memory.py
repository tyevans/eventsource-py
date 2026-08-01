"""Feed-less in-memory store used to prove the capability-by-omission model.

`PartitionedMemoryStore` implements `EventAppender`, `StreamReader`,
`EventLookup`, and `CategoryQuery` -- but deliberately *not*
`GlobalEventFeed`. Events live in per-stream lists with no shared global
order, so envelopes always carry `position=None` and the class exposes no
`read_all`/`current_position` attributes at all (not even ones that raise).
This is the thing a consumer coded against `GlobalEventFeed` should fail to
type-check against, and the thing `GlobalFeedConformance` should never be
run against.

Kept sqlalchemy-free and independent of any adapter: the expected-version
dispatch below is a deliberate copy of the pattern in
`eventsource.adapters.memory.store.InMemoryEventStore._check_expected`, not an
import from it -- the testing package must not depend on a specific adapter.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.events import DomainEvent
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    ReadDirection,
    StreamReadOptions,
)

# Sentinel ints preserved for OptimisticLockError's expected_version field, which
# predates ExpectedVersion and is still int-typed.
_ANY_SENTINEL = -1
_NO_STREAM_SENTINEL = 0
_STREAM_EXISTS_SENTINEL = -2


class PartitionedMemoryStore:
    """In-memory store with per-stream partitions and no global feed.

    Implements `EventAppender`, `StreamReader`, `EventLookup`, and
    `CategoryQuery`. Does not implement `GlobalEventFeed`: there is no
    `read_all` or `current_position` method, and `EventEnvelope.position`
    is always None.

    Attributes:
        max_append_batch: No batch-size limit is enforced by this store.
    """

    max_append_batch: int | None = None

    def __init__(self) -> None:
        self._streams: dict[str, list[EventEnvelope]] = {}
        self._event_ids: set[UUID] = set()
        self._lock = asyncio.Lock()

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

        async with self._lock:
            key = stream.render()
            existing = self._streams.get(key, [])
            current_version = len(existing)

            self._check_expected(current_version, expected, stream)

            seen_in_batch: set[UUID] = set()
            for event in events:
                if event.event_id in self._event_ids or event.event_id in seen_in_batch:
                    raise DuplicateEventError(
                        f"event_id {event.event_id} already exists in the store"
                    )
                seen_in_batch.add(event.event_id)

            new_envelopes: list[EventEnvelope] = []
            version = current_version
            for event in events:
                version += 1
                new_envelopes.append(
                    EventEnvelope(
                        event=event,
                        stream_id=stream,
                        stream_version=version,
                        position=None,
                        stored_at=datetime.now(UTC),
                    )
                )

            for envelope in new_envelopes:
                self._event_ids.add(envelope.event.event_id)

            self._streams[key] = existing + new_envelopes

            return AppendResult(stream=stream, new_version=version, position=None)

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
        envelopes = list(self._streams.get(stream.render(), []))

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
        envelopes = [
            envelope
            for envelopes in self._streams.values()
            for envelope in envelopes
            if envelope.stream_id.category == category
        ]

        if options.tenant_id is not None:
            envelopes = [
                e for e in envelopes if getattr(e.event, "tenant_id", None) == options.tenant_id
            ]
        if options.from_timestamp is not None:
            envelopes = [e for e in envelopes if e.stored_at >= options.from_timestamp]

        # `position` is always `None` on this adapter (see module docstring),
        # so there is no position key to break `stored_at` ties with. `sorted`
        # is stable, so ties fall back to the order envelopes were collected
        # above: `self._streams` dict-insertion order (i.e. first-append-wins
        # across streams), then per-stream append order. That is a
        # deterministic, if adapter-specific, tie-break -- not `stored_at`
        # collisions resolved by a storage-assigned sequence like the other
        # adapters, but stable and reproducible for a given append sequence.
        envelopes = sorted(envelopes, key=lambda e: e.stored_at)

        if options.limit is not None:
            envelopes = envelopes[: options.limit]

        for envelope in envelopes:
            yield envelope
