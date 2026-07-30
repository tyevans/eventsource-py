"""Legacy `EventStore` compatibility wrapper over the new `FullEventStore` ports.

`LegacyStoreAdapter` implements the OLD `stores.interface.EventStore` ABC by
delegating to any adapter that structurally satisfies the new
`ports.FullEventStore` protocol (e.g. `adapters.memory.MemoryEventStore`,
`adapters.sqlite.SQLiteEventStore`, `adapters.postgresql.PostgreSQLEventStore`).

This lets old call sites (`append_events`, `get_events`, `read_all`, ...)
keep working unmodified against the new ring architecture, translating
old-style int versions/positions to and from the new value objects.
"""

from collections.abc import AsyncIterator
from datetime import datetime
from uuid import UUID

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.ports import FullEventStore
from eventsource.ports.envelopes import CategoryReadOptions, FeedReadOptions, StreamReadOptions
from eventsource.ports.positions import ExpectedVersion as PortExpectedVersion
from eventsource.ports.positions import Position
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)


def _expected_from_int(expected_version: int) -> PortExpectedVersion:
    """Map an old int `expected_version` to the new `ExpectedVersion` VO.

    The old class constants (`ExpectedVersion.ANY`, `.NO_STREAM`,
    `.STREAM_EXISTS`) map BY NAME, not by numeric coincidence, per the
    spec's Compatibility Layer section.
    """
    if expected_version == ExpectedVersion.ANY:
        return PortExpectedVersion.any_()
    if expected_version == ExpectedVersion.NO_STREAM:
        return PortExpectedVersion.no_stream()
    if expected_version == ExpectedVersion.STREAM_EXISTS:
        return PortExpectedVersion.stream_exists()
    if expected_version >= 0:
        return PortExpectedVersion.exact(expected_version)
    raise ValueError(f"unrecognized expected_version sentinel: {expected_version}")


class LegacyStoreAdapter(EventStore):
    """Adapts a new-ring `FullEventStore`-shaped adapter to the old `EventStore` ABC.

    Args:
        adapter: Any object structurally satisfying `ports.FullEventStore`
            (e.g. `MemoryEventStore`, `SQLiteEventStore`, `PostgreSQLEventStore`).
        store_id: The `store_id` the wrapped adapter stamps onto its
            `Position` values. Defaults to the wrapped adapter's public
            `store_id` attribute when present (all three known adapters
            expose one); pass this explicitly to override it, or when
            wrapping an adapter that doesn't expose `store_id`. Raises
            `TypeError` if neither is available. It must match the wrapped
            adapter's own `store_id` or positions it emits will fail to
            round-trip through the codec (`PositionForeignError`).

    Known incompatibilities with the old `EventStore` ABC:
        - `get_events(aggregate_id, aggregate_type=None, ...)`: the old ABC
          allows omitting `aggregate_type` to fetch all events for an
          `aggregate_id` across every type. The new `StreamId` port requires
          a category, so this wrapper has no way to express "any type" and
          raises `ValueError` when `aggregate_type` is `None`. Old call
          sites that rely on the cross-type lookup (e.g.
          `migration.dual_write.DualWriteInterceptor`, which forwards
          `aggregate_type=None` unmodified) cannot be routed through this
          wrapper without also passing a concrete `aggregate_type`.
        - `read_all(ReadOptions(direction=BACKWARD))` is implemented by
          materializing the full forward feed in memory and reversing it
          (O(n) in the number of matching events), unlike a backend that
          might support a native reverse scan. Fine for compatibility-shim
          use; not recommended for very large stores.
        - `get_events`/`read_all` timestamp filters (`from_timestamp`,
          `to_timestamp`) are applied client-side after collecting from the
          port iterator, since `StreamReadOptions`/`FeedReadOptions` have no
          timestamp fields -- no pushdown to the adapter.
    """

    def __init__(self, adapter: FullEventStore, store_id: str | None = None) -> None:
        self._adapter = adapter
        resolved_store_id = store_id if store_id is not None else getattr(adapter, "store_id", None)
        if resolved_store_id is None:
            raise TypeError(
                "LegacyStoreAdapter requires a store_id: the wrapped adapter has no "
                "public 'store_id' attribute, so one must be passed explicitly"
            )
        self._codec = IntPositionCodec(resolved_store_id)

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        stream = StreamId(aggregate_id=aggregate_id, category=aggregate_type)
        expected = _expected_from_int(expected_version)
        result = await self._adapter.append(stream, events, expected)
        global_position = self._codec.value_of(result.position) if result.position else 0
        return AppendResult(
            success=True,
            new_version=result.new_version,
            global_position=global_position,
            conflict=False,
        )

    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        if aggregate_type is None:
            raise ValueError(
                "LegacyStoreAdapter.get_events requires aggregate_type: the new "
                "StreamId port requires a category, unlike the old optional param"
            )
        stream = StreamId(aggregate_id=aggregate_id, category=aggregate_type)

        # Old from_version is a "skip count" over the 0-indexed event list;
        # the new StreamReadOptions.from_version is an inclusive 1-based
        # stream version. from_version=0 (old) means "no skip" -> None (new).
        new_from_version = from_version + 1 if from_version > 0 else None
        options = StreamReadOptions(from_version=new_from_version)

        events: list[DomainEvent] = []
        async for envelope in self._adapter.read_stream(stream, options):
            if from_timestamp is not None and envelope.event.occurred_at < from_timestamp:
                continue
            if to_timestamp is not None and envelope.event.occurred_at > to_timestamp:
                continue
            events.append(envelope.event)

        # Intentional correctness fix over the old InMemoryEventStore quirk,
        # where `version` was the length of the (from_version-)filtered
        # events list rather than the true stream version; here it is
        # always the true stream version regardless of filtering.
        version = await self._adapter.get_stream_version(stream)
        return EventStream(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=events,
            version=version,
        )

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        options = CategoryReadOptions(tenant_id=tenant_id, from_timestamp=from_timestamp)
        events: list[DomainEvent] = []
        async for envelope in self._adapter.read_category(aggregate_type, options):
            events.append(envelope.event)
        return events

    async def event_exists(self, event_id: UUID) -> bool:
        return await self._adapter.event_exists(event_id)

    async def get_stream_version(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> int:
        stream = StreamId(aggregate_id=aggregate_id, category=aggregate_type)
        return await self._adapter.get_stream_version(stream)

    async def read_all(
        self,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        opts = options or ReadOptions()
        backward = opts.direction == ReadDirection.BACKWARD

        from_position: Position | None = None
        if opts.from_position not in (0, -1):
            from_position = self._codec.encode(opts.from_position)

        # Mirrors stores/in_memory.py's `_do_read_all`: filters (position,
        # timestamp, tenant) are applied in forward order first, direction
        # is applied next, and limit is applied LAST -- so for BACKWARD
        # reads, limit keeps the *last* N events of the forward-filtered
        # feed (i.e. the most recent N), not the first N. Limit can only be
        # pushed down to the adapter when it is safe to apply first: forward
        # direction with no client-side timestamp filtering left to shrink
        # the result further. In every other case (BACKWARD, or timestamp
        # filters present) the full matching forward feed is materialized in
        # memory (O(n) in the number of matching events), filtered, then
        # reversed and/or limited client-side to match the old ordering.
        push_down_limit = not backward and opts.from_timestamp is None and opts.to_timestamp is None
        feed_options = FeedReadOptions(
            tenant_id=opts.tenant_id, limit=opts.limit if push_down_limit else None
        )

        envelopes = []
        async for envelope in self._adapter.read_all(from_position, feed_options):
            if opts.from_timestamp is not None and envelope.event.occurred_at < opts.from_timestamp:
                continue
            if opts.to_timestamp is not None and envelope.event.occurred_at > opts.to_timestamp:
                continue
            envelopes.append(envelope)

        if backward:
            envelopes = list(reversed(envelopes))
        if not push_down_limit and opts.limit is not None:
            envelopes = envelopes[: opts.limit]

        for envelope in envelopes:
            global_position = (
                self._codec.value_of(envelope.position) if envelope.position is not None else 0
            )
            yield StoredEvent(
                event=envelope.event,
                stream_id=envelope.stream_id.render(),
                stream_position=envelope.stream_version,
                global_position=global_position,
                stored_at=envelope.stored_at,
            )

    async def get_global_position(self) -> int:
        position = await self._adapter.current_position()
        return self._codec.value_of(position) if position is not None else 0


__all__ = ["LegacyStoreAdapter"]
