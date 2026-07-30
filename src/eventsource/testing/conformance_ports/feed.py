"""Conformance suite for the `GlobalEventFeed` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance
implementing both `GlobalEventFeed` and `EventAppender` (appending is the
only way to get events into the store to read back).
"""

from abc import ABC, abstractmethod
from typing import Protocol
from uuid import uuid4

import pytest

from eventsource.ports import ExpectedVersion, FeedReadOptions, Position, collect
from eventsource.ports.store import EventAppender, GlobalEventFeed
from eventsource.testing.conformance_ports._fixtures import ConformanceEvent, make_stream


class _AppenderFeed(EventAppender, GlobalEventFeed, Protocol):
    """Adapter surface needed by this suite: append plus global feed reads."""


class GlobalFeedConformance(ABC):
    """Conformance suite for `GlobalEventFeed` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `GlobalEventFeed` + `EventAppender`."""
        raise NotImplementedError

    async def test_full_read_returns_all_events_in_position_order(
        self, store: _AppenderFeed
    ) -> None:
        stream_a = make_stream(aggregate_id=uuid4())
        stream_b = make_stream(aggregate_id=uuid4())
        await store.append(
            stream_a,
            [ConformanceEvent(aggregate_id=stream_a.aggregate_id)],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream_b,
            [ConformanceEvent(aggregate_id=stream_b.aggregate_id)],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream_a,
            [ConformanceEvent(aggregate_id=stream_a.aggregate_id)],
            ExpectedVersion.any_(),
        )

        envelopes = await collect(store.read_all())

        assert len(envelopes) == 3
        assert all(e.position is not None for e in envelopes)
        positions: list[Position] = [e.position for e in envelopes if e.position is not None]
        assert positions == sorted(positions)
        assert len(set(positions)) == 3

    async def test_exclusive_resumption_from_position(self, store: _AppenderFeed) -> None:
        stream_a = make_stream(aggregate_id=uuid4())
        stream_b = make_stream(aggregate_id=uuid4())
        for i in range(5):
            stream = stream_a if i % 2 == 0 else stream_b
            await store.append(
                stream,
                [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))],
                ExpectedVersion.any_(),
            )

        full = await collect(store.read_all())
        assert len(full) == 5
        resume_position = full[1].position
        assert resume_position is not None

        resumed = await collect(store.read_all(from_position=resume_position))

        assert [e.event.event_id for e in resumed] == [e.event.event_id for e in full[2:]]

    async def test_from_position_none_reads_from_start(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        await store.append(
            stream, [ConformanceEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_()
        )

        envelopes = await collect(store.read_all(from_position=None))

        assert len(envelopes) == 1

    async def test_current_position_none_when_empty(self, store: _AppenderFeed) -> None:
        assert await store.current_position() is None

    async def test_current_position_equals_last_envelope_position(
        self, store: _AppenderFeed
    ) -> None:
        stream = make_stream()
        await store.append(
            stream, [ConformanceEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_()
        )
        await store.append(
            stream, [ConformanceEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_()
        )

        envelopes = await collect(store.read_all())
        current = await store.current_position()

        assert current == envelopes[-1].position

    async def test_tenant_filter_honored(self, store: _AppenderFeed) -> None:
        tenant_a = uuid4()
        tenant_b = uuid4()
        stream_a = make_stream(aggregate_id=uuid4())
        stream_b = make_stream(aggregate_id=uuid4())

        await store.append(
            stream_a,
            [ConformanceEvent(aggregate_id=stream_a.aggregate_id, tenant_id=tenant_a, payload="1")],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream_b,
            [ConformanceEvent(aggregate_id=stream_b.aggregate_id, tenant_id=tenant_b, payload="2")],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream_a,
            [ConformanceEvent(aggregate_id=stream_a.aggregate_id, tenant_id=tenant_a, payload="3")],
            ExpectedVersion.any_(),
        )

        envelopes = await collect(store.read_all(options=FeedReadOptions(tenant_id=tenant_a)))

        assert [e.event.payload for e in envelopes] == ["1", "3"]  # type: ignore[attr-defined]

    async def test_position_round_trip_resumption(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        for i in range(3):
            await store.append(
                stream,
                [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))],
                ExpectedVersion.any_(),
            )

        full = await collect(store.read_all())
        position = full[0].position
        assert position is not None
        round_tripped = Position.from_str(position.to_str())

        direct = await collect(store.read_all(from_position=position))
        via_round_trip = await collect(store.read_all(from_position=round_tripped))

        assert [e.event.event_id for e in direct] == [e.event.event_id for e in via_round_trip]

    async def test_limit_honored(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        for i in range(5):
            await store.append(
                stream,
                [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))],
                ExpectedVersion.any_(),
            )

        envelopes = await collect(store.read_all(options=FeedReadOptions(limit=2)))

        assert len(envelopes) == 2
