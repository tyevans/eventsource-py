"""Conformance suite for the `GlobalEventFeed` port."""

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

    async def test_full_read_returns_all_events_in_position_order(self, store: _AppenderFeed) -> None:  # fmt: skip
        stream_a, stream_b = make_stream(aggregate_id=uuid4()), make_stream(aggregate_id=uuid4())
        for s in (stream_a, stream_b, stream_a):
            await store.append(s, [ConformanceEvent(aggregate_id=s.aggregate_id)], ExpectedVersion.any_())  # fmt: skip
        envelopes = await collect(store.read_all())
        assert len(envelopes) == 3 and all(e.position is not None for e in envelopes)
        positions: list[Position] = [e.position for e in envelopes if e.position is not None]
        assert positions == sorted(positions) and len(set(positions)) == 3

    async def test_exclusive_resumption_from_position(self, store: _AppenderFeed) -> None:
        stream_a, stream_b = make_stream(aggregate_id=uuid4()), make_stream(aggregate_id=uuid4())
        for i in range(5):
            s = stream_a if i % 2 == 0 else stream_b
            ev = ConformanceEvent(aggregate_id=s.aggregate_id, payload=str(i))
            await store.append(s, [ev], ExpectedVersion.any_())
        full = await collect(store.read_all())
        assert len(full) == 5 and (resume_position := full[1].position) is not None
        resumed = await collect(store.read_all(from_position=resume_position))
        assert [e.event.event_id for e in resumed] == [e.event.event_id for e in full[2:]]

    async def test_from_position_none_reads_from_start(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        await store.append(stream, [ConformanceEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_())  # fmt: skip
        assert len(await collect(store.read_all(from_position=None))) == 1

    async def test_current_position_none_when_empty(self, store: _AppenderFeed) -> None:
        assert await store.current_position() is None

    async def test_current_position_equals_last_envelope_position(self, store: _AppenderFeed) -> None:  # fmt: skip
        stream = make_stream()
        for _ in range(2):
            await store.append(stream, [ConformanceEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_())  # fmt: skip
        envelopes = await collect(store.read_all())
        assert await store.current_position() == envelopes[-1].position

    async def test_tenant_filter_honored(self, store: _AppenderFeed) -> None:
        tenant_a, tenant_b = uuid4(), uuid4()
        stream_a, stream_b = make_stream(aggregate_id=uuid4()), make_stream(aggregate_id=uuid4())
        for s, t, p in ((stream_a, tenant_a, "1"), (stream_b, tenant_b, "2"), (stream_a, tenant_a, "3")):  # fmt: skip
            ev = ConformanceEvent(aggregate_id=s.aggregate_id, tenant_id=t, payload=p)
            await store.append(s, [ev], ExpectedVersion.any_())
        envelopes = await collect(store.read_all(options=FeedReadOptions(tenant_id=tenant_a)))
        assert [e.event.payload for e in envelopes] == ["1", "3"]  # type: ignore[attr-defined]

    async def test_aggregate_type_filter_honored(self, store: _AppenderFeed) -> None:
        wanted = make_stream(category="Conformance", aggregate_id=uuid4())
        other = make_stream(category="ConformanceOther", aggregate_id=uuid4())
        for s, agg_type, p in ((wanted, "Conformance", "1"), (other, "ConformanceOther", "2"), (wanted, "Conformance", "3")):  # fmt: skip
            ev = ConformanceEvent(aggregate_id=s.aggregate_id, aggregate_type=agg_type, payload=p)
            await store.append(s, [ev], ExpectedVersion.any_())
        envelopes = await collect(store.read_all(options=FeedReadOptions(aggregate_type="Conformance")))  # fmt: skip
        assert [e.event.payload for e in envelopes] == ["1", "3"]  # type: ignore[attr-defined]

    async def test_aggregate_type_filter_composes_with_position_and_limit(self, store: _AppenderFeed) -> None:  # fmt: skip
        wanted = make_stream(category="Conformance", aggregate_id=uuid4())
        other = make_stream(category="ConformanceOther", aggregate_id=uuid4())
        for i in range(4):
            w_ev = ConformanceEvent(aggregate_id=wanted.aggregate_id, payload=str(i))
            await store.append(wanted, [w_ev], ExpectedVersion.any_())
            o_ev = ConformanceEvent(aggregate_id=other.aggregate_id, aggregate_type="ConformanceOther", payload=f"other-{i}")  # fmt: skip
            await store.append(other, [o_ev], ExpectedVersion.any_())
        options = FeedReadOptions(aggregate_type="Conformance")
        first = await collect(store.read_all(options=options))
        assert [e.event.payload for e in first] == ["0", "1", "2", "3"]  # type: ignore[attr-defined]
        page = await collect(store.read_all(options=FeedReadOptions(aggregate_type="Conformance", limit=2)))  # fmt: skip
        assert [e.event.payload for e in page] == ["0", "1"] and page[-1].position is not None  # type: ignore[attr-defined]
        rest = await collect(store.read_all(from_position=page[-1].position, options=options))
        assert [e.event.payload for e in rest] == ["2", "3"]  # type: ignore[attr-defined]

    async def test_aggregate_type_filter_no_match_returns_empty(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        await store.append(stream, [ConformanceEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_())  # fmt: skip
        assert await collect(store.read_all(options=FeedReadOptions(aggregate_type="NeverAppended"))) == []  # fmt: skip

    async def test_position_round_trip_resumption(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        for i in range(3):
            await store.append(stream, [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))], ExpectedVersion.any_())  # fmt: skip
        full = await collect(store.read_all())
        assert (position := full[0].position) is not None
        round_tripped = Position.from_str(position.to_str())
        direct = await collect(store.read_all(from_position=position))
        via_round_trip = await collect(store.read_all(from_position=round_tripped))
        assert [e.event.event_id for e in direct] == [e.event.event_id for e in via_round_trip]

    async def test_limit_honored(self, store: _AppenderFeed) -> None:
        stream = make_stream()
        for i in range(5):
            await store.append(stream, [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))], ExpectedVersion.any_())  # fmt: skip
        assert len(await collect(store.read_all(options=FeedReadOptions(limit=2)))) == 2
