"""Conformance suite for the `StreamReader` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance
implementing both `StreamReader` and `EventAppender` (appending is the
only way to get events into the store to read back).
"""

from abc import ABC, abstractmethod
from typing import Protocol
from uuid import uuid4

import pytest

from eventsource.ports import ExpectedVersion, ReadDirection, StreamReadOptions
from eventsource.ports.store import EventAppender, StreamReader, collect
from eventsource.testing.conformance_ports._fixtures import (
    ConformanceEvent,
    make_event,
    make_stream,
)


class _AppenderReader(EventAppender, StreamReader, Protocol):
    """Adapter surface needed by this suite: append plus stream reads."""


class StreamReaderConformance(ABC):
    """Conformance suite for `StreamReader` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `StreamReader` + `EventAppender`."""
        raise NotImplementedError

    async def test_read_returns_exactly_appended_events_in_order(
        self, store: _AppenderReader
    ) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(3)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream))

        assert [e.event.event_id for e in envelopes] == [ev.event_id for ev in events]

    async def test_from_version_honored(self, store: _AppenderReader) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(5)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream, StreamReadOptions(from_version=3)))

        assert [e.stream_version for e in envelopes] == [3, 4, 5]

    async def test_to_version_honored(self, store: _AppenderReader) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(5)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream, StreamReadOptions(to_version=2)))

        assert [e.stream_version for e in envelopes] == [1, 2]

    async def test_limit_honored(self, store: _AppenderReader) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(5)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream, StreamReadOptions(limit=2)))

        assert [e.stream_version for e in envelopes] == [1, 2]

    async def test_backward_direction_honored(self, store: _AppenderReader) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(3)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = await collect(
            store.read_stream(stream, StreamReadOptions(direction=ReadDirection.BACKWARD))
        )

        assert [e.stream_version for e in envelopes] == [3, 2, 1]

    async def test_envelopes_carry_correct_stream_version_sequence(
        self, store: _AppenderReader
    ) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(4)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream))

        assert [e.stream_version for e in envelopes] == [1, 2, 3, 4]

    async def test_get_stream_version_matches_appended_count(self, store: _AppenderReader) -> None:
        stream = make_stream()
        events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(3)]
        await store.append(stream, events, ExpectedVersion.no_stream())

        assert await store.get_stream_version(stream) == 3

    async def test_get_stream_version_of_absent_stream_is_zero(
        self, store: _AppenderReader
    ) -> None:
        stream = make_stream()
        assert await store.get_stream_version(stream) == 0

    async def test_read_of_absent_stream_returns_no_envelopes(self, store: _AppenderReader) -> None:
        stream = make_stream()
        envelopes = await collect(store.read_stream(stream))
        assert envelopes == []

    async def test_stream_isolation(self, store: _AppenderReader) -> None:
        stream_a = make_stream(aggregate_id=uuid4())
        stream_b = make_stream(aggregate_id=uuid4())
        event_a = make_event(stream_a.aggregate_id)
        event_b = make_event(stream_b.aggregate_id)
        await store.append(stream_a, [event_a], ExpectedVersion.no_stream())
        await store.append(stream_b, [event_b], ExpectedVersion.no_stream())

        envelopes_a = await collect(store.read_stream(stream_a))
        envelopes_b = await collect(store.read_stream(stream_b))

        assert [e.event.event_id for e in envelopes_a] == [event_a.event_id]
        assert [e.event.event_id for e in envelopes_b] == [event_b.event_id]

    async def test_envelope_metadata_preserved_through_round_trip(
        self, store: _AppenderReader
    ) -> None:
        stream = make_stream()
        tenant_id = uuid4()
        event = ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_id)
        await store.append(stream, [event], ExpectedVersion.no_stream())

        envelopes = await collect(store.read_stream(stream))
        envelope = envelopes[0]
        retrieved = envelope.event

        assert retrieved.event_id == event.event_id
        assert retrieved.event_type == event.event_type
        assert retrieved.aggregate_id == event.aggregate_id
        assert retrieved.aggregate_version == event.aggregate_version
        assert retrieved.occurred_at == event.occurred_at
        assert retrieved.tenant_id == tenant_id
        assert envelope.stream_version == 1
        assert envelope.stream_id == stream
