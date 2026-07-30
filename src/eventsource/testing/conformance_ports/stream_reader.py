"""Conformance suite for the `StreamReader` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance
implementing both `StreamReader` and `EventAppender` (appending is the
only way to get events into the store to read back).
"""

from abc import ABC, abstractmethod
from typing import Protocol

import pytest

from eventsource.ports import ExpectedVersion, ReadDirection, StreamReadOptions
from eventsource.ports.store import EventAppender, StreamReader, collect
from eventsource.testing.conformance_ports._fixtures import make_event, make_stream


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
