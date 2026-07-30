"""Direct tests for PartitionedMemoryStore, plus conformance proving it is
feed-less: it passes every store-port suite except GlobalFeedConformance.
"""

from collections.abc import AsyncIterator

import pytest

from eventsource.ports import ExpectedVersion, collect
from eventsource.testing.conformance_ports import (
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    StreamReaderConformance,
)
from eventsource.testing.conformance_ports._fixtures import make_event, make_stream
from eventsource.testing.partitioned_memory import PartitionedMemoryStore


async def test_read_stream_envelopes_carry_none_position() -> None:
    store = PartitionedMemoryStore()
    stream = make_stream()
    await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream())

    envelopes = await collect(store.read_stream(stream))

    assert envelopes[0].position is None


def test_store_has_no_feed_surface() -> None:
    store = PartitionedMemoryStore()
    assert not hasattr(store, "read_all")
    assert not hasattr(store, "current_position")


async def test_basic_append_and_read_round_trip() -> None:
    store = PartitionedMemoryStore()
    stream = make_stream()
    events = [make_event(stream.aggregate_id, payload=str(i)) for i in range(2)]

    result = await store.append(stream, events, ExpectedVersion.no_stream())

    assert result.new_version == 2

    envelopes = await collect(store.read_stream(stream))
    assert [e.event.event_id for e in envelopes] == [ev.event_id for ev in events]
    assert await store.get_stream_version(stream) == 2


class TestPartitionedAppender(AppenderConformance):
    positions_expected = False

    @pytest.fixture
    async def store(self) -> AsyncIterator[PartitionedMemoryStore]:
        yield PartitionedMemoryStore()


class TestPartitionedStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[PartitionedMemoryStore]:
        yield PartitionedMemoryStore()


class TestPartitionedEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[PartitionedMemoryStore]:
        yield PartitionedMemoryStore()


class TestPartitionedCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[PartitionedMemoryStore]:
        yield PartitionedMemoryStore()
