"""Unit tests for SyncEventStoreAdapter init, append, read_stream, read_events, and stream_exists."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import EventEnvelope, ExpectedVersion, StreamReadOptions


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"
    data: str = "test"


@pytest.fixture
def async_store() -> InMemoryEventStore:
    return InMemoryEventStore()


@pytest.fixture
def sync_store(async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
    return SyncEventStoreAdapter(async_store, timeout=5.0)


class TestSyncEventStoreAdapterInit:
    """Tests for SyncEventStoreAdapter initialization."""

    def test_init_with_valid_store(self) -> None:
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=5.0)
        assert adapter.wrapped_store is store
        assert adapter.timeout == 5.0

    def test_init_with_default_timeout(self) -> None:
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store)
        assert adapter.timeout == 30.0

    def test_init_with_invalid_store_raises_attribute_error_on_use(self) -> None:
        adapter = SyncEventStoreAdapter("not a store")  # type: ignore[arg-type]
        with pytest.raises(AttributeError):
            adapter.get_stream_version(StreamId(aggregate_id=uuid4(), category="Sample"))

    def test_repr(self) -> None:
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=10.0)
        repr_str = repr(adapter)
        assert "SyncEventStoreAdapter" in repr_str
        assert "InMemoryEventStore" in repr_str
        assert "timeout=10.0" in repr_str


class TestSyncEventStoreAdapterAppend:
    """Tests for append method."""

    def test_append_success(self, sync_store: SyncEventStoreAdapter) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        result = sync_store.append(stream, [event], ExpectedVersion.no_stream())
        assert result.new_version == 1
        assert result.stream == stream

    def test_append_single_event_and_default_expected(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        e1 = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        e2 = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2)
        res1 = sync_store.append(stream, e1)
        res2 = sync_store.append(stream, e2)
        assert res1.new_version == 1
        assert res2.new_version == 2

    def test_append_multiple_events(self, sync_store: SyncEventStoreAdapter) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        result = sync_store.append(stream, events, ExpectedVersion.no_stream())
        assert result.new_version == 3

    def test_append_optimistic_lock_error(self, sync_store: SyncEventStoreAdapter) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        sync_store.append(stream, [event], ExpectedVersion.no_stream())
        event2 = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2)
        with pytest.raises(OptimisticLockError):
            sync_store.append(stream, [event2], ExpectedVersion.no_stream())

    def test_append_empty_events_raises(self, sync_store: SyncEventStoreAdapter) -> None:
        stream = StreamId(aggregate_id=uuid4(), category="Sample")
        with pytest.raises(ValueError, match="empty batch"):
            sync_store.append(stream, [], ExpectedVersion.no_stream())

    def test_append_with_timeout_override(self, async_store: InMemoryEventStore) -> None:
        sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        result = sync_store.append(stream, [event], ExpectedVersion.no_stream(), timeout=1.0)
        assert result.new_version == 1


class TestSyncEventStoreAdapterReadStream:
    """Tests for read_stream and read_events methods."""

    def test_read_stream_returns_empty_list_for_nonexistent(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        stream = StreamId(aggregate_id=uuid4(), category="Sample")
        assert sync_store.read_stream(stream) == []
        assert sync_store.read_events(stream) == []

    def test_read_stream_returns_stored_events(self, sync_store: SyncEventStoreAdapter) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        sync_store.append(stream, [event], ExpectedVersion.no_stream())
        envelopes = sync_store.read_stream(stream)
        assert len(envelopes) == 1
        assert isinstance(envelopes[0], EventEnvelope)
        assert envelopes[0].event.aggregate_id == agg_id
        assert envelopes[0].stream_version == 1
        assert sync_store.read_events(stream) == [event]

    def test_read_stream_with_from_version(self, sync_store: SyncEventStoreAdapter) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        sync_store.append(stream, events, ExpectedVersion.no_stream())
        opts = StreamReadOptions(from_version=2)
        assert len(sync_store.read_stream(stream, opts)) == 2
        assert sync_store.read_events(stream, opts) == events[1:]


class TestSyncEventStoreAdapterGetStreamVersion:
    """Tests for get_stream_version and stream_exists methods."""

    def test_get_stream_version_returns_zero_for_new_aggregate(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        stream = StreamId(aggregate_id=uuid4(), category="Sample")
        assert sync_store.get_stream_version(stream) == 0
        assert sync_store.stream_exists(stream) is False

    def test_get_stream_version_returns_correct_version(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        sync_store.append(stream, events, ExpectedVersion.no_stream())
        assert sync_store.get_stream_version(stream) == 3
        assert sync_store.stream_exists(stream) is True
