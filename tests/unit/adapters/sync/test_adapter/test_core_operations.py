"""Unit tests for SyncEventStoreAdapter init, append, read_stream, and get_stream_version."""

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
    """Create an async event store."""
    return InMemoryEventStore()


@pytest.fixture
def sync_store(async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
    """Create a sync adapter."""
    return SyncEventStoreAdapter(async_store, timeout=5.0)


class TestSyncEventStoreAdapterInit:
    """Tests for SyncEventStoreAdapter initialization."""

    def test_init_with_valid_store(self) -> None:
        """Adapter initializes with valid store."""
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=5.0)
        assert adapter.wrapped_store is store
        assert adapter.timeout == 5.0

    def test_init_with_default_timeout(self) -> None:
        """Adapter uses default timeout of 30.0."""
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store)
        assert adapter.timeout == 30.0

    def test_init_with_invalid_store_raises_attribute_error_on_use(self) -> None:
        """A non-store object is accepted at construction but fails on first use."""
        adapter = SyncEventStoreAdapter("not a store")  # type: ignore[arg-type]
        with pytest.raises(AttributeError):
            adapter.get_stream_version(StreamId(aggregate_id=uuid4(), category="Sample"))

    def test_repr(self) -> None:
        """Adapter has useful string representation."""
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=10.0)
        repr_str = repr(adapter)
        assert "SyncEventStoreAdapter" in repr_str
        assert "InMemoryEventStore" in repr_str
        assert "timeout=10.0" in repr_str


class TestSyncEventStoreAdapterAppend:
    """Tests for append method."""

    def test_append_success(self, sync_store: SyncEventStoreAdapter) -> None:
        """append successfully appends events."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        result = sync_store.append(stream, [event], ExpectedVersion.no_stream())
        assert result.new_version == 1
        assert result.stream == stream

    def test_append_multiple_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """append appends multiple events."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        result = sync_store.append(stream, events, ExpectedVersion.no_stream())
        assert result.new_version == 3

    def test_append_optimistic_lock_error(self, sync_store: SyncEventStoreAdapter) -> None:
        """append raises OptimisticLockError on version mismatch."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        sync_store.append(stream, [event], ExpectedVersion.no_stream())

        event2 = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2)
        with pytest.raises(OptimisticLockError):
            sync_store.append(stream, [event2], ExpectedVersion.no_stream())

    def test_append_empty_events_raises(self, sync_store: SyncEventStoreAdapter) -> None:
        """append propagates the store's rejection of an empty batch."""
        stream = StreamId(aggregate_id=uuid4(), category="Sample")
        with pytest.raises(ValueError, match="empty batch"):
            sync_store.append(stream, [], ExpectedVersion.no_stream())

    def test_append_with_timeout_override(self, async_store: InMemoryEventStore) -> None:
        """append respects timeout override."""
        sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        result = sync_store.append(stream, [event], ExpectedVersion.no_stream(), timeout=1.0)
        assert result.new_version == 1


class TestSyncEventStoreAdapterReadStream:
    """Tests for read_stream method."""

    def test_read_stream_returns_empty_list_for_nonexistent(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """read_stream returns an empty list for a nonexistent aggregate."""
        stream = StreamId(aggregate_id=uuid4(), category="Sample")
        envelopes = sync_store.read_stream(stream)
        assert envelopes == []
        assert isinstance(envelopes, list)

    def test_read_stream_returns_stored_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_stream returns stored events as EventEnvelope objects."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        sync_store.append(stream, [event], ExpectedVersion.no_stream())
        envelopes = sync_store.read_stream(stream)

        assert isinstance(envelopes, list)
        assert len(envelopes) == 1
        assert isinstance(envelopes[0], EventEnvelope)
        assert envelopes[0].event.aggregate_id == agg_id
        assert envelopes[0].stream_version == 1

    def test_read_stream_with_from_version(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_stream respects StreamReadOptions.from_version."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        sync_store.append(stream, events, ExpectedVersion.no_stream())
        envelopes = sync_store.read_stream(stream, StreamReadOptions(from_version=2))
        assert len(envelopes) == 2


class TestSyncEventStoreAdapterGetStreamVersion:
    """Tests for get_stream_version method."""

    def test_get_stream_version_returns_zero_for_new_aggregate(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_stream_version returns 0 for nonexistent aggregate."""
        stream = StreamId(aggregate_id=uuid4(), category="Sample")
        assert sync_store.get_stream_version(stream) == 0

    def test_get_stream_version_returns_correct_version(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_stream_version returns correct version after appends."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        sync_store.append(stream, events, ExpectedVersion.no_stream())
        assert sync_store.get_stream_version(stream) == 3
