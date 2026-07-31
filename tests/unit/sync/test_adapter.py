"""Unit tests for SyncEventStoreAdapter."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.ports import (
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    StreamReadOptions,
)
from eventsource.sync import SyncEventStoreAdapter


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleEvent"
    data: str = "test"


class TestSyncEventStoreAdapterInit:
    """Tests for SyncEventStoreAdapter initialization."""

    def test_init_with_valid_store(self) -> None:
        """Adapter initializes with valid store."""
        store = MemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=5.0)

        assert adapter.wrapped_store is store
        assert adapter.timeout == 5.0

    def test_init_with_default_timeout(self) -> None:
        """Adapter uses default timeout of 30.0."""
        store = MemoryEventStore()
        adapter = SyncEventStoreAdapter(store)

        assert adapter.timeout == 30.0

    def test_init_with_invalid_store_raises_attribute_error_on_use(self) -> None:
        """A non-store object is accepted at construction (structural typing) but
        fails on first use with AttributeError."""
        adapter = SyncEventStoreAdapter("not a store")  # type: ignore[arg-type]

        with pytest.raises(AttributeError):
            adapter.get_stream_version(StreamId(aggregate_id=uuid4(), category="Sample"))

    def test_repr(self) -> None:
        """Adapter has useful string representation."""
        store = MemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=10.0)

        repr_str = repr(adapter)
        assert "SyncEventStoreAdapter" in repr_str
        assert "MemoryEventStore" in repr_str
        assert "timeout=10.0" in repr_str


class TestSyncEventStoreAdapterAppend:
    """Tests for append method."""

    @pytest.fixture
    def async_store(self) -> MemoryEventStore:
        """Create an async event store."""
        return MemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: MemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_append_success(self, sync_store: SyncEventStoreAdapter) -> None:
        """append successfully appends events."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

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
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        sync_store.append(stream, [event], ExpectedVersion.no_stream())

        event2 = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=2,
        )
        with pytest.raises(OptimisticLockError):
            sync_store.append(stream, [event2], ExpectedVersion.no_stream())

    def test_append_empty_events_raises(self, sync_store: SyncEventStoreAdapter) -> None:
        """append propagates the store's rejection of an empty batch."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")

        with pytest.raises(ValueError, match="empty batch"):
            sync_store.append(stream, [], ExpectedVersion.no_stream())

    def test_append_with_timeout_override(self, async_store: MemoryEventStore) -> None:
        """append respects timeout override."""
        sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        result = sync_store.append(
            stream,
            [event],
            ExpectedVersion.no_stream(),
            timeout=1.0,
        )

        assert result.new_version == 1


class TestSyncEventStoreAdapterReadStream:
    """Tests for read_stream method."""

    @pytest.fixture
    def async_store(self) -> MemoryEventStore:
        """Create an async event store."""
        return MemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: MemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

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
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

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

    @pytest.fixture
    def async_store(self) -> MemoryEventStore:
        """Create an async event store."""
        return MemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: MemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_get_stream_version_returns_zero_for_new_aggregate(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_stream_version returns 0 for nonexistent aggregate."""
        stream = StreamId(aggregate_id=uuid4(), category="Sample")

        version = sync_store.get_stream_version(stream)

        assert version == 0

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
        version = sync_store.get_stream_version(stream)

        assert version == 3


class TestSyncEventStoreAdapterEventExists:
    """Tests for event_exists method."""

    @pytest.fixture
    def async_store(self) -> MemoryEventStore:
        """Create an async event store."""
        return MemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: MemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_event_exists_returns_false_for_nonexistent(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """event_exists returns False for nonexistent event."""
        assert sync_store.event_exists(uuid4()) is False

    def test_event_exists_returns_true_for_existing(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """event_exists returns True for existing event."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        sync_store.append(stream, [event], ExpectedVersion.no_stream())

        assert sync_store.event_exists(event.event_id) is True


class TestSyncEventStoreAdapterReadAll:
    """Tests for read_all method."""

    @pytest.fixture
    def async_store(self) -> MemoryEventStore:
        """Create an async event store."""
        return MemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: MemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_read_all_returns_empty_list(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_all returns an empty list for an empty store."""
        envelopes = sync_store.read_all()

        assert envelopes == []
        assert isinstance(envelopes, list)

    def test_read_all_returns_all_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_all returns all events in the store."""
        agg_id1 = uuid4()
        agg_id2 = uuid4()
        stream1 = StreamId(aggregate_id=agg_id1, category="Sample")
        stream2 = StreamId(aggregate_id=agg_id2, category="Sample")

        event1 = SampleEvent(
            aggregate_id=agg_id1,
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = SampleEvent(
            aggregate_id=agg_id2,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        sync_store.append(stream1, [event1], ExpectedVersion.no_stream())
        sync_store.append(stream2, [event2], ExpectedVersion.no_stream())

        envelopes = sync_store.read_all()

        assert isinstance(envelopes, list)
        assert len(envelopes) == 2

    def test_read_all_with_options(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_all respects FeedReadOptions.limit."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 6)
        ]

        sync_store.append(stream, events, ExpectedVersion.no_stream())

        envelopes = sync_store.read_all(None, FeedReadOptions(limit=2))

        assert len(envelopes) == 2


class TestSyncEventStoreAdapterCurrentPosition:
    """Tests for current_position method."""

    @pytest.fixture
    def async_store(self) -> MemoryEventStore:
        """Create an async event store."""
        return MemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: MemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_current_position_returns_none_for_empty_store(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """current_position returns None for an empty store -- not a comparable
        zero floor."""
        assert sync_store.current_position() is None

    def test_current_position_returns_position_after_appends(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """current_position returns a Position after appends."""
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]

        sync_store.append(stream, events, ExpectedVersion.no_stream())
        position = sync_store.current_position()

        assert isinstance(position, Position)


class TestSyncEventStoreAdapterTimeout:
    """Tests for timeout handling."""

    def test_timeout_raises_error(self) -> None:
        """Operations timeout correctly."""

        class SlowStore(MemoryEventStore):
            async def read_stream(self, *args: object, **kwargs: object):  # type: ignore[override]
                await asyncio.sleep(1.0)
                async for envelope in super().read_stream(*args, **kwargs):  # type: ignore[arg-type]
                    yield envelope

        slow_store = SlowStore()
        sync_store = SyncEventStoreAdapter(slow_store, timeout=0.01)

        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            sync_store.read_stream(StreamId(aggregate_id=uuid4(), category="Sample"))

    def test_per_call_timeout_override(self) -> None:
        """Per-call timeout overrides default."""

        class SlowStore(MemoryEventStore):
            async def event_exists(self, event_id: object) -> bool:  # type: ignore[override]
                await asyncio.sleep(0.5)
                return False

        slow_store = SlowStore()
        sync_store = SyncEventStoreAdapter(slow_store, timeout=10.0)

        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            sync_store.event_exists(uuid4(), timeout=0.01)


class TestSyncEventStoreAdapterExecutorManagement:
    """Tests for executor lifecycle management."""

    def test_shutdown_executor(self) -> None:
        """shutdown_executor cleans up executor."""
        store = MemoryEventStore()
        _adapter = SyncEventStoreAdapter(store)

        executor = SyncEventStoreAdapter._get_executor()
        assert executor is not None

        SyncEventStoreAdapter.shutdown_executor()
        assert SyncEventStoreAdapter._executor is None

        executor2 = SyncEventStoreAdapter._get_executor()
        assert executor2 is not None

        SyncEventStoreAdapter.shutdown_executor()

    def test_executor_is_shared(self) -> None:
        """Executor is shared across adapter instances."""
        store1 = MemoryEventStore()
        store2 = MemoryEventStore()

        _adapter1 = SyncEventStoreAdapter(store1)
        _adapter2 = SyncEventStoreAdapter(store2)

        executor1 = SyncEventStoreAdapter._get_executor()
        executor2 = SyncEventStoreAdapter._get_executor()

        assert executor1 is executor2

        SyncEventStoreAdapter.shutdown_executor()
