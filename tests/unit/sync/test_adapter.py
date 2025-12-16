"""Unit tests for SyncEventStoreAdapter."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.stores.interface import EventStream, ReadOptions
from eventsource.sync import SyncEventStoreAdapter


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleEvent"
    data: str = "test"


class TestSyncEventStoreAdapterInit:
    """Tests for SyncEventStoreAdapter initialization."""

    def test_init_with_valid_store(self) -> None:
        """Adapter initializes with valid EventStore."""
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=5.0)

        assert adapter.wrapped_store is store
        assert adapter.timeout == 5.0

    def test_init_with_default_timeout(self) -> None:
        """Adapter uses default timeout of 30.0."""
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store)

        assert adapter.timeout == 30.0

    def test_init_with_invalid_store_raises_type_error(self) -> None:
        """Adapter raises TypeError for non-EventStore."""
        with pytest.raises(TypeError, match="must be an EventStore instance"):
            SyncEventStoreAdapter("not a store")  # type: ignore[arg-type]

    def test_repr(self) -> None:
        """Adapter has useful string representation."""
        store = InMemoryEventStore()
        adapter = SyncEventStoreAdapter(store, timeout=10.0)

        repr_str = repr(adapter)
        assert "SyncEventStoreAdapter" in repr_str
        assert "InMemoryEventStore" in repr_str
        assert "timeout=10.0" in repr_str


class TestSyncEventStoreAdapterAppendEvents:
    """Tests for append_events_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_append_events_sync_success(self, sync_store: SyncEventStoreAdapter) -> None:
        """append_events_sync successfully appends events."""
        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        result = sync_store.append_events_sync(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1
        assert result.global_position == 1

    def test_append_events_sync_multiple_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """append_events_sync appends multiple events."""
        agg_id = uuid4()
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]

        result = sync_store.append_events_sync(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 3

    def test_append_events_sync_optimistic_lock_error(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """append_events_sync raises OptimisticLockError on version mismatch."""
        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # First append succeeds
        sync_store.append_events_sync(agg_id, "Sample", [event], 0)

        # Second append with wrong expected version fails
        event2 = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=2,
        )
        with pytest.raises(OptimisticLockError):
            sync_store.append_events_sync(agg_id, "Sample", [event2], 0)

    def test_append_events_sync_empty_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """append_events_sync handles empty events list."""
        agg_id = uuid4()

        result = sync_store.append_events_sync(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            events=[],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 0

    def test_append_events_sync_with_timeout_override(
        self, async_store: InMemoryEventStore
    ) -> None:
        """append_events_sync respects timeout override."""
        sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # Should work with custom timeout
        result = sync_store.append_events_sync(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            events=[event],
            expected_version=0,
            timeout=1.0,
        )

        assert result.success is True


class TestSyncEventStoreAdapterGetEvents:
    """Tests for get_events_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_get_events_sync_returns_empty_stream(self, sync_store: SyncEventStoreAdapter) -> None:
        """get_events_sync returns empty stream for nonexistent aggregate."""
        agg_id = uuid4()

        stream = sync_store.get_events_sync(agg_id, "Sample")

        assert stream.is_empty is True
        assert stream.version == 0
        assert len(stream.events) == 0

    def test_get_events_sync_returns_stored_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """get_events_sync returns stored events."""
        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        sync_store.append_events_sync(agg_id, "Sample", [event], 0)
        stream = sync_store.get_events_sync(agg_id, "Sample")

        assert len(stream.events) == 1
        assert stream.events[0].aggregate_id == agg_id
        assert stream.version == 1

    def test_get_events_sync_with_from_version(self, sync_store: SyncEventStoreAdapter) -> None:
        """get_events_sync respects from_version parameter."""
        agg_id = uuid4()
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]

        sync_store.append_events_sync(agg_id, "Sample", events, 0)
        stream = sync_store.get_events_sync(agg_id, "Sample", from_version=1)

        # from_version=1 skips first event
        assert len(stream.events) == 2

    def test_get_events_sync_without_aggregate_type(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_events_sync works without aggregate type filter."""
        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        sync_store.append_events_sync(agg_id, "Sample", [event], 0)
        stream = sync_store.get_events_sync(agg_id)

        assert len(stream.events) == 1


class TestSyncEventStoreAdapterGetEventsByType:
    """Tests for get_events_by_type_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_get_events_by_type_sync_returns_empty_list(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_events_by_type_sync returns empty list for no events."""
        events = sync_store.get_events_by_type_sync("NonExistent")
        assert events == []

    def test_get_events_by_type_sync_returns_events(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_events_by_type_sync returns events of specified type."""
        agg_id1 = uuid4()
        agg_id2 = uuid4()

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

        sync_store.append_events_sync(agg_id1, "Sample", [event1], 0)
        sync_store.append_events_sync(agg_id2, "Sample", [event2], 0)

        events = sync_store.get_events_by_type_sync("Sample")
        assert len(events) == 2

    def test_get_events_by_type_sync_with_tenant_filter(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_events_by_type_sync filters by tenant_id."""
        tenant_id = uuid4()
        agg_id1 = uuid4()
        agg_id2 = uuid4()

        event1 = SampleEvent(
            aggregate_id=agg_id1,
            aggregate_type="Sample",
            aggregate_version=1,
            tenant_id=tenant_id,
        )
        event2 = SampleEvent(
            aggregate_id=agg_id2,
            aggregate_type="Sample",
            aggregate_version=1,
            tenant_id=uuid4(),  # Different tenant
        )

        sync_store.append_events_sync(agg_id1, "Sample", [event1], 0)
        sync_store.append_events_sync(agg_id2, "Sample", [event2], 0)

        events = sync_store.get_events_by_type_sync("Sample", tenant_id=tenant_id)
        assert len(events) == 1
        assert events[0].tenant_id == tenant_id


class TestSyncEventStoreAdapterGetStreamVersion:
    """Tests for get_stream_version_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_get_stream_version_sync_returns_zero_for_new_aggregate(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_stream_version_sync returns 0 for nonexistent aggregate."""
        agg_id = uuid4()

        version = sync_store.get_stream_version_sync(agg_id, "Sample")

        assert version == 0

    def test_get_stream_version_sync_returns_correct_version(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_stream_version_sync returns correct version after appends."""
        agg_id = uuid4()
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]

        sync_store.append_events_sync(agg_id, "Sample", events, 0)
        version = sync_store.get_stream_version_sync(agg_id, "Sample")

        assert version == 3


class TestSyncEventStoreAdapterEventExists:
    """Tests for event_exists_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_event_exists_sync_returns_false_for_nonexistent(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """event_exists_sync returns False for nonexistent event."""
        event_id = uuid4()

        exists = sync_store.event_exists_sync(event_id)

        assert exists is False

    def test_event_exists_sync_returns_true_for_existing(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """event_exists_sync returns True for existing event."""
        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        sync_store.append_events_sync(agg_id, "Sample", [event], 0)
        exists = sync_store.event_exists_sync(event.event_id)

        assert exists is True


class TestSyncEventStoreAdapterReadAll:
    """Tests for read_all_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_read_all_sync_returns_empty_list(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_all_sync returns empty list for empty store."""
        events = sync_store.read_all_sync()
        assert events == []

    def test_read_all_sync_returns_all_events(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_all_sync returns all events in store."""
        agg_id1 = uuid4()
        agg_id2 = uuid4()

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

        sync_store.append_events_sync(agg_id1, "Sample", [event1], 0)
        sync_store.append_events_sync(agg_id2, "Sample", [event2], 0)

        stored_events = sync_store.read_all_sync()

        assert len(stored_events) == 2

    def test_read_all_sync_with_options(self, sync_store: SyncEventStoreAdapter) -> None:
        """read_all_sync respects ReadOptions."""
        agg_id = uuid4()
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 6)
        ]

        sync_store.append_events_sync(agg_id, "Sample", events, 0)

        options = ReadOptions(limit=2)
        stored_events = sync_store.read_all_sync(options)

        assert len(stored_events) == 2


class TestSyncEventStoreAdapterGetGlobalPosition:
    """Tests for get_global_position_sync method."""

    @pytest.fixture
    def async_store(self) -> InMemoryEventStore:
        """Create an async event store."""
        return InMemoryEventStore()

    @pytest.fixture
    def sync_store(self, async_store: InMemoryEventStore) -> SyncEventStoreAdapter:
        """Create a sync adapter."""
        return SyncEventStoreAdapter(async_store, timeout=5.0)

    def test_get_global_position_sync_returns_zero_for_empty(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_global_position_sync returns 0 for empty store."""
        position = sync_store.get_global_position_sync()
        assert position == 0

    def test_get_global_position_sync_returns_correct_position(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        """get_global_position_sync returns correct position after appends."""
        agg_id = uuid4()
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]

        sync_store.append_events_sync(agg_id, "Sample", events, 0)
        position = sync_store.get_global_position_sync()

        assert position == 3


class TestSyncEventStoreAdapterTimeout:
    """Tests for timeout handling."""

    def test_timeout_raises_error(self) -> None:
        """Operations timeout correctly."""

        # Create a mock store with a slow method
        class SlowStore(InMemoryEventStore):
            async def get_events(self, *args: object, **kwargs: object) -> EventStream:
                await asyncio.sleep(1.0)
                return EventStream(
                    aggregate_id=uuid4(),
                    aggregate_type="Test",
                    events=[],
                    version=0,
                )

        slow_store = SlowStore()
        sync_store = SyncEventStoreAdapter(slow_store, timeout=0.01)

        # Should raise TimeoutError (which could be asyncio.TimeoutError)
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            sync_store.get_events_sync(uuid4(), "Test")

    def test_per_call_timeout_override(self) -> None:
        """Per-call timeout overrides default."""

        class SlowStore(InMemoryEventStore):
            async def event_exists(self, event_id: object) -> bool:
                await asyncio.sleep(0.5)
                return False

        slow_store = SlowStore()
        sync_store = SyncEventStoreAdapter(slow_store, timeout=10.0)

        # Short timeout should fail
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            sync_store.event_exists_sync(uuid4(), timeout=0.01)


class TestSyncEventStoreAdapterExecutorManagement:
    """Tests for executor lifecycle management."""

    def test_shutdown_executor(self) -> None:
        """shutdown_executor cleans up executor."""
        store = InMemoryEventStore()
        _adapter = SyncEventStoreAdapter(store)

        # Access executor to create it
        executor = SyncEventStoreAdapter._get_executor()
        assert executor is not None

        # Shutdown
        SyncEventStoreAdapter.shutdown_executor()
        assert SyncEventStoreAdapter._executor is None

        # Can recreate after shutdown
        executor2 = SyncEventStoreAdapter._get_executor()
        assert executor2 is not None

        # Cleanup
        SyncEventStoreAdapter.shutdown_executor()

    def test_executor_is_shared(self) -> None:
        """Executor is shared across adapter instances."""
        store1 = InMemoryEventStore()
        store2 = InMemoryEventStore()

        _adapter1 = SyncEventStoreAdapter(store1)
        _adapter2 = SyncEventStoreAdapter(store2)

        executor1 = SyncEventStoreAdapter._get_executor()
        executor2 = SyncEventStoreAdapter._get_executor()

        assert executor1 is executor2

        # Cleanup
        SyncEventStoreAdapter.shutdown_executor()
