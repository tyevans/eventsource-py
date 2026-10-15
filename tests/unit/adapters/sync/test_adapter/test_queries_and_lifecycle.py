"""Unit tests for SyncEventStoreAdapter queries, timeouts, running loop guard, and close."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion, FeedReadOptions, Position


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


class TestSyncEventStoreAdapterEventExists:
    """Tests for event_exists method."""

    def test_event_exists_returns_false_for_nonexistent(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        assert sync_store.event_exists(uuid4()) is False

    def test_event_exists_returns_true_for_existing(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        event = SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        sync_store.append(stream, [event], ExpectedVersion.no_stream())
        assert sync_store.event_exists(event.event_id) is True


class TestSyncEventStoreAdapterReadAll:
    """Tests for read_all method."""

    def test_read_all_returns_empty_list(self, sync_store: SyncEventStoreAdapter) -> None:
        envelopes = sync_store.read_all()
        assert envelopes == []
        assert isinstance(envelopes, list)

    def test_read_all_returns_all_events(self, sync_store: SyncEventStoreAdapter) -> None:
        agg_id1, agg_id2 = uuid4(), uuid4()
        stream1 = StreamId(aggregate_id=agg_id1, category="Sample")
        stream2 = StreamId(aggregate_id=agg_id2, category="Sample")
        event1 = SampleEvent(aggregate_id=agg_id1, aggregate_type="Sample", aggregate_version=1)
        event2 = SampleEvent(aggregate_id=agg_id2, aggregate_type="Sample", aggregate_version=1)
        sync_store.append(stream1, [event1], ExpectedVersion.no_stream())
        sync_store.append(stream2, [event2], ExpectedVersion.no_stream())
        envelopes = sync_store.read_all()
        assert isinstance(envelopes, list)
        assert len(envelopes) == 2

    def test_read_all_with_options(self, sync_store: SyncEventStoreAdapter) -> None:
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

    def test_current_position_returns_none_for_empty_store(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        assert sync_store.current_position() is None

    def test_current_position_returns_position_after_appends(
        self, sync_store: SyncEventStoreAdapter
    ) -> None:
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        events = [
            SampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=i)
            for i in range(1, 4)
        ]
        sync_store.append(stream, events, ExpectedVersion.no_stream())
        assert isinstance(sync_store.current_position(), Position)


class TestSyncEventStoreAdapterTimeout:
    """Tests for timeout handling."""

    def test_timeout_raises_error(self) -> None:
        class SlowStore(InMemoryEventStore):
            async def read_stream(self, *args: object, **kwargs: object):  # type: ignore[override]
                await asyncio.sleep(1.0)
                async for envelope in super().read_stream(*args, **kwargs):  # type: ignore[arg-type]
                    yield envelope

        sync_store = SyncEventStoreAdapter(SlowStore(), timeout=0.01)
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            sync_store.read_stream(StreamId(aggregate_id=uuid4(), category="Sample"))

    def test_per_call_timeout_override(self) -> None:
        class SlowStore(InMemoryEventStore):
            async def event_exists(self, event_id: object) -> bool:  # type: ignore[override]
                await asyncio.sleep(0.5)
                return False

        sync_store = SyncEventStoreAdapter(SlowStore(), timeout=10.0)
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            sync_store.event_exists(uuid4(), timeout=0.01)


class TestSyncEventStoreAdapterRunningLoop:
    """Calling the adapter from a thread that already runs a loop."""

    async def test_raises_runtime_error_instead_of_deadlocking(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
        with pytest.raises(RuntimeError) as exc_info:
            sync_store.get_stream_version(StreamId(aggregate_id=uuid4(), category="Sample"))
        message = str(exc_info.value)
        assert "running event loop" in message
        assert "asyncio.to_thread" in message

    async def test_does_not_leak_the_unawaited_coroutine(
        self, recwarn: pytest.WarningsRecorder
    ) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
        with pytest.raises(RuntimeError):
            sync_store.event_exists(uuid4())
        assert not [w for w in recwarn if issubclass(w.category, RuntimeWarning)]


class _ClosableStore(InMemoryEventStore):
    """An in-memory store that also implements `SupportsClose`."""

    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class TestSyncEventStoreAdapterClose:
    """`close()` gives sync callers a way to release the wrapped store."""

    def test_close_delegates_to_a_closable_store(self) -> None:
        store = _ClosableStore()
        sync_store = SyncEventStoreAdapter(store)
        sync_store.close()
        assert store.close_calls == 1

    def test_close_is_idempotent(self) -> None:
        store = _ClosableStore()
        sync_store = SyncEventStoreAdapter(store)
        sync_store.close()
        sync_store.close()
        assert store.close_calls == 2

    def test_close_is_a_no_op_for_a_store_without_the_capability(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore())
        sync_store.close()

    def test_context_manager_closes_on_exit(self) -> None:
        store = _ClosableStore()
        with SyncEventStoreAdapter(store) as sync_store:
            assert sync_store.wrapped_store is store
        assert store.close_calls == 1
