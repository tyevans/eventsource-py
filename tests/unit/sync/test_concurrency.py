"""Concurrency tests for SyncEventStoreAdapter."""

from __future__ import annotations

import threading
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.sync import SyncEventStoreAdapter


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleEvent"
    data: str = "test"


class TestThreadSafety:
    """Tests for thread safety of SyncEventStoreAdapter."""

    def test_concurrent_access_from_multiple_threads(self) -> None:
        """Multiple threads can use adapter concurrently."""
        store = InMemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=10.0)

        errors: list[tuple[int, Exception]] = []
        success_count = [0]
        lock = threading.Lock()

        def worker(thread_id: int) -> None:
            try:
                for i in range(10):
                    agg_id = uuid4()
                    event = SampleEvent(
                        aggregate_id=agg_id,
                        aggregate_type="Sample",
                        aggregate_version=1,
                        data=f"thread_{thread_id}_event_{i}",
                    )

                    # Append event
                    result = sync_store.append_events_sync(agg_id, "Sample", [event], 0)
                    assert result.success is True

                    # Read it back
                    stream = sync_store.get_events_sync(agg_id, "Sample")
                    assert len(stream.events) == 1
                    assert stream.events[0].aggregate_id == agg_id

                    # Check version
                    version = sync_store.get_stream_version_sync(agg_id, "Sample")
                    assert version == 1

                    with lock:
                        success_count[0] += 1

            except Exception as e:
                with lock:
                    errors.append((thread_id, e))

        # Run 5 threads concurrently
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors occurred: {errors}"
        assert success_count[0] == 50

    def test_concurrent_appends_to_different_aggregates(self) -> None:
        """Concurrent appends to different aggregates work correctly."""
        store = InMemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=10.0)

        aggregate_ids: list[uuid4] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(thread_id: int) -> None:
            try:
                agg_id = uuid4()
                with lock:
                    aggregate_ids.append(agg_id)

                # Append multiple events
                for i in range(5):
                    event = SampleEvent(
                        aggregate_id=agg_id,
                        aggregate_type="Sample",
                        aggregate_version=i + 1,
                        data=f"event_{i}",
                    )
                    sync_store.append_events_sync(agg_id, "Sample", [event], i)

            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors occurred: {errors}"

        # Verify all aggregates have correct versions
        for agg_id in aggregate_ids:
            version = sync_store.get_stream_version_sync(agg_id, "Sample")
            assert version == 5

    def test_concurrent_reads_and_writes(self) -> None:
        """Concurrent reads and writes work correctly."""
        store = InMemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=10.0)

        # Create some initial data
        agg_id = uuid4()
        initial_event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )
        sync_store.append_events_sync(agg_id, "Sample", [initial_event], 0)

        errors: list[Exception] = []
        read_results: list[int] = []
        lock = threading.Lock()

        def reader(thread_id: int) -> None:
            try:
                for _ in range(20):
                    stream = sync_store.get_events_sync(agg_id, "Sample")
                    with lock:
                        read_results.append(len(stream.events))
            except Exception as e:
                with lock:
                    errors.append(e)

        def writer(thread_id: int) -> None:
            try:
                for _ in range(5):
                    new_agg_id = uuid4()
                    event = SampleEvent(
                        aggregate_id=new_agg_id,
                        aggregate_type="Sample",
                        aggregate_version=1,
                    )
                    sync_store.append_events_sync(new_agg_id, "Sample", [event], 0)
            except Exception as e:
                with lock:
                    errors.append(e)

        # Mix readers and writers
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=reader, args=(i,)))
            threads.append(threading.Thread(target=writer, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors occurred: {errors}"
        # All reads should see at least 1 event (the initial one)
        assert all(count >= 1 for count in read_results)


class TestEventLoopScenarios:
    """Tests for different event loop scenarios."""

    def test_no_event_loop_in_thread(self) -> None:
        """Works when no event loop exists (fresh thread)."""
        result: list[str] = []
        errors: list[Exception] = []

        def run_in_thread() -> None:
            try:
                store = InMemoryEventStore()
                sync_store = SyncEventStoreAdapter(store, timeout=5.0)

                agg_id = uuid4()
                event = SampleEvent(
                    aggregate_id=agg_id,
                    aggregate_type="Sample",
                    aggregate_version=1,
                )

                sync_result = sync_store.append_events_sync(agg_id, "Sample", [event], 0)
                assert sync_result.success is True

                stream = sync_store.get_events_sync(agg_id, "Sample")
                assert len(stream.events) == 1

                result.append("success")
            except Exception as e:
                errors.append(e)

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()

        assert not errors, f"Errors occurred: {errors}"
        assert result == ["success"]

    def test_multiple_threads_without_event_loops(self) -> None:
        """Multiple threads without event loops work correctly."""
        store = InMemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=10.0)

        results: list[str] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def run_in_thread(thread_id: int) -> None:
            try:
                agg_id = uuid4()
                event = SampleEvent(
                    aggregate_id=agg_id,
                    aggregate_type="Sample",
                    aggregate_version=1,
                )

                result = sync_store.append_events_sync(agg_id, "Sample", [event], 0)
                assert result.success is True

                with lock:
                    results.append(f"thread_{thread_id}")
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [threading.Thread(target=run_in_thread, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors occurred: {errors}"
        assert len(results) == 10


class TestExceptionPropagation:
    """Tests for exception propagation in concurrent scenarios."""

    def test_exceptions_propagate_from_threads(self) -> None:
        """Exceptions from the store propagate correctly."""
        store = InMemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=5.0)

        agg_id = uuid4()
        event = SampleEvent(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # First append succeeds
        sync_store.append_events_sync(agg_id, "Sample", [event], 0)

        errors: list[Exception] = []
        lock = threading.Lock()

        def attempt_conflicting_append() -> None:
            try:
                # This should fail due to version conflict
                event2 = SampleEvent(
                    aggregate_id=agg_id,
                    aggregate_type="Sample",
                    aggregate_version=2,
                )
                sync_store.append_events_sync(agg_id, "Sample", [event2], 0)
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [threading.Thread(target=attempt_conflicting_append) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All attempts should have raised OptimisticLockError
        assert len(errors) == 5
        from eventsource.exceptions import OptimisticLockError

        assert all(isinstance(e, OptimisticLockError) for e in errors)


class TestStressTest:
    """Stress tests for high-concurrency scenarios."""

    @pytest.mark.slow
    def test_high_concurrency_stress(self) -> None:
        """High concurrency stress test."""
        store = InMemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=30.0)

        total_operations = [0]
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(thread_id: int) -> None:
            try:
                for _ in range(50):
                    agg_id = uuid4()
                    event = SampleEvent(
                        aggregate_id=agg_id,
                        aggregate_type="Sample",
                        aggregate_version=1,
                    )

                    sync_store.append_events_sync(agg_id, "Sample", [event], 0)
                    sync_store.get_events_sync(agg_id, "Sample")
                    sync_store.event_exists_sync(event.event_id)

                    with lock:
                        total_operations[0] += 3
            except Exception as e:
                with lock:
                    errors.append(e)

        # Run 20 threads with 50 iterations each
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors occurred: {errors}"
        # 20 threads * 50 iterations * 3 operations
        assert total_operations[0] == 3000
