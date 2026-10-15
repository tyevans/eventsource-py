"""Concurrency tests for SyncEventStoreAdapter."""

from __future__ import annotations

import threading
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import ExpectedVersion


class SampleEvent(DomainEvent):
    aggregate_type: str = "Sample"
    data: str = "test"


def _run_threads(threads: list[threading.Thread]) -> None:
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class TestThreadSafety:
    def test_concurrent_access_from_multiple_threads(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        errors: list[tuple[int, Exception]] = []
        success_count = [0]
        lock = threading.Lock()

        def worker(thread_id: int) -> None:
            try:
                for i in range(10):
                    agg_id = uuid4()
                    stream = StreamId(aggregate_id=agg_id, category="Sample")
                    event = SampleEvent(
                        aggregate_id=agg_id,
                        aggregate_version=1,
                        data=f"thread_{thread_id}_event_{i}",
                    )
                    result = sync_store.append(stream, [event], ExpectedVersion.no_stream())
                    assert result.new_version == 1
                    envelopes = sync_store.read_stream(stream)
                    assert len(envelopes) == 1
                    assert envelopes[0].event.aggregate_id == agg_id
                    assert sync_store.get_stream_version(stream) == 1
                    with lock:
                        success_count[0] += 1
            except Exception as e:
                with lock:
                    errors.append((thread_id, e))

        _run_threads([threading.Thread(target=worker, args=(i,)) for i in range(5)])
        assert not errors, f"Errors occurred: {errors}"
        assert success_count[0] == 50

    def test_concurrent_appends_to_different_aggregates(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        streams: list[StreamId] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(thread_id: int) -> None:
            try:
                agg_id = uuid4()
                stream = StreamId(aggregate_id=agg_id, category="Sample")
                with lock:
                    streams.append(stream)
                for i in range(5):
                    event = SampleEvent(
                        aggregate_id=agg_id, aggregate_version=i + 1, data=f"event_{i}"
                    )
                    sync_store.append(stream, [event], ExpectedVersion.exact(i))
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=worker, args=(i,)) for i in range(10)])
        assert not errors, f"Errors occurred: {errors}"
        for stream in streams:
            assert sync_store.get_stream_version(stream) == 5

    def test_concurrent_reads_and_writes(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        sync_store.append(
            stream,
            [SampleEvent(aggregate_id=agg_id, aggregate_version=1)],
            ExpectedVersion.no_stream(),
        )
        errors: list[Exception] = []
        read_results: list[int] = []
        lock = threading.Lock()

        def reader(thread_id: int) -> None:
            try:
                for _ in range(20):
                    envelopes = sync_store.read_stream(stream)
                    with lock:
                        read_results.append(len(envelopes))
            except Exception as e:
                with lock:
                    errors.append(e)

        def writer(thread_id: int) -> None:
            try:
                for _ in range(5):
                    new_agg_id = uuid4()
                    new_stream = StreamId(aggregate_id=new_agg_id, category="Sample")
                    event = SampleEvent(aggregate_id=new_agg_id, aggregate_version=1)
                    sync_store.append(new_stream, [event], ExpectedVersion.no_stream())
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=reader, args=(i,)))
            threads.append(threading.Thread(target=writer, args=(i,)))
        _run_threads(threads)
        assert not errors, f"Errors occurred: {errors}"
        assert all(count >= 1 for count in read_results)


class TestEventLoopScenarios:
    def test_no_event_loop_in_thread(self) -> None:
        result: list[str] = []
        errors: list[Exception] = []

        def run_in_thread() -> None:
            try:
                sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
                agg_id = uuid4()
                stream = StreamId(aggregate_id=agg_id, category="Sample")
                event = SampleEvent(aggregate_id=agg_id, aggregate_version=1)
                append_result = sync_store.append(stream, [event], ExpectedVersion.no_stream())
                assert append_result.new_version == 1
                envelopes = sync_store.read_stream(stream)
                assert len(envelopes) == 1
                result.append("success")
            except Exception as e:
                errors.append(e)

        _run_threads([threading.Thread(target=run_in_thread)])
        assert not errors, f"Errors occurred: {errors}"
        assert result == ["success"]

    def test_multiple_threads_without_event_loops(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        results: list[str] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def run_in_thread(thread_id: int) -> None:
            try:
                agg_id = uuid4()
                stream = StreamId(aggregate_id=agg_id, category="Sample")
                event = SampleEvent(aggregate_id=agg_id, aggregate_version=1)
                result = sync_store.append(stream, [event], ExpectedVersion.no_stream())
                assert result.new_version == 1
                with lock:
                    results.append(f"thread_{thread_id}")
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=run_in_thread, args=(i,)) for i in range(10)])
        assert not errors, f"Errors occurred: {errors}"
        assert len(results) == 10


class TestExceptionPropagation:
    def test_exceptions_propagate_from_threads(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Sample")
        sync_store.append(
            stream,
            [SampleEvent(aggregate_id=agg_id, aggregate_version=1)],
            ExpectedVersion.no_stream(),
        )
        errors: list[Exception] = []
        lock = threading.Lock()

        def attempt_conflicting_append() -> None:
            try:
                event2 = SampleEvent(aggregate_id=agg_id, aggregate_version=2)
                sync_store.append(stream, [event2], ExpectedVersion.no_stream())
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=attempt_conflicting_append) for _ in range(5)])
        assert len(errors) == 5
        assert all(isinstance(e, OptimisticLockError) for e in errors)


class TestStressTest:
    @pytest.mark.slow
    def test_high_concurrency_stress(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=30.0)
        total_operations = [0]
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(thread_id: int) -> None:
            try:
                for _ in range(50):
                    agg_id = uuid4()
                    stream = StreamId(aggregate_id=agg_id, category="Sample")
                    event = SampleEvent(aggregate_id=agg_id, aggregate_version=1)
                    sync_store.append(stream, [event], ExpectedVersion.no_stream())
                    sync_store.read_stream(stream)
                    sync_store.event_exists(event.event_id)
                    with lock:
                        total_operations[0] += 3
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=worker, args=(i,)) for i in range(20)])
        assert not errors, f"Errors occurred: {errors}"
        assert total_operations[0] == 3000
