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


def _new_stream() -> tuple[uuid4, StreamId]:
    agg_id = uuid4()
    return agg_id, StreamId(aggregate_id=agg_id, category="Sample")


class TestThreadSafety:
    def test_concurrent_access_from_multiple_threads(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        errors: list[tuple[int, Exception]] = []
        success_count = [0]
        lock = threading.Lock()

        def worker(tid: int) -> None:
            try:
                for i in range(10):
                    agg_id, stream = _new_stream()
                    evt = SampleEvent(aggregate_id=agg_id, aggregate_version=1, data=f"{tid}_{i}")
                    result = sync_store.append(stream, [evt], ExpectedVersion.no_stream())
                    assert result.new_version == 1
                    envelopes = sync_store.read_stream(stream)
                    assert len(envelopes) == 1 and envelopes[0].event.aggregate_id == agg_id
                    assert sync_store.get_stream_version(stream) == 1
                    with lock:
                        success_count[0] += 1
            except Exception as e:
                with lock:
                    errors.append((tid, e))

        _run_threads([threading.Thread(target=worker, args=(i,)) for i in range(5)])
        assert not errors and success_count[0] == 50

    def test_concurrent_appends_to_different_aggregates(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        streams: list[StreamId] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(tid: int) -> None:
            try:
                agg_id, stream = _new_stream()
                with lock:
                    streams.append(stream)
                for i in range(5):
                    evt = SampleEvent(aggregate_id=agg_id, aggregate_version=i + 1, data=f"e_{i}")
                    sync_store.append(stream, [evt], ExpectedVersion.exact(i))
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=worker, args=(i,)) for i in range(10)])
        assert not errors
        for stream in streams:
            assert sync_store.get_stream_version(stream) == 5

    def test_concurrent_reads_and_writes(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        agg_id, stream = _new_stream()
        sync_store.append(
            stream,
            [SampleEvent(aggregate_id=agg_id, aggregate_version=1)],
            ExpectedVersion.no_stream(),
        )
        errors: list[Exception] = []
        read_results: list[int] = []
        lock = threading.Lock()

        def reader(tid: int) -> None:
            try:
                for _ in range(20):
                    envelopes = sync_store.read_stream(stream)
                    with lock:
                        read_results.append(len(envelopes))
            except Exception as e:
                with lock:
                    errors.append(e)

        def writer(tid: int) -> None:
            try:
                for _ in range(5):
                    new_id, new_stream = _new_stream()
                    evt = SampleEvent(aggregate_id=new_id, aggregate_version=1)
                    sync_store.append(new_stream, [evt], ExpectedVersion.no_stream())
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [
            threading.Thread(target=fn, args=(i,)) for i in range(5) for fn in (reader, writer)
        ]
        _run_threads(threads)
        assert not errors and all(count >= 1 for count in read_results)


class TestEventLoopScenarios:
    def test_no_event_loop_in_thread(self) -> None:
        result: list[str] = []
        errors: list[Exception] = []

        def run_in_thread() -> None:
            try:
                sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
                agg_id, stream = _new_stream()
                evt = SampleEvent(aggregate_id=agg_id, aggregate_version=1)
                append_result = sync_store.append(stream, [evt], ExpectedVersion.no_stream())
                assert append_result.new_version == 1 and len(sync_store.read_stream(stream)) == 1
                result.append("success")
            except Exception as e:
                errors.append(e)

        _run_threads([threading.Thread(target=run_in_thread)])
        assert not errors and result == ["success"]

    def test_multiple_threads_without_event_loops(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=10.0)
        results: list[str] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def run_in_thread(tid: int) -> None:
            try:
                agg_id, stream = _new_stream()
                evt = SampleEvent(aggregate_id=agg_id, aggregate_version=1)
                assert (
                    sync_store.append(stream, [evt], ExpectedVersion.no_stream()).new_version == 1
                )
                with lock:
                    results.append(f"thread_{tid}")
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=run_in_thread, args=(i,)) for i in range(10)])
        assert not errors and len(results) == 10


class TestExceptionPropagation:
    def test_exceptions_propagate_from_threads(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
        agg_id, stream = _new_stream()
        sync_store.append(
            stream,
            [SampleEvent(aggregate_id=agg_id, aggregate_version=1)],
            ExpectedVersion.no_stream(),
        )
        errors: list[Exception] = []
        lock = threading.Lock()

        def attempt_conflicting_append() -> None:
            try:
                evt2 = SampleEvent(aggregate_id=agg_id, aggregate_version=2)
                sync_store.append(stream, [evt2], ExpectedVersion.no_stream())
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=attempt_conflicting_append) for _ in range(5)])
        assert len(errors) == 5 and all(isinstance(e, OptimisticLockError) for e in errors)


class TestStressTest:
    @pytest.mark.slow
    def test_high_concurrency_stress(self) -> None:
        sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=30.0)
        total_operations = [0]
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(tid: int) -> None:
            try:
                for _ in range(50):
                    agg_id, stream = _new_stream()
                    evt = SampleEvent(aggregate_id=agg_id, aggregate_version=1)
                    sync_store.append(stream, [evt], ExpectedVersion.no_stream())
                    sync_store.read_stream(stream)
                    sync_store.event_exists(evt.event_id)
                    with lock:
                        total_operations[0] += 3
            except Exception as e:
                with lock:
                    errors.append(e)

        _run_threads([threading.Thread(target=worker, args=(i,)) for i in range(20)])
        assert not errors and total_operations[0] == 3000
