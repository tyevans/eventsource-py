"""
Performance benchmarks for event store operations.

Tests critical paths:
- Single event append operations
- Batch event append operations
- Event reading (single aggregate)
- Concurrent append operations

Target baselines (InMemory):
- Append single event: < 0.1ms
- Read 100 events: < 5ms
- Read 1000 events: < 10ms
- 100 concurrent appends: < 50ms
"""

import asyncio
from typing import Any
from uuid import uuid4

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.ports.positions import ExpectedVersion
from tests.benchmarks.conftest import run_async
from tests.fixtures import SampleEvent


class TestEventStoreAppendBenchmarks:
    """Benchmarks for event store append operations."""

    def test_append_single_event(
        self,
        benchmark: Any,
        benchmark_store: InMemoryEventStore,
        event_generator: Any,
    ) -> None:
        """
        Benchmark: Append a single event to a new aggregate.

        This measures the baseline performance for the most common
        write operation in event sourcing.
        """

        def append_one() -> Any:
            aggregate_id = uuid4()
            event = event_generator(SampleEvent, aggregate_id=aggregate_id)
            stream_id = StreamId(str(aggregate_id), "TestAggregate")
            return run_async(
                benchmark_store.append(
                    stream_id,
                    [event],
                    ExpectedVersion.exact(0),
                )
            )

        result = benchmark(append_one)
        assert result.new_version == 1

    def test_append_batch_10_events(
        self,
        benchmark: Any,
        event_generator: Any,
    ) -> None:
        """
        Benchmark: Append 10 events in a single batch.

        Batch appends are more efficient than individual appends.
        """
        aggregate_id = uuid4()
        events = [
            event_generator(
                SampleEvent,
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
            )
            for i in range(10)
        ]

        def append_batch() -> Any:
            store = InMemoryEventStore()
            stream_id = StreamId(str(aggregate_id), "TestAggregate")
            return run_async(
                store.append(
                    stream_id,
                    events,
                    ExpectedVersion.exact(0),
                )
            )

        result = benchmark(append_batch)
        assert result.new_version == 10

    def test_append_batch_100_events(
        self,
        benchmark: Any,
        event_generator: Any,
    ) -> None:
        """
        Benchmark: Append 100 events in a single batch.
        """
        aggregate_id = uuid4()
        events = [
            event_generator(
                SampleEvent,
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
            )
            for i in range(100)
        ]

        def append_batch() -> Any:
            store = InMemoryEventStore()
            stream_id = StreamId(str(aggregate_id), "TestAggregate")
            return run_async(
                store.append(
                    stream_id,
                    events,
                    ExpectedVersion.exact(0),
                )
            )

        result = benchmark(append_batch)
        assert result.new_version == 100

    def test_append_sequential_events(
        self,
        benchmark: Any,
        event_generator: Any,
    ) -> None:
        """
        Benchmark: Append 100 events sequentially (one at a time).

        This simulates a long-running aggregate with many individual updates.
        """

        def append_sequential() -> None:
            store = InMemoryEventStore()
            aggregate_id = uuid4()

            async def do_appends() -> None:
                stream_id = StreamId(str(aggregate_id), "TestAggregate")
                for i in range(100):
                    event = event_generator(
                        SampleEvent,
                        aggregate_id=aggregate_id,
                        aggregate_version=i + 1,
                    )
                    await store.append(
                        stream_id,
                        [event],
                        ExpectedVersion.exact(i),
                    )

            run_async(do_appends())

        benchmark.pedantic(append_sequential, rounds=10)


class TestEventStoreReadBenchmarks:
    """Benchmarks for event store read operations."""

    def test_read_100_events(
        self,
        benchmark: Any,
        populated_benchmark_store_100: InMemoryEventStore,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Read 100 events from a single aggregate.
        """
        aggregate_id = sample_events_100[0].aggregate_id

        def read_events() -> list[Any]:
            async def collect_events() -> list[Any]:
                stream_id = StreamId(str(aggregate_id), "TestAggregate")
                envelopes = []
                async for envelope in populated_benchmark_store_100.read_stream(stream_id):
                    envelopes.append(envelope)
                return envelopes

            return run_async(collect_events())

        result = benchmark(read_events)
        assert len(result) == 100

    def test_read_1000_events(
        self,
        benchmark: Any,
        populated_benchmark_store_1000: InMemoryEventStore,
        sample_events_1000: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Read 1000 events from a single aggregate.
        """
        aggregate_id = sample_events_1000[0].aggregate_id

        def read_events() -> list[Any]:
            async def collect_events() -> list[Any]:
                stream_id = StreamId(str(aggregate_id), "TestAggregate")
                envelopes = []
                async for envelope in populated_benchmark_store_1000.read_stream(stream_id):
                    envelopes.append(envelope)
                return envelopes

            return run_async(collect_events())

        result = benchmark(read_events)
        assert len(result) == 1000

    def test_read_events_by_type(
        self,
        benchmark: Any,
        populated_benchmark_store_100: InMemoryEventStore,
    ) -> None:
        """
        Benchmark: Read all events of a specific aggregate type.
        """

        def read_by_type() -> list[Any]:
            async def collect_all() -> list[Any]:
                envelopes = []
                async for envelope in populated_benchmark_store_100.read_all(from_position=None):
                    envelopes.append(envelope)
                return envelopes

            return run_async(collect_all())

        result = benchmark(read_by_type)
        assert len(result) == 100

    def test_read_stream_iterator(
        self,
        benchmark: Any,
        populated_benchmark_store_100: InMemoryEventStore,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Read events using async iterator (read_stream).
        """
        aggregate_id = sample_events_100[0].aggregate_id
        stream_id = StreamId(str(aggregate_id), "TestAggregate")

        def read_stream() -> list[Any]:
            async def collect() -> list[Any]:
                events: list[Any] = []
                async for envelope in populated_benchmark_store_100.read_stream(stream_id):
                    events.append(envelope)
                return events

            result: list[Any] = run_async(collect())
            return result

        result = benchmark(read_stream)
        assert len(result) == 100

    def test_get_stream_version(
        self,
        benchmark: Any,
        populated_benchmark_store_1000: InMemoryEventStore,
        sample_events_1000: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Get the current version of an aggregate.
        """
        aggregate_id = sample_events_1000[0].aggregate_id

        def get_version() -> int:
            stream_id = StreamId(str(aggregate_id), "TestAggregate")
            version: int = run_async(populated_benchmark_store_1000.get_stream_version(stream_id))
            return version

        version = benchmark(get_version)
        assert version == 1000


class TestEventStoreConcurrencyBenchmarks:
    """Benchmarks for concurrent event store operations."""

    def test_concurrent_appends_to_different_aggregates(
        self,
        benchmark: Any,
        event_generator: Any,
    ) -> None:
        """
        Benchmark: 100 concurrent appends to different aggregates.

        This tests how well the event store handles concurrent writes
        to independent aggregates.
        """

        def concurrent_appends() -> None:
            store = InMemoryEventStore()

            async def do_concurrent() -> None:
                tasks = []
                for _ in range(100):
                    aggregate_id = uuid4()
                    event = event_generator(SampleEvent, aggregate_id=aggregate_id)
                    stream_id = StreamId(str(aggregate_id), "TestAggregate")
                    tasks.append(
                        store.append(
                            stream_id,
                            [event],
                            ExpectedVersion.exact(0),
                        )
                    )
                await asyncio.gather(*tasks)

            run_async(do_concurrent())

        benchmark.pedantic(concurrent_appends, rounds=10)

    def test_concurrent_reads(
        self,
        benchmark: Any,
        populated_benchmark_store_100: InMemoryEventStore,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: 100 concurrent read operations.

        This tests how well the event store handles concurrent reads.
        """
        aggregate_id = sample_events_100[0].aggregate_id
        stream_id = StreamId(str(aggregate_id), "TestAggregate")

        def concurrent_reads() -> None:
            async def do_concurrent() -> None:
                tasks = []
                for _ in range(100):

                    async def read_one() -> list[Any]:
                        envelopes = []
                        async for envelope in populated_benchmark_store_100.read_stream(stream_id):
                            envelopes.append(envelope)
                        return envelopes

                    tasks.append(read_one())
                await asyncio.gather(*tasks)

            run_async(do_concurrent())

        benchmark.pedantic(concurrent_reads, rounds=10)

    def test_mixed_read_write_workload(
        self,
        benchmark: Any,
        event_generator: Any,
    ) -> None:
        """
        Benchmark: Mixed workload with reads and writes.

        This simulates a realistic workload with both reads and writes
        happening concurrently.
        """

        def mixed_workload() -> None:
            store = InMemoryEventStore()
            aggregate_ids = [uuid4() for _ in range(10)]

            # Pre-populate with some events
            async def setup() -> None:
                for agg_id in aggregate_ids:
                    events = [
                        event_generator(
                            SampleEvent,
                            aggregate_id=agg_id,
                            aggregate_version=i + 1,
                        )
                        for i in range(10)
                    ]
                    stream_id = StreamId(str(agg_id), "TestAggregate")
                    await store.append(
                        stream_id,
                        events,
                        ExpectedVersion.exact(0),
                    )

            run_async(setup())

            # Mixed operations
            async def do_mixed() -> None:
                read_tasks = []
                write_tasks = []
                for i in range(50):
                    agg_id = aggregate_ids[i % len(aggregate_ids)]
                    if i % 2 == 0:
                        # Read operation
                        async def read_one(aid: Any) -> list[Any]:
                            stream_id = StreamId(str(aid), "TestAggregate")
                            envelopes = []
                            async for envelope in store.read_stream(stream_id):
                                envelopes.append(envelope)
                            return envelopes

                        read_tasks.append(read_one(agg_id))
                    else:
                        # Write operation to new aggregate
                        new_agg_id = uuid4()
                        event = event_generator(SampleEvent, aggregate_id=new_agg_id)
                        stream_id = StreamId(str(new_agg_id), "TestAggregate")
                        write_tasks.append(
                            store.append(
                                stream_id,
                                [event],
                                ExpectedVersion.exact(0),
                            )
                        )
                # Run all tasks concurrently
                await asyncio.gather(*read_tasks, *write_tasks)

            run_async(do_mixed())

        benchmark.pedantic(mixed_workload, rounds=10)


class TestEventStoreIdempotencyBenchmarks:
    """Benchmarks for idempotency-related operations."""

    def test_event_exists_check(
        self,
        benchmark: Any,
        populated_benchmark_store_100: InMemoryEventStore,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Check if an event exists (idempotency check).
        """
        event_id = sample_events_100[50].event_id

        def check_exists() -> bool:
            exists: bool = run_async(populated_benchmark_store_100.event_exists(event_id))
            return exists

        result = benchmark(check_exists)
        assert result is True

    def test_event_exists_check_not_found(
        self,
        benchmark: Any,
        populated_benchmark_store_100: InMemoryEventStore,
    ) -> None:
        """
        Benchmark: Check for non-existent event ID.
        """
        non_existent_id = uuid4()

        def check_not_exists() -> bool:
            exists: bool = run_async(populated_benchmark_store_100.event_exists(non_existent_id))
            return exists

        result = benchmark(check_not_exists)
        assert result is False
