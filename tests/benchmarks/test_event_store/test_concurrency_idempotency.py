"""Benchmarks for event store concurrency and idempotency operations."""

import asyncio
from typing import Any
from uuid import uuid4

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import ExpectedVersion
from tests.benchmarks.conftest import run_async
from tests.fixtures import SampleEvent


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
                    stream_id = StreamId(aggregate_id, "TestAggregate")
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
        stream_id = StreamId(aggregate_id, "TestAggregate")

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
                    stream_id = StreamId(agg_id, "TestAggregate")
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
                            stream_id = StreamId(aid, "TestAggregate")
                            envelopes = []
                            async for envelope in store.read_stream(stream_id):
                                envelopes.append(envelope)
                            return envelopes

                        read_tasks.append(read_one(agg_id))
                    else:
                        # Write operation to new aggregate
                        new_agg_id = uuid4()
                        event = event_generator(SampleEvent, aggregate_id=new_agg_id)
                        stream_id = StreamId(new_agg_id, "TestAggregate")
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
