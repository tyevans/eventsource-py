"""Benchmarks for event store append and read operations."""

from typing import Any
from uuid import uuid4

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
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
            stream_id = StreamId(aggregate_id, "TestAggregate")
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
            stream_id = StreamId(aggregate_id, "TestAggregate")
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
            stream_id = StreamId(aggregate_id, "TestAggregate")
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
                stream_id = StreamId(aggregate_id, "TestAggregate")
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
                stream_id = StreamId(aggregate_id, "TestAggregate")
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
                stream_id = StreamId(aggregate_id, "TestAggregate")
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
        stream_id = StreamId(aggregate_id, "TestAggregate")

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
            stream_id = StreamId(aggregate_id, "TestAggregate")
            version: int = run_async(populated_benchmark_store_1000.get_stream_version(stream_id))
            return version

        version = benchmark(get_version)
        assert version == 1000
