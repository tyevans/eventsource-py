"""Benchmarks for projection event processing throughput and checkpoint tracking."""

from typing import Any
from uuid import uuid4

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.domain.event import DomainEvent
from tests.benchmarks.conftest import run_async
from tests.benchmarks.test_projections._projections import CountingProjection
from tests.fixtures import (
    CounterDecremented,
    CounterIncremented,
    SampleEvent,
    create_event,
)


class TestProjectionProcessingBenchmarks:
    """Benchmarks for projection event processing throughput."""

    def test_process_single_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Process a single event through a projection.
        """
        projection = CountingProjection()
        event = create_event(SampleEvent)

        def process_one() -> None:
            run_async(projection.handle(event))

        benchmark(process_one)
        assert projection.count > 0

    def test_process_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process 100 events through a projection.
        """

        def process_batch() -> int:
            projection = CountingProjection()

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_batch)
        assert count == 100

    def test_process_1000_events(
        self,
        benchmark: Any,
        sample_events_1000: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process 1000 events through a projection.
        """

        def process_batch() -> int:
            projection = CountingProjection()

            async def process_all() -> None:
                for event in sample_events_1000:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark.pedantic(process_batch, rounds=10)
        assert count == 1000

    def test_process_with_multiple_handlers(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Process events through projection with multiple handlers.
        """
        events: list[DomainEvent] = []
        aggregate_id = uuid4()

        # Create a mix of event types
        for i in range(100):
            if i % 2 == 0:
                events.append(
                    CounterIncremented(
                        aggregate_id=aggregate_id,
                        aggregate_version=i + 1,
                        increment=1,
                    )
                )
            else:
                events.append(
                    CounterDecremented(
                        aggregate_id=aggregate_id,
                        aggregate_version=i + 1,
                        decrement=1,
                    )
                )

        def process_mixed() -> int:
            projection = CountingProjection()

            async def process_all() -> None:
                for event in events:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_mixed)
        assert count == 100


class TestProjectionCheckpointBenchmarks:
    """Benchmarks for projection checkpoint tracking overhead."""

    def test_process_with_checkpoint_tracking(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process events with checkpoint tracking enabled.

        This measures the overhead of checkpoint persistence.
        """

        def process_with_checkpoint() -> int:
            checkpoint_repo = InMemoryCheckpointRepository()
            projection = CountingProjection()
            projection._checkpoint_repo = checkpoint_repo

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_with_checkpoint)
        assert count == 100

    def test_checkpoint_overhead_comparison(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Compare processing with and without checkpoint tracking.

        This uses pedantic mode to get accurate timing.
        """

        def process_without_checkpoint() -> int:
            # Create projection with in-memory checkpoint (minimal overhead)
            projection = CountingProjection()

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection._process_event(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_without_checkpoint)
        assert count == 100
