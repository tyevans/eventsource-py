"""Benchmarks for outbox and concurrent repository operations."""

import asyncio
from typing import Any

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.domain.event import DomainEvent
from tests.benchmarks.conftest import run_async
from tests.fixtures import SampleEvent, create_event


class TestOutboxRepositoryBenchmarks:
    """Benchmarks for outbox repository operations."""

    def test_add_event_to_outbox(
        self,
        benchmark: Any,
        benchmark_outbox_repo: InMemoryOutboxRepository,
    ) -> None:
        """
        Benchmark: Add a single event to outbox.
        """
        event = create_event(SampleEvent)

        def add_one() -> None:
            run_async(benchmark_outbox_repo.add_event(event))

        benchmark(add_one)

    def test_add_100_events_to_outbox(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Add 100 events to outbox.
        """

        def add_many() -> None:
            repo = InMemoryOutboxRepository()

            async def do_adds() -> None:
                for event in sample_events_100:
                    await repo.add_event(event)

            run_async(do_adds())

        benchmark(add_many)

    def test_get_pending_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Get pending events from outbox.
        """
        repo = InMemoryOutboxRepository()

        # Pre-populate
        async def setup() -> None:
            for event in sample_events_100:
                await repo.add_event(event)

        run_async(setup())

        def get_pending() -> Any:
            return run_async(repo.get_pending_events(limit=50))

        result = benchmark(get_pending)
        assert len(result) == 50

    def test_mark_event_published(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Mark an event as published.
        """

        def mark_published() -> None:
            repo = InMemoryOutboxRepository()
            event = create_event(SampleEvent)

            async def setup_and_mark() -> None:
                outbox_id = await repo.add_event(event)
                await repo.mark_published(outbox_id)

            run_async(setup_and_mark())

        benchmark(mark_published)

    def test_outbox_workflow(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Complete outbox workflow (add, get pending, mark published).
        """

        def complete_workflow() -> None:
            repo = InMemoryOutboxRepository()

            async def workflow() -> None:
                # Add 10 events
                events = [create_event(SampleEvent) for _ in range(10)]
                for event in events:
                    await repo.add_event(event)

                # Get pending
                pending = await repo.get_pending_events(limit=10)

                # Mark all as published
                for entry in pending:
                    await repo.mark_published(entry.id)

            run_async(workflow())

        benchmark(complete_workflow)


class TestConcurrentRepositoryBenchmarks:
    """Benchmarks for concurrent repository operations."""

    def test_concurrent_checkpoint_updates(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: 100 concurrent checkpoint updates.
        """

        def concurrent_updates() -> None:
            repo = InMemoryCheckpointRepository()

            async def do_concurrent() -> None:
                tasks = [
                    repo.update_checkpoint(
                        projection_name=f"Projection_{i % 10}",
                        event_id=event.event_id,
                        event_type=event.event_type,
                    )
                    for i, event in enumerate(sample_events_100)
                ]
                await asyncio.gather(*tasks)

            run_async(do_concurrent())

        benchmark.pedantic(concurrent_updates, rounds=10)

    def test_concurrent_outbox_adds(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: 100 concurrent outbox additions.
        """

        def concurrent_adds() -> None:
            repo = InMemoryOutboxRepository()

            async def do_concurrent() -> None:
                tasks = [repo.add_event(event) for event in sample_events_100]
                await asyncio.gather(*tasks)

            run_async(do_concurrent())

        benchmark.pedantic(concurrent_adds, rounds=10)
