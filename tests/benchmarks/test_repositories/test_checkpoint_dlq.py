"""Benchmarks for checkpoint and DLQ repository operations."""

from typing import Any

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.domain.event import DomainEvent
from tests.benchmarks.conftest import run_async
from tests.fixtures import SampleEvent, create_event


async def _populate_dlq(repo: InMemoryDLQRepository, events: list[DomainEvent]) -> None:
    error = Exception("Test error")
    for event in events:
        await repo.add_failed_event(
            event_id=event.event_id,
            projection_name="BenchmarkProjection",
            event_type=event.event_type,
            event_data=event.model_dump(mode="json"),
            error=error,
            retry_count=1,
        )


class TestCheckpointRepositoryBenchmarks:
    def test_update_checkpoint_single(
        self, benchmark: Any, benchmark_checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        event = create_event(SampleEvent)

        def update_one() -> None:
            run_async(
                benchmark_checkpoint_repo.update_checkpoint(
                    projection_name="BenchmarkProjection",
                    event_id=event.event_id,
                    event_type=event.event_type,
                )
            )

        benchmark(update_one)

    def test_update_checkpoint_100_times(
        self, benchmark: Any, sample_events_100: list[DomainEvent]
    ) -> None:
        def update_many() -> None:
            repo = InMemoryCheckpointRepository()

            async def do_updates() -> None:
                for event in sample_events_100:
                    await repo.update_checkpoint(
                        projection_name="BenchmarkProjection",
                        event_id=event.event_id,
                        event_type=event.event_type,
                    )

            run_async(do_updates())

        benchmark(update_many)

    def test_update_checkpoint_multiple_projections(
        self, benchmark: Any, sample_events_100: list[DomainEvent]
    ) -> None:
        def update_multiple() -> None:
            repo = InMemoryCheckpointRepository()

            async def do_updates() -> None:
                for i, event in enumerate(sample_events_100):
                    await repo.update_checkpoint(
                        projection_name=f"Projection_{i % 10}",
                        event_id=event.event_id,
                        event_type=event.event_type,
                    )

            run_async(do_updates())

        benchmark(update_multiple)

    def test_get_checkpoint(
        self, benchmark: Any, populated_checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        result = benchmark(
            lambda: run_async(populated_checkpoint_repo.get_checkpoint("Projection_5"))
        )
        assert result is not None

    def test_get_checkpoint_not_found(
        self, benchmark: Any, benchmark_checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        result = benchmark(
            lambda: run_async(benchmark_checkpoint_repo.get_checkpoint("NonExistent"))
        )
        assert result is None

    def test_get_lag_metrics(
        self, benchmark: Any, populated_checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        def get_metrics() -> Any:
            return run_async(
                populated_checkpoint_repo.get_lag_metrics(
                    projection_name="Projection_0", event_types=["SampleEvent"]
                )
            )

        assert benchmark(get_metrics) is not None

    def test_reset_checkpoint(self, benchmark: Any) -> None:
        def reset_checkpoint() -> None:
            repo = InMemoryCheckpointRepository()
            event = create_event(SampleEvent)

            async def setup_and_reset() -> None:
                await repo.update_checkpoint(
                    projection_name="BenchmarkProjection",
                    event_id=event.event_id,
                    event_type=event.event_type,
                )
                await repo.reset_checkpoint("BenchmarkProjection")

            run_async(setup_and_reset())

        benchmark(reset_checkpoint)

    def test_get_all_checkpoints(
        self, benchmark: Any, populated_checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        result = benchmark(lambda: run_async(populated_checkpoint_repo.get_all_checkpoints()))
        assert len(result) == 10


class TestDLQRepositoryBenchmarks:
    def test_add_failed_event(
        self, benchmark: Any, benchmark_dlq_repo: InMemoryDLQRepository
    ) -> None:
        event = create_event(SampleEvent)
        error = Exception("Test error for benchmarking")

        def add_one() -> None:
            run_async(
                benchmark_dlq_repo.add_failed_event(
                    event_id=event.event_id,
                    projection_name="BenchmarkProjection",
                    event_type=event.event_type,
                    event_data=event.model_dump(mode="json"),
                    error=error,
                    retry_count=1,
                )
            )

        benchmark(add_one)

    def test_add_100_failed_events(
        self, benchmark: Any, sample_events_100: list[DomainEvent]
    ) -> None:
        def add_many() -> None:
            run_async(_populate_dlq(InMemoryDLQRepository(), sample_events_100))

        benchmark(add_many)

    def test_get_failed_events(self, benchmark: Any, sample_events_100: list[DomainEvent]) -> None:
        repo = InMemoryDLQRepository()
        run_async(_populate_dlq(repo, sample_events_100))
        result = benchmark(lambda: run_async(repo.get_failed_events()))
        assert len(result) == 100

    def test_get_failed_event_by_id(
        self, benchmark: Any, sample_events_100: list[DomainEvent]
    ) -> None:
        repo = InMemoryDLQRepository()
        run_async(_populate_dlq(repo, sample_events_100))
        result = benchmark(lambda: run_async(repo.get_failed_event_by_id(50)))
        assert result is not None
