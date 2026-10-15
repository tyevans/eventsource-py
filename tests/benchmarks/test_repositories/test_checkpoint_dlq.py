"""Benchmarks for checkpoint and DLQ repository operations."""

from typing import Any

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.domain.event import DomainEvent
from tests.benchmarks.conftest import run_async
from tests.fixtures import SampleEvent, create_event


class TestCheckpointRepositoryBenchmarks:
    def test_update_checkpoint_single(
        self,
        benchmark: Any,
        benchmark_checkpoint_repo: InMemoryCheckpointRepository,
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
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
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
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
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
        self,
        benchmark: Any,
        populated_checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        def get_one() -> Any:
            return run_async(populated_checkpoint_repo.get_checkpoint("Projection_5"))

        result = benchmark(get_one)
        assert result is not None

    def test_get_checkpoint_not_found(
        self,
        benchmark: Any,
        benchmark_checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        def get_not_found() -> Any:
            return run_async(benchmark_checkpoint_repo.get_checkpoint("NonExistent"))

        result = benchmark(get_not_found)
        assert result is None

    def test_get_lag_metrics(
        self,
        benchmark: Any,
        populated_checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        def get_metrics() -> Any:
            return run_async(
                populated_checkpoint_repo.get_lag_metrics(
                    projection_name="Projection_0",
                    event_types=["SampleEvent"],
                )
            )

        result = benchmark(get_metrics)
        assert result is not None

    def test_reset_checkpoint(
        self,
        benchmark: Any,
    ) -> None:
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
        self,
        benchmark: Any,
        populated_checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        def get_all() -> Any:
            return run_async(populated_checkpoint_repo.get_all_checkpoints())

        result = benchmark(get_all)
        assert len(result) == 10


class TestDLQRepositoryBenchmarks:
    def test_add_failed_event(
        self,
        benchmark: Any,
        benchmark_dlq_repo: InMemoryDLQRepository,
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
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        error = Exception("Test error for benchmarking")

        def add_many() -> None:
            repo = InMemoryDLQRepository()

            async def do_adds() -> None:
                for event in sample_events_100:
                    await repo.add_failed_event(
                        event_id=event.event_id,
                        projection_name="BenchmarkProjection",
                        event_type=event.event_type,
                        event_data=event.model_dump(mode="json"),
                        error=error,
                        retry_count=1,
                    )

            run_async(do_adds())

        benchmark(add_many)

    def test_get_failed_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        error = Exception("Test error")
        repo = InMemoryDLQRepository()

        async def setup() -> None:
            for event in sample_events_100:
                await repo.add_failed_event(
                    event_id=event.event_id,
                    projection_name="BenchmarkProjection",
                    event_type=event.event_type,
                    event_data=event.model_dump(mode="json"),
                    error=error,
                    retry_count=1,
                )

        run_async(setup())

        def get_all() -> Any:
            return run_async(repo.get_failed_events())

        result = benchmark(get_all)
        assert len(result) == 100

    def test_get_failed_event_by_id(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        error = Exception("Test error")
        repo = InMemoryDLQRepository()

        async def setup() -> int:
            for event in sample_events_100:
                await repo.add_failed_event(
                    event_id=event.event_id,
                    projection_name="BenchmarkProjection",
                    event_type=event.event_type,
                    event_data=event.model_dump(mode="json"),
                    error=error,
                    retry_count=1,
                )
            return 50

        target_id = run_async(setup())

        def get_one() -> Any:
            return run_async(repo.get_failed_event_by_id(target_id))

        result = benchmark(get_one)
        assert result is not None
