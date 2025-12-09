"""
Performance benchmarks for tracing overhead.

Tests for:
- Tracing overhead measurement (append_events, get_events)
- Event bus tracing overhead
- Repository tracing overhead
- Acceptable overhead thresholds

These benchmarks help ensure that tracing instrumentation does not
significantly impact application performance.

Note:
    Performance tests are marked with 'benchmark' marker and may be
    excluded from regular test runs. Run with:
    pytest tests/integration/observability/test_tracing_performance.py -v
"""

from __future__ import annotations

import statistics
import time
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource import DomainEvent, InMemoryEventBus, register_event
from eventsource.aggregates.repository import AggregateRepository
from eventsource.stores.in_memory import InMemoryEventStore

from .conftest import TracingTestEvent

if TYPE_CHECKING:
    pass


pytestmark = [pytest.mark.integration, pytest.mark.benchmark]


# ============================================================================
# Performance Configuration
# ============================================================================

# Number of iterations for benchmarks
BENCHMARK_ITERATIONS = 100

# Maximum acceptable overhead percentage
# Note: For in-memory operations, tracing adds a fixed overhead (~30-70 microseconds)
# which results in high percentage overhead for very fast operations.
# For production with I/O-bound operations (database, network), the relative
# overhead is much smaller because the baseline is longer.
#
# Maximum acceptable absolute overhead in microseconds per operation
# For in-memory operations, we focus on absolute overhead rather than percentage
# because the baseline is so fast that percentage overhead is not meaningful.
# Tracing adds ~50-200us per span, which is acceptable for observability benefits.
# For operations with multiple spans (e.g., dispatch + multiple handlers),
# the threshold needs to be higher.
MAX_ABSOLUTE_OVERHEAD_US = 250.0  # 250 microseconds per single span

# For tests with multiple spans (e.g., event bus with multiple handlers)
MAX_ABSOLUTE_OVERHEAD_MULTI_SPAN_US = 750.0  # 750 microseconds for multi-span operations

# Minimum time threshold (microseconds) below which overhead calculation is unreliable
MIN_BASELINE_TIME_US = 5


# ============================================================================
# Test Events for Benchmarks
# ============================================================================


@register_event
class BenchmarkEventCreated(DomainEvent):
    """Event for benchmark aggregate creation."""

    event_type: str = "BenchmarkEventCreated"
    aggregate_type: str = "BenchmarkAggregate"
    name: str


# ============================================================================
# Test Aggregate for Repository Benchmarks
# ============================================================================


class BenchmarkAggregateState:
    """State for BenchmarkAggregate."""

    def __init__(self) -> None:
        self.name: str = ""


class BenchmarkAggregate:
    """Simple aggregate for benchmarking."""

    aggregate_type = "BenchmarkAggregate"
    schema_version = 1

    def __init__(self, aggregate_id):
        self.aggregate_id = aggregate_id
        self._version = 0
        self._uncommitted_events: list[DomainEvent] = []
        self._state = BenchmarkAggregateState()

    @property
    def version(self):
        return self._version

    @property
    def uncommitted_events(self):
        return self._uncommitted_events

    @property
    def state(self):
        return self._state

    def mark_events_as_committed(self):
        self._uncommitted_events.clear()

    def apply_event(self, event: DomainEvent, is_new: bool = True) -> None:
        if isinstance(event, BenchmarkEventCreated):
            self._state.name = event.name

        if is_new:
            self._uncommitted_events.append(event)

        self._version += 1

    def load_from_history(self, events: list[DomainEvent]) -> None:
        for event in events:
            self.apply_event(event, is_new=False)

    def create(self, name: str) -> None:
        event = BenchmarkEventCreated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self._version + 1,
            name=name,
        )
        self.apply_event(event)


# ============================================================================
# Helper Functions
# ============================================================================


def calculate_overhead(baseline_times: list[float], traced_times: list[float]) -> dict:
    """
    Calculate overhead statistics from timing measurements.

    Args:
        baseline_times: List of execution times without tracing (seconds)
        traced_times: List of execution times with tracing (seconds)

    Returns:
        Dictionary with overhead statistics
    """
    baseline_mean = statistics.mean(baseline_times)
    traced_mean = statistics.mean(traced_times)

    # Avoid division by zero
    if baseline_mean == 0:
        overhead_percent = 0.0
    else:
        overhead_percent = ((traced_mean - baseline_mean) / baseline_mean) * 100

    return {
        "baseline_mean_us": baseline_mean * 1_000_000,
        "traced_mean_us": traced_mean * 1_000_000,
        "overhead_us": (traced_mean - baseline_mean) * 1_000_000,
        "overhead_percent": overhead_percent,
        "baseline_stdev_us": statistics.stdev(baseline_times) * 1_000_000
        if len(baseline_times) > 1
        else 0,
        "traced_stdev_us": statistics.stdev(traced_times) * 1_000_000
        if len(traced_times) > 1
        else 0,
    }


# ============================================================================
# Event Store Performance Tests
# ============================================================================


class TestEventStoreTracingOverhead:
    """Performance tests for event store tracing overhead."""

    @pytest.mark.asyncio
    async def test_append_events_tracing_overhead(self):
        """Measure tracing overhead for append_events operation.

        This test verifies that the tracing overhead for appending events
        is within acceptable limits.
        """
        store_no_trace = InMemoryEventStore(enable_tracing=False)
        store_with_trace = InMemoryEventStore(enable_tracing=True)

        # Warm up
        for _ in range(10):
            aggregate_id = uuid4()
            event = TracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="warmup",
            )
            await store_no_trace.append_events(aggregate_id, "TracingTestAggregate", [event], 0)
            await store_with_trace.append_events(aggregate_id, "TracingTestAggregate", [event], 0)

        # Benchmark without tracing
        baseline_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate_id = uuid4()
            event = TracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="benchmark",
            )
            start = time.perf_counter()
            await store_no_trace.append_events(aggregate_id, "TracingTestAggregate", [event], 0)
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate_id = uuid4()
            event = TracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="benchmark",
            )
            start = time.perf_counter()
            await store_with_trace.append_events(aggregate_id, "TracingTestAggregate", [event], 0)
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        # Log results
        print("\nappend_events tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        # Assert absolute overhead is acceptable (percentage is not meaningful for fast ops)
        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_US}us)"
        )

    @pytest.mark.asyncio
    async def test_get_events_tracing_overhead(self):
        """Measure tracing overhead for get_events operation."""
        store_no_trace = InMemoryEventStore(enable_tracing=False)
        store_with_trace = InMemoryEventStore(enable_tracing=True)

        # Set up test data - create aggregates with events
        aggregate_ids = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate_id = uuid4()
            aggregate_ids.append(aggregate_id)

            events = [
                TracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=j + 1,
                    name=f"event_{j}",
                )
                for j in range(5)  # 5 events per aggregate
            ]
            await store_no_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)
            await store_with_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)

        # Benchmark without tracing
        baseline_times: list[float] = []
        for aggregate_id in aggregate_ids:
            start = time.perf_counter()
            await store_no_trace.get_events(aggregate_id, "TracingTestAggregate")
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for aggregate_id in aggregate_ids:
            start = time.perf_counter()
            await store_with_trace.get_events(aggregate_id, "TracingTestAggregate")
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        print("\nget_events tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_US}us)"
        )


# ============================================================================
# Event Bus Performance Tests
# ============================================================================


class TestEventBusTracingOverhead:
    """Performance tests for event bus tracing overhead."""

    @pytest.mark.asyncio
    async def test_publish_tracing_overhead(self):
        """Measure tracing overhead for event bus publish operation."""
        bus_no_trace = InMemoryEventBus(enable_tracing=False)
        bus_with_trace = InMemoryEventBus(enable_tracing=True)

        # Register a simple handler
        async def handler(event: DomainEvent) -> None:
            pass

        bus_no_trace.subscribe(TracingTestEvent, handler)
        bus_with_trace.subscribe(TracingTestEvent, handler)

        # Warm up
        for _ in range(10):
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="warmup",
            )
            await bus_no_trace.publish([event])
            await bus_with_trace.publish([event])

        # Benchmark without tracing
        baseline_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="benchmark",
            )
            start = time.perf_counter()
            await bus_no_trace.publish([event])
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="benchmark",
            )
            start = time.perf_counter()
            await bus_with_trace.publish([event])
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        print("\nevent bus publish tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_US}us)"
        )

    @pytest.mark.asyncio
    async def test_multiple_handlers_tracing_overhead(self):
        """Measure tracing overhead with multiple event handlers."""
        bus_no_trace = InMemoryEventBus(enable_tracing=False)
        bus_with_trace = InMemoryEventBus(enable_tracing=True)

        # Register multiple handlers
        for _ in range(5):

            async def handler(event: DomainEvent) -> None:
                pass

            bus_no_trace.subscribe(TracingTestEvent, handler)
            bus_with_trace.subscribe(TracingTestEvent, handler)

        # Benchmark without tracing
        baseline_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="benchmark",
            )
            start = time.perf_counter()
            await bus_no_trace.publish([event])
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="benchmark",
            )
            start = time.perf_counter()
            await bus_with_trace.publish([event])
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        print("\nmultiple handlers tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        # Use multi-span threshold since this test creates 5 handler spans per event
        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_MULTI_SPAN_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_MULTI_SPAN_US}us)"
        )


# ============================================================================
# Repository Performance Tests
# ============================================================================


class TestRepositoryTracingOverhead:
    """Performance tests for repository tracing overhead."""

    @pytest.mark.asyncio
    async def test_save_tracing_overhead(self):
        """Measure tracing overhead for repository save operation."""
        store_no_trace = InMemoryEventStore(enable_tracing=False)
        store_with_trace = InMemoryEventStore(enable_tracing=True)

        repo_no_trace = AggregateRepository(
            event_store=store_no_trace,
            aggregate_factory=BenchmarkAggregate,
            aggregate_type="BenchmarkAggregate",
            enable_tracing=False,
        )

        repo_with_trace = AggregateRepository(
            event_store=store_with_trace,
            aggregate_factory=BenchmarkAggregate,
            aggregate_type="BenchmarkAggregate",
            enable_tracing=True,
        )

        # Warm up
        for _ in range(10):
            aggregate = BenchmarkAggregate(uuid4())
            aggregate.create("warmup")
            await repo_no_trace.save(aggregate)

            aggregate = BenchmarkAggregate(uuid4())
            aggregate.create("warmup")
            await repo_with_trace.save(aggregate)

        # Benchmark without tracing
        baseline_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate = BenchmarkAggregate(uuid4())
            aggregate.create("benchmark")
            start = time.perf_counter()
            await repo_no_trace.save(aggregate)
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate = BenchmarkAggregate(uuid4())
            aggregate.create("benchmark")
            start = time.perf_counter()
            await repo_with_trace.save(aggregate)
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        print("\nrepository save tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_US}us)"
        )

    @pytest.mark.asyncio
    async def test_load_tracing_overhead(self):
        """Measure tracing overhead for repository load operation."""
        store_no_trace = InMemoryEventStore(enable_tracing=False)
        store_with_trace = InMemoryEventStore(enable_tracing=True)

        repo_no_trace = AggregateRepository(
            event_store=store_no_trace,
            aggregate_factory=BenchmarkAggregate,
            aggregate_type="BenchmarkAggregate",
            enable_tracing=False,
        )

        repo_with_trace = AggregateRepository(
            event_store=store_with_trace,
            aggregate_factory=BenchmarkAggregate,
            aggregate_type="BenchmarkAggregate",
            enable_tracing=True,
        )

        # Create test aggregates - each repo gets its own set
        aggregate_ids_no_trace = []
        aggregate_ids_with_trace = []
        for i in range(BENCHMARK_ITERATIONS):
            # Create for no-trace repo
            aggregate_id_no_trace = uuid4()
            aggregate_ids_no_trace.append(aggregate_id_no_trace)
            aggregate = BenchmarkAggregate(aggregate_id_no_trace)
            aggregate.create(f"aggregate_{i}")
            await repo_no_trace.save(aggregate)

            # Create for traced repo
            aggregate_id_with_trace = uuid4()
            aggregate_ids_with_trace.append(aggregate_id_with_trace)
            aggregate = BenchmarkAggregate(aggregate_id_with_trace)
            aggregate.create(f"aggregate_{i}")
            await repo_with_trace.save(aggregate)

        # Benchmark without tracing
        baseline_times: list[float] = []
        for aggregate_id in aggregate_ids_no_trace:
            start = time.perf_counter()
            await repo_no_trace.load(aggregate_id)
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for aggregate_id in aggregate_ids_with_trace:
            start = time.perf_counter()
            await repo_with_trace.load(aggregate_id)
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        print("\nrepository load tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_US}us)"
        )


# ============================================================================
# Bulk Operation Performance Tests
# ============================================================================


class TestBulkOperationTracingOverhead:
    """Performance tests for bulk operations with tracing."""

    @pytest.mark.asyncio
    async def test_batch_append_tracing_overhead(self):
        """Measure tracing overhead when appending multiple events at once."""
        store_no_trace = InMemoryEventStore(enable_tracing=False)
        store_with_trace = InMemoryEventStore(enable_tracing=True)

        batch_size = 10

        # Warm up
        for _ in range(5):
            aggregate_id = uuid4()
            events = [
                TracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=j + 1,
                    name=f"warmup_{j}",
                )
                for j in range(batch_size)
            ]
            await store_no_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)
            await store_with_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)

        # Benchmark without tracing
        baseline_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate_id = uuid4()
            events = [
                TracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=j + 1,
                    name=f"batch_{j}",
                )
                for j in range(batch_size)
            ]
            start = time.perf_counter()
            await store_no_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)
            baseline_times.append(time.perf_counter() - start)

        # Benchmark with tracing
        traced_times: list[float] = []
        for _ in range(BENCHMARK_ITERATIONS):
            aggregate_id = uuid4()
            events = [
                TracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=j + 1,
                    name=f"batch_{j}",
                )
                for j in range(batch_size)
            ]
            start = time.perf_counter()
            await store_with_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)
            traced_times.append(time.perf_counter() - start)

        stats = calculate_overhead(baseline_times, traced_times)

        print(f"\nbatch append ({batch_size} events) tracing overhead:")
        print(f"  Baseline: {stats['baseline_mean_us']:.2f} us")
        print(f"  With tracing: {stats['traced_mean_us']:.2f} us")
        print(f"  Overhead: {stats['overhead_us']:.2f} us ({stats['overhead_percent']:.2f}%)")

        assert stats["overhead_us"] < MAX_ABSOLUTE_OVERHEAD_US, (
            f"Tracing overhead ({stats['overhead_us']:.2f}us) exceeds threshold "
            f"({MAX_ABSOLUTE_OVERHEAD_US}us)"
        )


# ============================================================================
# Summary Benchmark
# ============================================================================


class TestTracingOverheadSummary:
    """Summary test that provides an overall view of tracing overhead."""

    @pytest.mark.asyncio
    async def test_overall_tracing_overhead_acceptable(self):
        """
        Comprehensive test that verifies tracing overhead is acceptable
        across all major operations.

        This test performs a mix of operations and measures the aggregate
        overhead to ensure the tracing implementation is efficient.
        """
        store_no_trace = InMemoryEventStore(enable_tracing=False)
        store_with_trace = InMemoryEventStore(enable_tracing=True)

        bus_no_trace = InMemoryEventBus(enable_tracing=False)
        bus_with_trace = InMemoryEventBus(enable_tracing=True)

        # Register handlers
        async def handler(event: DomainEvent) -> None:
            pass

        bus_no_trace.subscribe(TracingTestEvent, handler)
        bus_with_trace.subscribe(TracingTestEvent, handler)

        # Mixed workload
        iterations = 50

        # Without tracing
        start_no_trace = time.perf_counter()
        for _ in range(iterations):
            aggregate_id = uuid4()
            events = [
                TracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=j + 1,
                    name=f"event_{j}",
                )
                for j in range(3)
            ]
            await store_no_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)
            await store_no_trace.get_events(aggregate_id, "TracingTestAggregate")
            await bus_no_trace.publish([events[0]])
        time_no_trace = time.perf_counter() - start_no_trace

        # With tracing
        start_with_trace = time.perf_counter()
        for _ in range(iterations):
            aggregate_id = uuid4()
            events = [
                TracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=j + 1,
                    name=f"event_{j}",
                )
                for j in range(3)
            ]
            await store_with_trace.append_events(aggregate_id, "TracingTestAggregate", events, 0)
            await store_with_trace.get_events(aggregate_id, "TracingTestAggregate")
            await bus_with_trace.publish([events[0]])
        time_with_trace = time.perf_counter() - start_with_trace

        overhead_percent = ((time_with_trace - time_no_trace) / time_no_trace) * 100
        overhead_ms = (time_with_trace - time_no_trace) * 1000
        # Per-iteration overhead in microseconds
        overhead_per_iter_us = (time_with_trace - time_no_trace) * 1_000_000 / iterations

        print("\nOverall mixed workload tracing overhead:")
        print(f"  Baseline: {time_no_trace * 1000:.2f} ms ({iterations} iterations)")
        print(f"  With tracing: {time_with_trace * 1000:.2f} ms")
        print(f"  Overhead: {overhead_ms:.2f} ms ({overhead_percent:.2f}%)")
        print(f"  Per-iteration overhead: {overhead_per_iter_us:.2f} us")

        # For mixed workloads with 50 iterations, each iteration has multiple spans
        # Allow up to MAX_ABSOLUTE_OVERHEAD_US * 10 per iteration (multiple operations)
        max_per_iter_us = MAX_ABSOLUTE_OVERHEAD_US * 10
        assert overhead_per_iter_us < max_per_iter_us, (
            f"Per-iteration overhead ({overhead_per_iter_us:.2f}us) exceeds threshold ({max_per_iter_us:.0f}us)"
        )
