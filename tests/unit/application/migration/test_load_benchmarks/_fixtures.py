"""Shared fixtures, models, and helpers for migration load benchmarks."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports import EventEnvelope, ExpectedVersion, FeedReadOptions
from eventsource.ports.positions import Position

SOURCE_STORE_ID = "source-store"
TARGET_STORE_ID = "target-store"


def src_pos(n: int) -> Position:
    """A source-store position token for the int the mapper is keyed on."""
    return Position(store_id=SOURCE_STORE_ID, key=(n,))


def tgt_pos(n: int) -> Position:
    """A target-store position token, as the migrator writes back."""
    return Position(store_id=TARGET_STORE_ID, key=(n,))


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class BenchmarkEvent(DomainEvent):
    """Test event for benchmarks."""

    __test__ = False

    aggregate_type: str = "BenchmarkAggregate"
    payload: str = ""


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    __test__ = False

    name: str
    duration_seconds: float
    operations_count: int
    operations_per_second: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_timing(
        cls,
        name: str,
        start_time: float,
        end_time: float,
        operations_count: int,
        metadata: dict[str, Any] | None = None,
    ) -> BenchmarkResult:
        """Create result from timing measurements."""
        duration = end_time - start_time
        ops_per_second = operations_count / duration if duration > 0 else 0
        return cls(
            name=name,
            duration_seconds=duration,
            operations_count=operations_count,
            operations_per_second=ops_per_second,
            metadata=metadata or {},
        )


@dataclass
class LatencyResult:
    """Result of latency benchmarks."""

    __test__ = False

    name: str
    latencies_ms: list[float]
    min_ms: float
    max_ms: float
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_samples(
        cls,
        name: str,
        samples: list[float],
        metadata: dict[str, Any] | None = None,
    ) -> LatencyResult:
        """Create result from latency samples."""
        sorted_samples = sorted(samples)
        n = len(sorted_samples)

        return cls(
            name=name,
            latencies_ms=samples,
            min_ms=min(samples),
            max_ms=max(samples),
            mean_ms=statistics.mean(samples),
            p50_ms=sorted_samples[n // 2],
            p95_ms=sorted_samples[int(n * 0.95)],
            p99_ms=sorted_samples[int(n * 0.99)],
            metadata=metadata or {},
        )


@dataclass
class MemoryResult:
    """Result of memory benchmarks."""

    __test__ = False

    name: str
    peak_memory_mb: float
    current_memory_mb: float
    objects_tracked: int
    metadata: dict[str, Any] = field(default_factory=dict)


async def create_test_events(
    store: InMemoryEventStore,
    tenant_id: UUID,
    count: int,
    aggregate_count: int = 100,
    payload_size: int = 100,
) -> list[EventEnvelope]:
    """
    Create test events in a store.

    Args:
        store: Target event store
        tenant_id: Tenant ID for events
        count: Number of events to create
        aggregate_count: Number of unique aggregates
        payload_size: Size of payload data in bytes

    Returns:
        List of stored events
    """
    payload = "x" * payload_size
    stored_events: list[EventEnvelope] = []

    events_per_aggregate = count // aggregate_count
    remaining = count % aggregate_count

    for agg_index in range(aggregate_count):
        aggregate_id = uuid4()
        num_events = events_per_aggregate + (1 if agg_index < remaining else 0)

        events = [
            BenchmarkEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                payload=payload,
            )
            for _ in range(num_events)
        ]

        if events:
            await store.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                events,
                ExpectedVersion.no_stream(),
            )

    # Collect all stored events
    async for envelope in store.read_all(None, FeedReadOptions(tenant_id=tenant_id)):
        stored_events.append(envelope)

    return stored_events
