"""Benchmarks for event types, round-trip serialization, and event copying."""

import json
from decimal import Decimal
from typing import Any
from uuid import uuid4

from eventsource.domain.event import DomainEvent
from tests.fixtures import (
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    SampleEvent,
    create_event,
)


class TestEventTypeBenchmarks:
    """Benchmarks for different event types (varying complexity)."""

    def test_serialize_counter_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Serialize simple counter event.
        """
        event = CounterIncremented(
            aggregate_id=uuid4(),
            aggregate_version=1,
            increment=5,
        )

        def serialize() -> dict[str, Any]:
            return event.to_dict()

        result = benchmark(serialize)
        assert result["increment"] == 5

    def test_serialize_order_created_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Serialize order created event (with UUID field).
        """
        event = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=uuid4(),
        )

        def serialize() -> dict[str, Any]:
            return event.to_dict()

        result = benchmark(serialize)
        assert "customer_id" in result

    def test_serialize_order_item_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Serialize order item added event (with string and float).
        """
        event = OrderItemAdded(
            aggregate_id=uuid4(),
            aggregate_version=2,
            item_name="Test Item",
            price=Decimal("99.99"),
        )

        def serialize() -> dict[str, Any]:
            return event.to_dict()

        result = benchmark(serialize)
        assert result["item_name"] == "Test Item"
        # to_dict() renders Decimal as its string form; see
        # docs/guides/money-and-precision.md
        assert result["price"] == "99.99"


class TestRoundTripBenchmarks:
    """Benchmarks for full serialization/deserialization round trips."""

    def test_round_trip_single_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Full round trip (serialize -> deserialize).
        """
        event = create_event(SampleEvent)

        def round_trip() -> SampleEvent:
            serialized = event.to_dict()
            return SampleEvent.from_dict(serialized)

        result = benchmark(round_trip)
        assert result.event_id == event.event_id

    def test_round_trip_via_json(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Full round trip via JSON string.
        """
        event = create_event(SampleEvent)

        def round_trip_json() -> SampleEvent:
            json_str = json.dumps(event.to_dict())
            return SampleEvent.from_dict(json.loads(json_str))

        result = benchmark(round_trip_json)
        assert result.event_id == event.event_id

    def test_round_trip_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Full round trip for 100 events.
        """

        def round_trip_batch() -> list[SampleEvent]:
            return [SampleEvent.from_dict(event.to_dict()) for event in sample_events_100]

        result = benchmark(round_trip_batch)
        assert len(result) == 100

    def test_round_trip_100_events_via_json(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Full round trip via JSON for 100 events.
        """

        def round_trip_json_batch() -> list[SampleEvent]:
            results = []
            for event in sample_events_100:
                json_str = json.dumps(event.to_dict())
                results.append(SampleEvent.from_dict(json.loads(json_str)))
            return results

        result = benchmark(round_trip_json_batch)
        assert len(result) == 100


class TestEventCopyBenchmarks:
    """Benchmarks for event copy operations (with_* methods)."""

    def test_with_aggregate_version(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Create copy with new aggregate version.
        """
        event = create_event(SampleEvent)

        def copy_with_version() -> DomainEvent:
            return event.with_aggregate_version(42)

        result = benchmark(copy_with_version)
        assert result.aggregate_version == 42

    def test_with_metadata(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Create copy with additional metadata.
        """
        event = create_event(SampleEvent)

        def copy_with_metadata() -> DomainEvent:
            return event.with_metadata(trace_id="abc123", source="benchmark")

        result = benchmark(copy_with_metadata)
        assert result.metadata["trace_id"] == "abc123"

    def test_with_causation(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Create copy with causation tracking.
        """
        event1 = create_event(SampleEvent)
        event2 = create_event(SampleEvent)

        def copy_with_causation() -> DomainEvent:
            return event2.with_causation(event1)

        result = benchmark(copy_with_causation)
        assert result.causation_id == event1.event_id
