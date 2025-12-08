"""
Performance benchmarks for serialization/deserialization operations.

Tests critical paths:
- Event to_dict() serialization
- Event from_dict() deserialization
- Pydantic model_dump() and model_validate()
- JSON-compatible serialization

Target baselines:
- Single event serialization: < 0.1ms
- Single event deserialization: < 0.1ms
- Batch serialization (100 events): < 10ms
- Batch deserialization (100 events): < 10ms
"""

import json
from typing import Any
from uuid import uuid4

from eventsource.events.base import DomainEvent
from tests.fixtures import (
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    SampleEvent,
    create_event,
)


class TestEventSerializationBenchmarks:
    """Benchmarks for event serialization to dictionaries."""

    def test_serialize_single_event_to_dict(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Serialize a single event to dictionary.
        """
        event = create_event(SampleEvent)

        def serialize_one() -> dict[str, Any]:
            return event.to_dict()

        result = benchmark(serialize_one)
        assert "event_id" in result
        assert "event_type" in result

    def test_serialize_single_event_model_dump(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Serialize using model_dump() directly.
        """
        event = create_event(SampleEvent)

        def dump_one() -> dict[str, Any]:
            return event.model_dump(mode="json")

        result = benchmark(dump_one)
        assert "event_id" in result

    def test_serialize_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Serialize 100 events to dictionaries.
        """

        def serialize_batch() -> list[dict[str, Any]]:
            return [event.to_dict() for event in sample_events_100]

        result = benchmark(serialize_batch)
        assert len(result) == 100

    def test_serialize_1000_events(
        self,
        benchmark: Any,
        sample_events_1000: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Serialize 1000 events to dictionaries.
        """

        def serialize_batch() -> list[dict[str, Any]]:
            return [event.to_dict() for event in sample_events_1000]

        result = benchmark.pedantic(serialize_batch, rounds=10)
        assert len(result) == 1000

    def test_serialize_complex_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Serialize event with nested metadata.
        """
        event = create_event(SampleEvent).with_metadata(
            trace_id="trace-123",
            request_id="req-456",
            user_agent="benchmark-test",
            custom_data={"nested": {"key": "value", "list": [1, 2, 3]}},
        )

        def serialize_complex() -> dict[str, Any]:
            return event.to_dict()

        result = benchmark(serialize_complex)
        assert result["metadata"]["trace_id"] == "trace-123"


class TestEventDeserializationBenchmarks:
    """Benchmarks for event deserialization from dictionaries."""

    def test_deserialize_single_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Deserialize a single event from dictionary.
        """
        event = create_event(SampleEvent)
        event_dict = event.to_dict()

        def deserialize_one() -> SampleEvent:
            return SampleEvent.from_dict(event_dict)

        result = benchmark(deserialize_one)
        assert result.event_id == event.event_id

    def test_deserialize_single_event_model_validate(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Deserialize using model_validate() directly.
        """
        event = create_event(SampleEvent)
        event_dict = event.to_dict()

        def validate_one() -> SampleEvent:
            return SampleEvent.model_validate(event_dict)

        result = benchmark(validate_one)
        assert result.event_id == event.event_id

    def test_deserialize_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Deserialize 100 events from dictionaries.
        """
        event_dicts = [event.to_dict() for event in sample_events_100]

        def deserialize_batch() -> list[SampleEvent]:
            return [SampleEvent.from_dict(d) for d in event_dicts]

        result = benchmark(deserialize_batch)
        assert len(result) == 100

    def test_deserialize_1000_events(
        self,
        benchmark: Any,
        sample_events_1000: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Deserialize 1000 events from dictionaries.
        """
        event_dicts = [event.to_dict() for event in sample_events_1000]

        def deserialize_batch() -> list[SampleEvent]:
            return [SampleEvent.from_dict(d) for d in event_dicts]

        result = benchmark.pedantic(deserialize_batch, rounds=10)
        assert len(result) == 1000


class TestJSONSerializationBenchmarks:
    """Benchmarks for full JSON serialization (dict -> JSON string)."""

    def test_json_serialize_single_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Full JSON serialization of a single event.
        """
        event = create_event(SampleEvent)

        def to_json() -> str:
            return json.dumps(event.to_dict())

        result = benchmark(to_json)
        assert '"event_type":' in result

    def test_json_serialize_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Full JSON serialization of 100 events.
        """

        def to_json_batch() -> list[str]:
            return [json.dumps(event.to_dict()) for event in sample_events_100]

        result = benchmark(to_json_batch)
        assert len(result) == 100

    def test_json_deserialize_single_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Full JSON deserialization of a single event.
        """
        event = create_event(SampleEvent)
        json_str = json.dumps(event.to_dict())

        def from_json() -> SampleEvent:
            return SampleEvent.from_dict(json.loads(json_str))

        result = benchmark(from_json)
        assert result.event_id == event.event_id

    def test_json_deserialize_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Full JSON deserialization of 100 events.
        """
        json_strings = [json.dumps(event.to_dict()) for event in sample_events_100]

        def from_json_batch() -> list[SampleEvent]:
            return [SampleEvent.from_dict(json.loads(s)) for s in json_strings]

        result = benchmark(from_json_batch)
        assert len(result) == 100


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
            price=99.99,
        )

        def serialize() -> dict[str, Any]:
            return event.to_dict()

        result = benchmark(serialize)
        assert result["item_name"] == "Test Item"
        assert result["price"] == 99.99


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
