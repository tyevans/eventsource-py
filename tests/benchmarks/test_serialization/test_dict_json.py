"""Benchmarks for dict and JSON event serialization and deserialization."""

import json
from typing import Any

from eventsource.domain.event import DomainEvent
from tests.fixtures import SampleEvent, create_event


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
