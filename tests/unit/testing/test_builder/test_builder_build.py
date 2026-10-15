"""Tests for EventBuilder build() and __call__() methods."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.testing import EventBuilder
from tests.unit.testing.test_builder._fixtures import MinimalEvent, SampleEvent


class TestEventBuilderBuild:
    """Tests for build() and __call__() methods."""

    def test_build_creates_event_instance(self) -> None:
        customer_id = uuid4()
        event = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id)
            .with_field("amount", 100.0)
            .build()
        )
        assert isinstance(event, SampleEvent)
        assert event.customer_id == customer_id and event.amount == 100.0

    def test_build_auto_generates_aggregate_id(self) -> None:
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=0.0).build()
        assert event.aggregate_id is not None and isinstance(event.aggregate_id, UUID)

    def test_build_uses_custom_aggregate_id(self) -> None:
        agg_id = uuid4()
        event = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_fields(customer_id=uuid4(), amount=0.0)
            .build()
        )
        assert event.aggregate_id == agg_id

    def test_build_uses_default_version(self) -> None:
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=0.0).build()
        assert event.aggregate_version == 1

    def test_build_uses_custom_version(self) -> None:
        event = (
            EventBuilder(SampleEvent)
            .with_version(10)
            .with_fields(customer_id=uuid4(), amount=0.0)
            .build()
        )
        assert event.aggregate_version == 10

    def test_build_creates_minimal_event(self) -> None:
        event = EventBuilder(MinimalEvent).build()
        assert isinstance(event, MinimalEvent)
        assert event.aggregate_type == "Minimal" and event.event_type == "MinimalEvent"

    def test_build_raises_on_missing_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            EventBuilder(SampleEvent).build()

    def test_build_raises_on_invalid_field_types(self) -> None:
        with pytest.raises(ValidationError):
            EventBuilder(SampleEvent).with_fields(customer_id="not-a-uuid", amount=100.0).build()

    def test_build_raises_on_invalid_version(self) -> None:
        with pytest.raises(ValidationError):
            (
                EventBuilder(SampleEvent)
                .with_version(0)
                .with_fields(customer_id=uuid4(), amount=100.0)
                .build()
            )

    def test_build_can_be_called_multiple_times(self) -> None:
        builder = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=100.0)
        event1, event2 = builder.build(), builder.build()
        assert event1 is not event2 and event1.aggregate_id == event2.aggregate_id

    def test_build_with_overrides_does_not_mutate_builder(self) -> None:
        c1, c2 = uuid4(), uuid4()
        builder = EventBuilder(SampleEvent, customer_id=c1, amount=10.0)
        overridden = builder.build(customer_id=c2, amount=25.0, aggregate_version=3)
        subsequent = builder.build()
        assert (overridden.customer_id, overridden.amount, overridden.aggregate_version) == (c2, 25.0, 3)  # fmt: skip
        assert (subsequent.customer_id, subsequent.amount, subsequent.aggregate_version) == (c1, 10.0, 1)  # fmt: skip

    def test_call_delegates_to_build_with_and_without_overrides(self) -> None:
        c1, c2 = uuid4(), uuid4()
        builder = EventBuilder(SampleEvent, customer_id=c1, amount=10.0)
        ev1, ev2, ev3 = builder(), builder(customer_id=c2, amount=42.0), builder()
        assert (ev1.customer_id, ev1.amount) == (c1, 10.0)
        assert (ev2.customer_id, ev2.amount) == (c2, 42.0)
        assert (ev3.customer_id, ev3.amount) == (c1, 10.0)

    def test_build_applies_all_fields(self) -> None:
        agg_id, event_id, tenant_id = uuid4(), uuid4(), uuid4()
        correlation_id, causation_id, customer_id = uuid4(), uuid4(), uuid4()
        timestamp = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        metadata = {"trace_id": "test-trace"}

        event = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_event_id(event_id)
            .with_tenant_id(tenant_id)
            .with_version(7)
            .with_occurred_at(timestamp)
            .with_correlation_id(correlation_id)
            .with_causation_id(causation_id)
            .with_actor_id("test-user")
            .with_metadata(metadata)
            .with_field("customer_id", customer_id)
            .with_field("amount", 250.75)
            .build()
        )

        assert (event.aggregate_id, event.event_id, event.tenant_id) == (agg_id, event_id, tenant_id)  # fmt: skip
        assert event.aggregate_version == 7 and event.occurred_at == timestamp
        assert (event.correlation_id, event.causation_id) == (correlation_id, causation_id)
        assert event.actor_id == "test-user" and event.metadata == metadata
        assert event.customer_id == customer_id and event.amount == 250.75

    def test_init_with_initial_fields(self) -> None:
        customer_id = uuid4()
        event = EventBuilder(SampleEvent, customer_id=customer_id, amount=99.99, actor_id="user-1").build()  # fmt: skip
        assert isinstance(event, SampleEvent)
        assert event.customer_id == customer_id and event.amount == 99.99
        assert event.actor_id == "user-1" and event.aggregate_version == 1

    def test_init_initial_fields_override_defaults(self) -> None:
        agg_id, customer_id = uuid4(), uuid4()
        event = EventBuilder(SampleEvent, aggregate_id=agg_id, aggregate_version=5, customer_id=customer_id, amount=10.0).build()  # fmt: skip
        assert event.aggregate_id == agg_id and event.aggregate_version == 5


class TestEventBuilderBuildMany:
    """Tests for build_many() method."""

    def test_build_many_creates_sequential_versions_from_default(self) -> None:
        events = EventBuilder(SampleEvent, customer_id=uuid4(), amount=25.0).build_many(3)
        assert len(events) == 3 and [e.aggregate_version for e in events] == [1, 2, 3]
        assert len({e.aggregate_id for e in events}) == 1 and len({e.event_id for e in events}) == 3

    def test_build_many_uses_builder_version_as_base(self) -> None:
        events = EventBuilder(MinimalEvent).with_version(10).build_many(3)
        assert [e.aggregate_version for e in events] == [10, 11, 12]

    def test_build_many_uses_explicit_start_version(self) -> None:
        events = EventBuilder(MinimalEvent).with_version(10).build_many(4, start_version=5)
        assert [e.aggregate_version for e in events] == [5, 6, 7, 8]

    def test_build_many_zero_returns_empty_list(self) -> None:
        assert EventBuilder(MinimalEvent).build_many(0) == []

    def test_build_many_negative_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="count must be >= 0"):
            EventBuilder(MinimalEvent).build_many(-1)

    def test_build_many_avoids_event_id_collision_when_event_id_set(self) -> None:
        explicit_id = uuid4()
        builder = EventBuilder(MinimalEvent).with_event_id(explicit_id)
        assert builder.build_many(1)[0].event_id == explicit_id
        assert len({e.event_id for e in builder.build_many(3)}) == 3


__all__ = ["TestEventBuilderBuild", "TestEventBuilderBuildMany"]  # pragma: allowlist secret
