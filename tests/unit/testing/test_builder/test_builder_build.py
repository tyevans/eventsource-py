"""Tests for EventBuilder build() method."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.testing import EventBuilder
from tests.unit.testing.test_builder._fixtures import MinimalEvent, SampleEvent


class TestEventBuilderBuild:
    """Tests for build() method."""

    def test_build_creates_event_instance(self) -> None:
        """build() creates valid event instance."""
        customer_id = uuid4()
        event = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id)
            .with_field("amount", 100.0)
            .build()
        )

        assert isinstance(event, SampleEvent)
        assert event.customer_id == customer_id
        assert event.amount == 100.0

    def test_build_auto_generates_aggregate_id(self) -> None:
        """build() uses auto-generated aggregate_id if not set."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=0.0).build()
        assert event.aggregate_id is not None
        assert isinstance(event.aggregate_id, UUID)

    def test_build_uses_custom_aggregate_id(self) -> None:
        """build() uses custom aggregate_id when set."""
        agg_id = uuid4()
        event = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_fields(customer_id=uuid4(), amount=0.0)
            .build()
        )
        assert event.aggregate_id == agg_id

    def test_build_uses_default_version(self) -> None:
        """build() defaults aggregate_version to 1."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=0.0).build()
        assert event.aggregate_version == 1

    def test_build_uses_custom_version(self) -> None:
        """build() uses custom version when set."""
        event = (
            EventBuilder(SampleEvent)
            .with_version(10)
            .with_fields(customer_id=uuid4(), amount=0.0)
            .build()
        )
        assert event.aggregate_version == 10

    def test_build_creates_minimal_event(self) -> None:
        """build() works with minimal event that has no custom fields."""
        event = EventBuilder(MinimalEvent).build()
        assert isinstance(event, MinimalEvent)
        assert event.aggregate_type == "Minimal"
        assert event.event_type == "MinimalEvent"

    def test_build_raises_on_missing_required_fields(self) -> None:
        """build() raises ValidationError for missing required fields."""
        with pytest.raises(ValidationError):
            EventBuilder(SampleEvent).build()  # Missing customer_id and amount

    def test_build_raises_on_invalid_field_types(self) -> None:
        """build() validates field types via Pydantic."""
        with pytest.raises(ValidationError):
            (EventBuilder(SampleEvent).with_fields(customer_id="not-a-uuid", amount=100.0).build())

    def test_build_raises_on_invalid_version(self) -> None:
        """build() validates version constraints."""
        with pytest.raises(ValidationError):
            (
                EventBuilder(SampleEvent)
                .with_version(0)  # Version must be >= 1
                .with_fields(customer_id=uuid4(), amount=100.0)
                .build()
            )

    def test_build_can_be_called_multiple_times(self) -> None:
        """build() can be called multiple times creating different instances."""
        builder = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=100.0)
        event1 = builder.build()
        event2 = builder.build()

        assert event1 is not event2
        assert event1.aggregate_id == event2.aggregate_id  # Same from builder

    def test_build_applies_all_fields(self) -> None:
        """build() applies all fields set via various methods."""
        agg_id = uuid4()
        event_id = uuid4()
        tenant_id = uuid4()
        correlation_id = uuid4()
        causation_id = uuid4()
        customer_id = uuid4()
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

        assert event.aggregate_id == agg_id
        assert event.event_id == event_id
        assert event.tenant_id == tenant_id
        assert event.aggregate_version == 7
        assert event.occurred_at == timestamp
        assert event.correlation_id == correlation_id
        assert event.causation_id == causation_id
        assert event.actor_id == "test-user"
        assert event.metadata == metadata
        assert event.customer_id == customer_id
        assert event.amount == 250.75

    def test_init_with_initial_fields(self) -> None:
        """EventBuilder accepts initial fields in constructor."""
        customer_id = uuid4()
        event = EventBuilder(
            SampleEvent, customer_id=customer_id, amount=99.99, actor_id="user-1"
        ).build()

        assert isinstance(event, SampleEvent)
        assert event.customer_id == customer_id
        assert event.amount == 99.99
        assert event.actor_id == "user-1"
        assert event.aggregate_version == 1

    def test_init_initial_fields_override_defaults(self) -> None:
        """Initial fields in constructor can override default aggregate_id and version."""
        agg_id = uuid4()
        customer_id = uuid4()
        event = EventBuilder(
            SampleEvent,
            aggregate_id=agg_id,
            aggregate_version=5,
            customer_id=customer_id,
            amount=10.0,
        ).build()

        assert event.aggregate_id == agg_id
        assert event.aggregate_version == 5


class TestEventBuilderBuildMany:
    """Tests for build_many() method."""

    def test_build_many_creates_sequential_versions_from_default(self) -> None:
        """build_many() builds events with sequential versions starting from default 1."""
        customer_id = uuid4()
        events = EventBuilder(SampleEvent, customer_id=customer_id, amount=25.0).build_many(3)

        assert len(events) == 3
        assert [e.aggregate_version for e in events] == [1, 2, 3]
        assert len({e.aggregate_id for e in events}) == 1
        assert len({e.event_id for e in events}) == 3

    def test_build_many_uses_builder_version_as_base(self) -> None:
        """build_many() starts from builder's configured aggregate_version when start_version is None."""
        events = EventBuilder(MinimalEvent).with_version(10).build_many(3)

        assert [e.aggregate_version for e in events] == [10, 11, 12]

    def test_build_many_uses_explicit_start_version(self) -> None:
        """build_many() uses explicit start_version over builder's version."""
        events = EventBuilder(MinimalEvent).with_version(10).build_many(4, start_version=5)

        assert [e.aggregate_version for e in events] == [5, 6, 7, 8]

    def test_build_many_zero_returns_empty_list(self) -> None:
        """build_many(0) returns an empty list."""
        events = EventBuilder(MinimalEvent).build_many(0)
        assert events == []

    def test_build_many_negative_raises_value_error(self) -> None:
        """build_many() with negative count raises ValueError."""
        with pytest.raises(ValueError, match="count must be >= 0"):
            EventBuilder(MinimalEvent).build_many(-1)

    def test_build_many_avoids_event_id_collision_when_event_id_set(self) -> None:
        """build_many(count > 1) generates distinct event_ids even if with_event_id was called."""
        explicit_id = uuid4()
        builder = EventBuilder(MinimalEvent).with_event_id(explicit_id)

        single = builder.build_many(1)
        assert single[0].event_id == explicit_id

        multiple = builder.build_many(3)
        assert len({e.event_id for e in multiple}) == 3


__all__ = [  # pragma: allowlist secret
    "TestEventBuilderBuild",
    "TestEventBuilderBuildMany",
]
