"""Tests for EventBuilder method chaining, repr, and type safety."""

from datetime import UTC, datetime
from uuid import uuid4

from eventsource.testing import EventBuilder
from tests.unit.testing.test_builder._fixtures import SampleEvent


class TestEventBuilderMethodChaining:
    """Tests for fluent method chaining."""

    def test_all_methods_chainable(self) -> None:
        """All with_* methods can be chained in sequence."""
        agg_id = uuid4()
        event_id = uuid4()
        tenant_id = uuid4()
        correlation_id = uuid4()
        causation_id = uuid4()
        customer_id = uuid4()
        timestamp = datetime.now(UTC)

        builder = EventBuilder(SampleEvent)
        result = (
            builder.with_aggregate_id(agg_id)
            .with_event_id(event_id)
            .with_tenant_id(tenant_id)
            .with_version(5)
            .with_occurred_at(timestamp)
            .with_correlation_id(correlation_id)
            .with_causation_id(causation_id)
            .with_actor_id("user-123")
            .with_metadata({"source": "test"})
            .with_field("customer_id", customer_id)
            .with_field("amount", 100.0)
        )

        assert result is builder
        assert builder._fields["aggregate_id"] == agg_id
        assert builder._fields["event_id"] == event_id
        assert builder._fields["tenant_id"] == tenant_id
        assert builder._fields["aggregate_version"] == 5
        assert builder._fields["occurred_at"] == timestamp
        assert builder._fields["correlation_id"] == correlation_id
        assert builder._fields["causation_id"] == causation_id
        assert builder._fields["actor_id"] == "user-123"
        assert builder._fields["metadata"] == {"source": "test"}
        assert builder._fields["customer_id"] == customer_id
        assert builder._fields["amount"] == 100.0

    def test_with_fields_chainable_with_other_methods(self) -> None:
        """with_fields can be chained with other methods."""
        agg_id = uuid4()
        customer_id = uuid4()

        builder = EventBuilder(SampleEvent)
        result = (
            builder.with_aggregate_id(agg_id)
            .with_fields(customer_id=customer_id, amount=50.0)
            .with_version(3)
        )

        assert result is builder
        assert builder._fields["aggregate_id"] == agg_id
        assert builder._fields["customer_id"] == customer_id
        assert builder._fields["amount"] == 50.0
        assert builder._fields["aggregate_version"] == 3


class TestEventBuilderRepr:
    """Tests for __repr__ method."""

    def test_repr_shows_event_class_name(self) -> None:
        """__repr__ shows the event class name."""
        builder = EventBuilder(SampleEvent)
        repr_str = repr(builder)
        assert "SampleEvent" in repr_str

    def test_repr_shows_field_names(self) -> None:
        """__repr__ shows field names that have been set."""
        builder = EventBuilder(SampleEvent)
        builder.with_field("customer_id", uuid4())
        repr_str = repr(builder)
        assert "customer_id" in repr_str

    def test_repr_shows_default_fields(self) -> None:
        """__repr__ shows default auto-generated fields."""
        builder = EventBuilder(SampleEvent)
        repr_str = repr(builder)
        assert "aggregate_id" in repr_str
        assert "aggregate_version" in repr_str

    def test_repr_format(self) -> None:
        """__repr__ follows expected format."""
        builder = EventBuilder(SampleEvent)
        repr_str = repr(builder)
        assert repr_str.startswith("EventBuilder(")
        assert "fields=" in repr_str


class TestEventBuilderTypeSafety:
    """Tests for type safety and generic behavior."""

    def test_build_returns_correct_type(self) -> None:
        """build() returns the correct event type, not base DomainEvent."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=100.0).build()
        # Type checker should infer this as SampleEvent, not just DomainEvent
        assert type(event) is SampleEvent
        # Can access SampleEvent-specific fields without type errors
        _ = event.customer_id
        _ = event.amount

    def test_generic_preserves_type_through_chaining(self) -> None:
        """Generic type is preserved through method chaining."""
        builder: EventBuilder[SampleEvent] = EventBuilder(SampleEvent)
        # All these should preserve the type
        chained = (
            builder.with_aggregate_id(uuid4()).with_version(1).with_field("customer_id", uuid4())
        )
        # Type should still be EventBuilder[SampleEvent]
        event = chained.with_field("amount", 100.0).build()
        assert isinstance(event, SampleEvent)


__all__ = [  # pragma: allowlist secret
    "TestEventBuilderMethodChaining",
    "TestEventBuilderRepr",
    "TestEventBuilderTypeSafety",
]
