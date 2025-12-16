"""
Unit tests for EventBuilder.

Tests the fluent builder pattern for creating test events with minimal boilerplate.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.events.base import DomainEvent
from eventsource.testing import EventBuilder


class SampleEvent(DomainEvent):
    """Sample event for testing EventBuilder."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleEvent"
    customer_id: UUID
    amount: float


class MinimalEvent(DomainEvent):
    """Minimal event with only required base fields."""

    aggregate_type: str = "Minimal"
    event_type: str = "MinimalEvent"


class TestEventBuilderInit:
    """Tests for EventBuilder initialization."""

    def test_accepts_domain_event_subclass(self) -> None:
        """Builder accepts DomainEvent subclass."""
        builder = EventBuilder(SampleEvent)
        assert builder._event_class is SampleEvent

    def test_rejects_non_domain_event_class(self) -> None:
        """Builder rejects non-DomainEvent classes."""
        with pytest.raises(TypeError, match="must be a DomainEvent subclass"):
            EventBuilder(dict)  # type: ignore[type-var]

    def test_rejects_non_class(self) -> None:
        """Builder rejects non-class arguments."""
        with pytest.raises(TypeError, match="must be a DomainEvent subclass"):
            EventBuilder("not a class")  # type: ignore[arg-type]

    def test_rejects_none(self) -> None:
        """Builder rejects None as event class."""
        with pytest.raises(TypeError, match="must be a DomainEvent subclass"):
            EventBuilder(None)  # type: ignore[arg-type]

    def test_rejects_domain_event_base_class(self) -> None:
        """Builder accepts DomainEvent base class (it's a valid subclass of itself)."""
        # Note: DomainEvent itself is a valid DomainEvent subclass
        # but building it will fail due to required fields without defaults
        builder = EventBuilder(DomainEvent)
        assert builder._event_class is DomainEvent

    def test_auto_generates_aggregate_id(self) -> None:
        """Builder auto-generates aggregate_id on init."""
        builder = EventBuilder(SampleEvent)
        assert "aggregate_id" in builder._fields
        assert isinstance(builder._fields["aggregate_id"], UUID)

    def test_auto_generates_default_version(self) -> None:
        """Builder defaults aggregate_version to 1."""
        builder = EventBuilder(SampleEvent)
        assert builder._fields["aggregate_version"] == 1


class TestEventBuilderWithAggregateId:
    """Tests for with_aggregate_id method."""

    def test_returns_self(self) -> None:
        """with_aggregate_id returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_aggregate_id(uuid4())
        assert result is builder

    def test_sets_aggregate_id(self) -> None:
        """with_aggregate_id sets the aggregate_id field."""
        builder = EventBuilder(SampleEvent)
        agg_id = uuid4()
        builder.with_aggregate_id(agg_id)
        assert builder._fields["aggregate_id"] == agg_id

    def test_overwrites_auto_generated_id(self) -> None:
        """with_aggregate_id overwrites the auto-generated ID."""
        builder = EventBuilder(SampleEvent)
        original_id = builder._fields["aggregate_id"]
        new_id = uuid4()
        builder.with_aggregate_id(new_id)
        assert builder._fields["aggregate_id"] == new_id
        assert builder._fields["aggregate_id"] != original_id


class TestEventBuilderWithEventId:
    """Tests for with_event_id method."""

    def test_returns_self(self) -> None:
        """with_event_id returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_event_id(uuid4())
        assert result is builder

    def test_sets_event_id(self) -> None:
        """with_event_id sets the event_id field."""
        builder = EventBuilder(SampleEvent)
        event_id = uuid4()
        builder.with_event_id(event_id)
        assert builder._fields["event_id"] == event_id


class TestEventBuilderWithTenantId:
    """Tests for with_tenant_id method."""

    def test_returns_self(self) -> None:
        """with_tenant_id returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_tenant_id(uuid4())
        assert result is builder

    def test_sets_tenant_id(self) -> None:
        """with_tenant_id sets the tenant_id field."""
        builder = EventBuilder(SampleEvent)
        tenant_id = uuid4()
        builder.with_tenant_id(tenant_id)
        assert builder._fields["tenant_id"] == tenant_id


class TestEventBuilderWithVersion:
    """Tests for with_version method."""

    def test_returns_self(self) -> None:
        """with_version returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_version(5)
        assert result is builder

    def test_sets_aggregate_version(self) -> None:
        """with_version sets the aggregate_version field."""
        builder = EventBuilder(SampleEvent)
        builder.with_version(42)
        assert builder._fields["aggregate_version"] == 42

    def test_overwrites_default_version(self) -> None:
        """with_version overwrites the default version of 1."""
        builder = EventBuilder(SampleEvent)
        assert builder._fields["aggregate_version"] == 1
        builder.with_version(10)
        assert builder._fields["aggregate_version"] == 10


class TestEventBuilderWithOccurredAt:
    """Tests for with_occurred_at method."""

    def test_returns_self(self) -> None:
        """with_occurred_at returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_occurred_at(datetime.now(UTC))
        assert result is builder

    def test_sets_occurred_at(self) -> None:
        """with_occurred_at sets the occurred_at field."""
        builder = EventBuilder(SampleEvent)
        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        builder.with_occurred_at(timestamp)
        assert builder._fields["occurred_at"] == timestamp


class TestEventBuilderWithCorrelationId:
    """Tests for with_correlation_id method."""

    def test_returns_self(self) -> None:
        """with_correlation_id returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_correlation_id(uuid4())
        assert result is builder

    def test_sets_correlation_id(self) -> None:
        """with_correlation_id sets the correlation_id field."""
        builder = EventBuilder(SampleEvent)
        correlation_id = uuid4()
        builder.with_correlation_id(correlation_id)
        assert builder._fields["correlation_id"] == correlation_id


class TestEventBuilderWithCausationId:
    """Tests for with_causation_id method."""

    def test_returns_self(self) -> None:
        """with_causation_id returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_causation_id(uuid4())
        assert result is builder

    def test_sets_causation_id(self) -> None:
        """with_causation_id sets the causation_id field."""
        builder = EventBuilder(SampleEvent)
        causation_id = uuid4()
        builder.with_causation_id(causation_id)
        assert builder._fields["causation_id"] == causation_id


class TestEventBuilderWithActorId:
    """Tests for with_actor_id method."""

    def test_returns_self(self) -> None:
        """with_actor_id returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_actor_id("user-123")
        assert result is builder

    def test_sets_actor_id(self) -> None:
        """with_actor_id sets the actor_id field."""
        builder = EventBuilder(SampleEvent)
        builder.with_actor_id("system:cron")
        assert builder._fields["actor_id"] == "system:cron"


class TestEventBuilderWithMetadata:
    """Tests for with_metadata method."""

    def test_returns_self(self) -> None:
        """with_metadata returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_metadata({"key": "value"})
        assert result is builder

    def test_sets_metadata(self) -> None:
        """with_metadata sets the metadata field."""
        builder = EventBuilder(SampleEvent)
        metadata = {"trace_id": "abc123", "source": "api"}
        builder.with_metadata(metadata)
        assert builder._fields["metadata"] == metadata

    def test_overwrites_existing_metadata(self) -> None:
        """with_metadata completely replaces existing metadata."""
        builder = EventBuilder(SampleEvent)
        builder.with_metadata({"key1": "value1"})
        builder.with_metadata({"key2": "value2"})
        assert builder._fields["metadata"] == {"key2": "value2"}


class TestEventBuilderWithField:
    """Tests for with_field method."""

    def test_returns_self(self) -> None:
        """with_field returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_field("customer_id", uuid4())
        assert result is builder

    def test_sets_arbitrary_field(self) -> None:
        """with_field sets arbitrary field by name."""
        builder = EventBuilder(SampleEvent)
        customer_id = uuid4()
        builder.with_field("customer_id", customer_id)
        assert builder._fields["customer_id"] == customer_id

    def test_sets_multiple_fields_separately(self) -> None:
        """with_field can set multiple fields with separate calls."""
        builder = EventBuilder(SampleEvent)
        customer_id = uuid4()
        builder.with_field("customer_id", customer_id)
        builder.with_field("amount", 99.99)
        assert builder._fields["customer_id"] == customer_id
        assert builder._fields["amount"] == 99.99

    def test_overwrites_existing_field(self) -> None:
        """with_field overwrites existing field value."""
        builder = EventBuilder(SampleEvent)
        builder.with_field("amount", 100.0)
        builder.with_field("amount", 200.0)
        assert builder._fields["amount"] == 200.0


class TestEventBuilderWithFields:
    """Tests for with_fields method."""

    def test_returns_self(self) -> None:
        """with_fields returns builder for chaining."""
        builder = EventBuilder(SampleEvent)
        result = builder.with_fields(customer_id=uuid4(), amount=50.0)
        assert result is builder

    def test_sets_multiple_fields(self) -> None:
        """with_fields sets multiple fields at once."""
        builder = EventBuilder(SampleEvent)
        customer_id = uuid4()
        builder.with_fields(customer_id=customer_id, amount=75.50)
        assert builder._fields["customer_id"] == customer_id
        assert builder._fields["amount"] == 75.50

    def test_overwrites_existing_fields(self) -> None:
        """with_fields overwrites existing field values."""
        builder = EventBuilder(SampleEvent)
        builder.with_field("amount", 100.0)
        builder.with_fields(amount=200.0, customer_id=uuid4())
        assert builder._fields["amount"] == 200.0


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
        # But event_id will be different (auto-generated per build)
        # Actually event_id is auto-generated by DomainEvent, not builder

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


class TestEventBuilderEdgeCases:
    """Edge case tests for EventBuilder."""

    def test_empty_metadata(self) -> None:
        """Builder handles empty metadata dictionary."""
        event = (
            EventBuilder(SampleEvent)
            .with_metadata({})
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )
        assert event.metadata == {}

    def test_field_overwrite_order(self) -> None:
        """Later field settings overwrite earlier ones."""
        customer_id_1 = uuid4()
        customer_id_2 = uuid4()

        event = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id_1)
            .with_field("customer_id", customer_id_2)
            .with_field("amount", 100.0)
            .build()
        )
        assert event.customer_id == customer_id_2

    def test_with_fields_overwrites_with_field(self) -> None:
        """with_fields can overwrite fields set with with_field."""
        event = (
            EventBuilder(SampleEvent)
            .with_field("amount", 100.0)
            .with_fields(customer_id=uuid4(), amount=200.0)
            .build()
        )
        assert event.amount == 200.0

    def test_zero_amount(self) -> None:
        """Builder handles zero values correctly."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=0.0).build()
        assert event.amount == 0.0

    def test_negative_amount(self) -> None:
        """Builder handles negative values correctly (if event allows)."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=-50.0).build()
        assert event.amount == -50.0

    def test_builder_immutability_not_enforced(self) -> None:
        """Builder is mutable - same builder can create multiple events."""
        builder = EventBuilder(SampleEvent)
        builder.with_fields(customer_id=uuid4(), amount=100.0)

        event1 = builder.build()
        builder.with_field("amount", 200.0)
        event2 = builder.build()

        assert event1.amount == 100.0
        assert event2.amount == 200.0

    def test_none_values_for_optional_fields(self) -> None:
        """Builder can set None for optional fields explicitly."""
        event = (
            EventBuilder(SampleEvent)
            .with_field("tenant_id", None)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )
        assert event.tenant_id is None


class TestEventBuilderIntegrationScenarios:
    """Integration-style tests showing real-world usage patterns."""

    def test_typical_test_event_creation(self) -> None:
        """Typical test scenario: create event with minimal required fields."""
        # In a test, you usually just need the business-relevant fields
        customer_id = uuid4()
        event = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id)
            .with_field("amount", 99.99)
            .build()
        )

        assert event.customer_id == customer_id
        assert event.amount == 99.99
        # Auto-generated fields should be present
        assert event.aggregate_id is not None
        assert event.event_id is not None
        assert event.occurred_at is not None
        assert event.aggregate_version == 1

    def test_event_chain_creation(self) -> None:
        """Create related events with correlation/causation tracking."""
        correlation_id = uuid4()
        agg_id = uuid4()

        event1 = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_correlation_id(correlation_id)
            .with_version(1)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        event2 = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_correlation_id(correlation_id)
            .with_causation_id(event1.event_id)
            .with_version(2)
            .with_fields(customer_id=uuid4(), amount=50.0)
            .build()
        )

        assert event2.correlation_id == event1.correlation_id
        assert event2.causation_id == event1.event_id
        assert event2.aggregate_version > event1.aggregate_version

    def test_multi_tenant_event_creation(self) -> None:
        """Create events for different tenants."""
        tenant_1 = uuid4()
        tenant_2 = uuid4()

        event1 = (
            EventBuilder(SampleEvent)
            .with_tenant_id(tenant_1)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        event2 = (
            EventBuilder(SampleEvent)
            .with_tenant_id(tenant_2)
            .with_fields(customer_id=uuid4(), amount=200.0)
            .build()
        )

        assert event1.tenant_id == tenant_1
        assert event2.tenant_id == tenant_2

    def test_historical_event_creation(self) -> None:
        """Create events with specific timestamps for replay testing."""
        past_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)

        event = (
            EventBuilder(SampleEvent)
            .with_occurred_at(past_time)
            .with_version(1)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        assert event.occurred_at == past_time

    def test_event_with_full_audit_trail(self) -> None:
        """Create event with complete audit information."""
        event = (
            EventBuilder(SampleEvent)
            .with_actor_id("user:admin@example.com")
            .with_metadata(
                {
                    "ip_address": "192.168.1.1",
                    "user_agent": "TestClient/1.0",
                    "request_id": "req-12345",
                }
            )
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        assert event.actor_id == "user:admin@example.com"
        assert event.metadata["ip_address"] == "192.168.1.1"
        assert event.metadata["user_agent"] == "TestClient/1.0"
        assert event.metadata["request_id"] == "req-12345"
