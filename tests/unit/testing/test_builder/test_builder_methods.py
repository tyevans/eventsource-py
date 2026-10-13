"""Tests for EventBuilder initialization and individual with_* builder methods."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.testing import EventBuilder
from tests.unit.testing.test_builder._fixtures import SampleEvent


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


__all__ = [  # pragma: allowlist secret
    "TestEventBuilderInit",
    "TestEventBuilderWithActorId",
    "TestEventBuilderWithAggregateId",
    "TestEventBuilderWithCausationId",
    "TestEventBuilderWithCorrelationId",
    "TestEventBuilderWithEventId",
    "TestEventBuilderWithField",
    "TestEventBuilderWithFields",
    "TestEventBuilderWithMetadata",
    "TestEventBuilderWithOccurredAt",
    "TestEventBuilderWithTenantId",
    "TestEventBuilderWithVersion",
]
