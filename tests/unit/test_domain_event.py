"""
Comprehensive unit tests for the DomainEvent base class.

Tests cover:
- Event creation with default and explicit values
- Immutability enforcement
- JSON serialization/deserialization roundtrip
- Causation and correlation tracking
- Metadata handling
- Subclassing and extension
- Validation and error handling
"""

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import Field, ValidationError

from eventsource.events.base import DomainEvent


# Test fixtures for creating sample events
class OrderCreated(DomainEvent):
    """Sample event for testing - represents an order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

    # Event-specific fields
    order_number: str = Field(..., description="Unique order number")
    customer_id: UUID = Field(..., description="Customer who placed the order")
    total_amount: float = Field(..., ge=0, description="Order total")


class OrderShipped(DomainEvent):
    """Sample event for testing - represents an order being shipped."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str = Field(..., description="Shipping tracking number")
    carrier: str = Field(..., description="Shipping carrier name")


class TestDomainEventCreation:
    """Tests for creating domain events."""

    def test_create_event_with_required_fields(self) -> None:
        """Event can be created with only required fields."""
        aggregate_id = uuid4()
        customer_id = uuid4()

        event = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
            total_amount=99.99,
        )

        assert event.aggregate_id == aggregate_id
        assert event.order_number == "ORD-001"
        assert event.customer_id == customer_id
        assert event.total_amount == 99.99
        assert event.event_type == "OrderCreated"
        assert event.aggregate_type == "Order"

    def test_event_generates_default_ids(self) -> None:
        """Event generates event_id and correlation_id by default."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert isinstance(event.event_id, UUID)
        assert isinstance(event.correlation_id, UUID)
        # Two different events should have different IDs
        event2 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-002",
            customer_id=uuid4(),
            total_amount=50.00,
        )
        assert event.event_id != event2.event_id
        assert event.correlation_id != event2.correlation_id

    def test_event_has_default_timestamp(self) -> None:
        """Event has occurred_at timestamp set to current time by default."""
        before = datetime.now(UTC)
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        after = datetime.now(UTC)

        assert before <= event.occurred_at <= after
        assert event.occurred_at.tzinfo is not None  # Timezone-aware

    def test_event_has_default_versions(self) -> None:
        """Event has default version values."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert event.event_version == 1
        assert event.aggregate_version == 1

    def test_event_optional_fields_default_to_none(self) -> None:
        """Optional fields default to None."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert event.tenant_id is None
        assert event.actor_id is None
        assert event.causation_id is None

    def test_event_with_all_optional_fields(self) -> None:
        """Event can be created with all optional fields."""
        event_id = uuid4()
        aggregate_id = uuid4()
        tenant_id = uuid4()
        correlation_id = uuid4()
        causation_id = uuid4()
        occurred_at = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)

        event = OrderCreated(
            event_id=event_id,
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            event_version=2,
            aggregate_version=5,
            tenant_id=tenant_id,
            actor_id="user:123",
            correlation_id=correlation_id,
            causation_id=causation_id,
            occurred_at=occurred_at,
            metadata={"source": "api", "version": "v2"},
        )

        assert event.event_id == event_id
        assert event.aggregate_id == aggregate_id
        assert event.event_version == 2
        assert event.aggregate_version == 5
        assert event.tenant_id == tenant_id
        assert event.actor_id == "user:123"
        assert event.correlation_id == correlation_id
        assert event.causation_id == causation_id
        assert event.occurred_at == occurred_at
        assert event.metadata == {"source": "api", "version": "v2"}

    def test_event_metadata_is_dict_by_default(self) -> None:
        """Event metadata defaults to empty dict."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert event.metadata == {}
        assert isinstance(event.metadata, dict)


class TestDomainEventImmutability:
    """Tests for event immutability."""

    def test_event_is_frozen(self) -> None:
        """Event fields cannot be modified after creation."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        with pytest.raises(ValidationError):
            event.order_number = "ORD-002"  # type: ignore[misc]

    def test_event_aggregate_id_is_frozen(self) -> None:
        """Event aggregate_id cannot be modified."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        with pytest.raises(ValidationError):
            event.aggregate_id = uuid4()  # type: ignore[misc]

    def test_event_metadata_attribute_is_frozen(self) -> None:
        """Event metadata attribute cannot be reassigned."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            metadata={"key": "value"},
        )

        # Pydantic frozen models prevent attribute reassignment
        with pytest.raises(ValidationError):
            event.metadata = {"new": "dict"}  # type: ignore[misc]


class TestDomainEventSerialization:
    """Tests for event serialization and deserialization."""

    def test_to_dict_returns_json_serializable(self) -> None:
        """to_dict() returns a JSON-serializable dictionary."""
        aggregate_id = uuid4()
        customer_id = uuid4()

        event = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
            total_amount=99.99,
        )

        data = event.to_dict()

        # Should be serializable to JSON
        json_str = json.dumps(data)
        assert isinstance(json_str, str)

        # UUIDs should be strings
        assert isinstance(data["event_id"], str)
        assert isinstance(data["aggregate_id"], str)
        assert isinstance(data["correlation_id"], str)
        assert data["aggregate_id"] == str(aggregate_id)

        # Datetime should be ISO string
        assert isinstance(data["occurred_at"], str)

    def test_from_dict_creates_event(self) -> None:
        """from_dict() creates event from dictionary."""
        aggregate_id = uuid4()
        customer_id = uuid4()
        event_id = uuid4()
        correlation_id = uuid4()
        occurred_at = datetime.now(UTC)

        data = {
            "event_id": str(event_id),
            "event_type": "OrderCreated",
            "event_version": 1,
            "occurred_at": occurred_at.isoformat(),
            "aggregate_id": str(aggregate_id),
            "aggregate_type": "Order",
            "aggregate_version": 1,
            "tenant_id": None,
            "actor_id": "user:123",
            "correlation_id": str(correlation_id),
            "causation_id": None,
            "metadata": {},
            "order_number": "ORD-001",
            "customer_id": str(customer_id),
            "total_amount": 99.99,
        }

        event = OrderCreated.from_dict(data)

        assert event.event_id == event_id
        assert event.aggregate_id == aggregate_id
        assert event.order_number == "ORD-001"
        assert event.customer_id == customer_id
        assert event.actor_id == "user:123"

    def test_serialization_roundtrip(self) -> None:
        """Event survives serialization roundtrip."""
        original = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=uuid4(),
            actor_id="user:123",
            metadata={"trace_id": "abc123"},
        )

        # Serialize to dict
        data = original.to_dict()

        # Deserialize back
        restored = OrderCreated.from_dict(data)

        # All fields should match
        assert restored.event_id == original.event_id
        assert restored.event_type == original.event_type
        assert restored.aggregate_id == original.aggregate_id
        assert restored.aggregate_type == original.aggregate_type
        assert restored.order_number == original.order_number
        assert restored.customer_id == original.customer_id
        assert restored.total_amount == original.total_amount
        assert restored.tenant_id == original.tenant_id
        assert restored.actor_id == original.actor_id
        assert restored.correlation_id == original.correlation_id
        assert restored.causation_id == original.causation_id
        assert restored.metadata == original.metadata

    def test_json_roundtrip(self) -> None:
        """Event survives full JSON roundtrip."""
        original = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        # Full JSON roundtrip
        json_str = json.dumps(original.to_dict())
        data = json.loads(json_str)
        restored = OrderCreated.from_dict(data)

        assert restored.event_id == original.event_id
        assert restored.order_number == original.order_number


class TestDomainEventCausation:
    """Tests for causation and correlation tracking."""

    def test_with_causation_sets_causation_id(self) -> None:
        """with_causation() sets causation_id from causing event."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        )

        caused = effect.with_causation(cause)

        assert caused.causation_id == cause.event_id
        assert caused.correlation_id == cause.correlation_id

    def test_with_causation_preserves_other_fields(self) -> None:
        """with_causation() preserves all other event fields."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        aggregate_id = uuid4()
        effect = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
            actor_id="system:shipping",
            metadata={"batch_id": "123"},
        )

        caused = effect.with_causation(cause)

        # Original fields preserved
        assert caused.aggregate_id == aggregate_id
        assert caused.tracking_number == "TRK-001"
        assert caused.carrier == "FedEx"
        assert caused.actor_id == "system:shipping"
        assert caused.metadata == {"batch_id": "123"}
        assert caused.event_id == effect.event_id  # Same event ID

    def test_with_causation_creates_new_instance(self) -> None:
        """with_causation() returns a new event instance."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        )

        caused = effect.with_causation(cause)

        assert caused is not effect
        assert effect.causation_id is None  # Original unchanged

    def test_is_caused_by_returns_true_for_cause(self) -> None:
        """is_caused_by() returns True for the causing event."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        ).with_causation(cause)

        assert effect.is_caused_by(cause) is True

    def test_is_caused_by_returns_false_for_non_cause(self) -> None:
        """is_caused_by() returns False for unrelated events."""
        event1 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        event2 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-002",
            customer_id=uuid4(),
            total_amount=50.00,
        )

        assert event1.is_caused_by(event2) is False
        assert event2.is_caused_by(event1) is False

    def test_is_correlated_with_returns_true_for_same_correlation(self) -> None:
        """is_correlated_with() returns True for same correlation_id."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        ).with_causation(cause)

        assert cause.is_correlated_with(effect) is True
        assert effect.is_correlated_with(cause) is True

    def test_is_correlated_with_returns_false_for_different_correlation(self) -> None:
        """is_correlated_with() returns False for different correlation_ids."""
        event1 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        event2 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-002",
            customer_id=uuid4(),
            total_amount=50.00,
        )

        assert event1.is_correlated_with(event2) is False


class TestDomainEventMetadata:
    """Tests for metadata handling."""

    def test_with_metadata_adds_metadata(self) -> None:
        """with_metadata() adds new metadata fields."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        enriched = event.with_metadata(trace_id="abc123", source="api")

        assert enriched.metadata["trace_id"] == "abc123"
        assert enriched.metadata["source"] == "api"

    def test_with_metadata_preserves_existing(self) -> None:
        """with_metadata() preserves existing metadata."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            metadata={"existing": "value"},
        )

        enriched = event.with_metadata(new_key="new_value")

        assert enriched.metadata["existing"] == "value"
        assert enriched.metadata["new_key"] == "new_value"

    def test_with_metadata_creates_new_instance(self) -> None:
        """with_metadata() returns a new event instance."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        enriched = event.with_metadata(key="value")

        assert enriched is not event
        assert event.metadata == {}  # Original unchanged

    def test_with_metadata_can_override_values(self) -> None:
        """with_metadata() can override existing metadata values."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            metadata={"key": "original"},
        )

        enriched = event.with_metadata(key="updated")

        assert enriched.metadata["key"] == "updated"


class TestDomainEventAggregateVersion:
    """Tests for aggregate version handling."""

    def test_with_aggregate_version_sets_version(self) -> None:
        """with_aggregate_version() sets the version."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        versioned = event.with_aggregate_version(5)

        assert versioned.aggregate_version == 5

    def test_with_aggregate_version_creates_new_instance(self) -> None:
        """with_aggregate_version() returns a new event instance."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        versioned = event.with_aggregate_version(5)

        assert versioned is not event
        assert event.aggregate_version == 1  # Original unchanged


class TestDomainEventStringRepresentation:
    """Tests for string representation."""

    def test_str_representation(self) -> None:
        """__str__ provides readable representation."""
        event_id = uuid4()
        aggregate_id = uuid4()

        event = OrderCreated(
            event_id=event_id,
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            aggregate_version=3,
        )

        str_repr = str(event)

        assert "OrderCreated" in str_repr
        assert str(event_id) in str_repr
        assert str(aggregate_id) in str_repr
        assert "version=3" in str_repr

    def test_repr_representation(self) -> None:
        """__repr__ provides detailed representation."""
        event_id = uuid4()
        aggregate_id = uuid4()
        tenant_id = uuid4()

        event = OrderCreated(
            event_id=event_id,
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=tenant_id,
        )

        repr_str = repr(event)

        assert "OrderCreated(" in repr_str
        assert "event_type='OrderCreated'" in repr_str
        assert "aggregate_type='Order'" in repr_str
        assert str(tenant_id) in repr_str


class TestDomainEventValidation:
    """Tests for validation behavior."""

    def test_missing_required_field_raises_error(self) -> None:
        """Creating event without required field raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            OrderCreated(
                aggregate_id=uuid4(),
                # Missing order_number
                customer_id=uuid4(),
                total_amount=99.99,
            )

        assert "order_number" in str(exc_info.value)

    def test_invalid_uuid_raises_error(self) -> None:
        """Invalid UUID string raises ValidationError."""
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id="not-a-uuid",  # type: ignore[arg-type]
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=99.99,
            )

    def test_negative_version_raises_error(self) -> None:
        """Negative version raises ValidationError."""
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=99.99,
                aggregate_version=0,  # Must be >= 1
            )

    def test_negative_event_version_raises_error(self) -> None:
        """Negative event version raises ValidationError."""
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=99.99,
                event_version=0,  # Must be >= 1
            )

    def test_negative_total_amount_raises_error(self) -> None:
        """Negative total_amount raises ValidationError (subclass field validation)."""
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=-10.0,  # Must be >= 0
            )


class TestDomainEventSubclassing:
    """Tests for subclassing behavior."""

    def test_subclass_inherits_base_fields(self) -> None:
        """Subclass inherits all base class fields."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        # Base class fields present
        assert hasattr(event, "event_id")
        assert hasattr(event, "event_type")
        assert hasattr(event, "aggregate_id")
        assert hasattr(event, "correlation_id")
        assert hasattr(event, "metadata")

    def test_subclass_can_define_own_fields(self) -> None:
        """Subclass can define additional fields."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        # Subclass-specific fields present
        assert hasattr(event, "order_number")
        assert hasattr(event, "customer_id")
        assert hasattr(event, "total_amount")

    def test_subclass_can_override_defaults(self) -> None:
        """Subclass can override event_type and aggregate_type defaults."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert event.event_type == "OrderCreated"
        assert event.aggregate_type == "Order"

    def test_subclass_serialization_includes_all_fields(self) -> None:
        """Subclass serialization includes both base and subclass fields."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        data = event.to_dict()

        # Base fields
        assert "event_id" in data
        assert "event_type" in data
        assert "aggregate_id" in data

        # Subclass fields
        assert "order_number" in data
        assert "customer_id" in data
        assert "total_amount" in data


class TestDomainEventMultiTenancy:
    """Tests for multi-tenancy support."""

    def test_tenant_id_is_optional(self) -> None:
        """tenant_id can be omitted for single-tenant use."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert event.tenant_id is None

    def test_tenant_id_can_be_set(self) -> None:
        """tenant_id can be set for multi-tenant use."""
        tenant_id = uuid4()

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=tenant_id,
        )

        assert event.tenant_id == tenant_id

    def test_tenant_id_survives_serialization(self) -> None:
        """tenant_id is properly serialized and deserialized."""
        tenant_id = uuid4()

        original = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=tenant_id,
        )

        data = original.to_dict()
        restored = OrderCreated.from_dict(data)

        assert restored.tenant_id == tenant_id


class TestDomainEventEquality:
    """Tests for event equality and hashing."""

    def test_events_with_same_data_are_equal(self) -> None:
        """Events with same data are equal."""
        event_id = uuid4()
        aggregate_id = uuid4()
        customer_id = uuid4()
        correlation_id = uuid4()
        occurred_at = datetime.now(UTC)

        event1 = OrderCreated(
            event_id=event_id,
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
            total_amount=99.99,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
        )

        event2 = OrderCreated(
            event_id=event_id,
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
            total_amount=99.99,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
        )

        assert event1 == event2

    def test_events_with_different_data_are_not_equal(self) -> None:
        """Events with different data are not equal."""
        aggregate_id = uuid4()
        customer_id = uuid4()

        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
            total_amount=99.99,
        )

        event2 = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-002",
            customer_id=customer_id,
            total_amount=50.00,
        )

        assert event1 != event2

    def test_events_with_metadata_are_not_hashable(self) -> None:
        """Events with metadata (dict) are not hashable due to mutable dict."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            metadata={"key": "value"},
        )

        # Events with metadata dict are not hashable
        with pytest.raises(TypeError):
            hash(event)
