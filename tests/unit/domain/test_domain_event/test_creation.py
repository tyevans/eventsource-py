"""Tests for domain event creation, immutability, string representation, and equality."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from tests.unit.domain.test_domain_event._fixtures import OrderCreated


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
