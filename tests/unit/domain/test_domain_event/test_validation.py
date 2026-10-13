"""Tests for domain event validation, subclassing, aggregate versions, type patterns, and vocabulary."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from eventsource.domain.event import DomainEvent
from tests.unit.domain.test_domain_event._fixtures import OrderCreated


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


class TestExtraForbid:
    """Tests for strict field validation."""

    def test_unknown_field_raises_validation_error(self) -> None:
        """Passing unknown fields to event constructor raises ValidationError."""

        class PaymentTaken(DomainEvent):
            aggregate_type: str = "Payment"
            amount: int

        with pytest.raises(ValidationError):
            PaymentTaken(aggregate_id=uuid4(), amount=5, amonut=7)  # typo must not be swallowed


class TestAggregateTypePattern:
    def test_invalid_category_characters_raise(self) -> None:
        class BadEvent(DomainEvent):
            aggregate_type: str = "Or:der"  # ':' is the StreamId separator

        with pytest.raises(ValidationError):
            BadEvent(aggregate_id=uuid4())

    def test_valid_category_passes(self) -> None:
        class GoodEvent(DomainEvent):
            aggregate_type: str = "Order_v2"

        assert GoodEvent(aggregate_id=uuid4()).aggregate_type == "Order_v2"

    def test_invalid_class_level_default_raises(self) -> None:
        """A class-level default (not passed as a kwarg) must still be
        validated -- this is the case validate_default=True previously
        covered; the after-validator must cover it without that config."""

        class BadDefaultEvent(DomainEvent):
            aggregate_type: str = "Bad:Name"

        with pytest.raises(ValidationError):
            BadDefaultEvent(aggregate_id=uuid4())


class TestTypesVocabulary:
    def test_identity_aliases_are_plain_uuid(self) -> None:
        from uuid import UUID

        from eventsource.domain import types

        assert types.TenantId is UUID
        assert types.CausationId is UUID

    def test_position_aliases_are_gone(self) -> None:
        from eventsource.domain import types

        for name in ("Version", "StreamPosition", "GlobalPosition"):
            assert not hasattr(types, name)
