"""Tests for domain event validation, subclassing, aggregate versions, type patterns, and vocabulary."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from eventsource.domain.event import DomainEvent
from tests.unit.domain.test_domain_event._fixtures import OrderCreated


class TestDomainEventAggregateVersion:
    """Tests for aggregate version handling."""

    def test_with_aggregate_version_sets_version(self) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        assert event.with_aggregate_version(5).aggregate_version == 5

    def test_with_aggregate_version_creates_new_instance(self) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        versioned = event.with_aggregate_version(5)
        assert versioned is not event
        assert event.aggregate_version == 1


class TestDomainEventValidation:
    """Tests for validation behavior."""

    def test_missing_required_field_raises_error(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            OrderCreated(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
                total_amount=99.99,
            )
        assert "order_number" in str(exc_info.value)

    def test_invalid_uuid_raises_error(self) -> None:
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id="not-a-uuid",  # type: ignore[arg-type]
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=99.99,
            )

    def test_negative_version_raises_error(self) -> None:
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=99.99,
                aggregate_version=0,
            )

    def test_negative_event_version_raises_error(self) -> None:
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=99.99,
                event_version=0,
            )

    def test_negative_total_amount_raises_error(self) -> None:
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
                total_amount=-10.0,
            )


class TestDomainEventSubclassing:
    """Tests for subclassing behavior."""

    def test_subclass_inherits_base_fields(self) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        for field in ("event_id", "event_type", "aggregate_id", "correlation_id", "metadata"):
            assert hasattr(event, field)

    def test_subclass_can_define_own_fields(self) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        for field in ("order_number", "customer_id", "total_amount"):
            assert hasattr(event, field)

    def test_subclass_can_override_defaults(self) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        assert event.event_type == "OrderCreated"
        assert event.aggregate_type == "Order"

    def test_subclass_serialization_includes_all_fields(self) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )
        data = event.to_dict()
        for key in (
            "event_id",
            "event_type",
            "aggregate_id",
            "order_number",
            "customer_id",
            "total_amount",
        ):
            assert key in data


class TestExtraForbid:
    """Tests for strict field validation."""

    def test_unknown_field_raises_validation_error(self) -> None:
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
        """A class-level default must still be validated by the after-validator."""

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
