"""Tests for TenantDomainEvent validation, inheritance, and TenantMismatchError."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from eventsource import TenantMismatchError
from eventsource.domain.exceptions import EventSourceError
from tests.unit.domain.test_tenant_events._fixtures import OrderCreated, ProductAdded


class TestTenantDomainEventValidation:
    """Tests for TenantDomainEvent field validation."""

    def test_tenant_id_is_required(self) -> None:
        """tenant_id field is required and validation fails without it."""
        with pytest.raises(ValidationError) as exc_info:
            OrderCreated(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
                # Missing tenant_id
            )

        errors = exc_info.value.errors()
        assert any("tenant_id" in str(error.get("loc", ())) for error in errors)

    def test_tenant_id_cannot_be_none(self) -> None:
        """tenant_id cannot be explicitly set to None."""
        with pytest.raises(ValidationError):
            OrderCreated(
                aggregate_id=uuid4(),
                tenant_id=None,  # type: ignore[arg-type]
                customer_id=uuid4(),
            )

    def test_explicit_tenant_id_works(self) -> None:
        """Can create event with explicit tenant_id."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        customer_id = uuid4()

        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            customer_id=customer_id,
            total_amount=99.99,
        )

        assert event.tenant_id == tenant_id
        assert event.aggregate_id == aggregate_id
        assert event.customer_id == customer_id
        assert event.total_amount == 99.99


class TestTenantDomainEventInheritance:
    """Tests that TenantDomainEvent inherits DomainEvent functionality."""

    def test_event_type_auto_derived(self) -> None:
        """event_type is automatically derived from class name."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            customer_id=uuid4(),
        )
        assert event.event_type == "OrderCreated"

    def test_aggregate_type_required(self) -> None:
        """aggregate_type is still required."""
        event = ProductAdded(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            product_name="Widget",
            price=9.99,
        )
        assert event.aggregate_type == "Catalog"

    def test_event_is_immutable(self) -> None:
        """Events are immutable (frozen)."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            customer_id=uuid4(),
        )

        with pytest.raises(ValidationError):
            event.tenant_id = uuid4()  # type: ignore[misc]

    def test_with_causation_works(self) -> None:
        """with_causation method from DomainEvent works."""
        event1 = OrderCreated(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            customer_id=uuid4(),
        )
        event2 = OrderCreated(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            customer_id=uuid4(),
        )

        caused = event2.with_causation(event1)
        assert caused.causation_id == event1.event_id
        assert caused.correlation_id == event1.correlation_id

    def test_to_dict_includes_tenant_id(self) -> None:
        """to_dict includes tenant_id."""
        tenant_id = uuid4()
        event = OrderCreated(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
            customer_id=uuid4(),
        )

        data = event.to_dict()
        assert data["tenant_id"] == str(tenant_id)

    def test_from_dict_with_tenant_id(self) -> None:
        """from_dict correctly deserializes tenant_id."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        customer_id = uuid4()

        data = {
            "aggregate_id": str(aggregate_id),
            "tenant_id": str(tenant_id),
            "aggregate_type": "Order",
            "customer_id": str(customer_id),
        }

        event = OrderCreated.from_dict(data)
        assert event.tenant_id == tenant_id


class TestTenantMismatchError:
    """Tests for TenantMismatchError exception."""

    def test_error_attributes(self) -> None:
        """TenantMismatchError has expected attributes."""
        expected = uuid4()
        actual = uuid4()
        event_ids = [uuid4(), uuid4()]

        error = TenantMismatchError(
            expected=expected,
            actual=actual,
            event_ids=event_ids,
        )

        assert error.expected == expected
        assert error.actual == actual
        assert error.event_ids == event_ids

    def test_error_message_format(self) -> None:
        """TenantMismatchError message is formatted correctly."""
        expected = uuid4()
        actual = uuid4()
        event_ids = [uuid4()]

        error = TenantMismatchError(
            expected=expected,
            actual=actual,
            event_ids=event_ids,
        )

        message = str(error)
        assert str(expected) in message
        assert str(actual) in message
        assert str(event_ids[0]) in message
        assert "Tenant mismatch" in message

    def test_error_message_truncates_many_events(self) -> None:
        """TenantMismatchError truncates list when more than 5 events."""
        expected = uuid4()
        actual = uuid4()
        event_ids = [uuid4() for _ in range(10)]

        error = TenantMismatchError(
            expected=expected,
            actual=actual,
            event_ids=event_ids,
        )

        message = str(error)
        assert "... and 5 more" in message

    def test_error_is_eventsource_error(self) -> None:
        """TenantMismatchError inherits from EventSourceError."""
        error = TenantMismatchError(
            expected=uuid4(),
            actual=uuid4(),
            event_ids=[uuid4()],
        )
        assert isinstance(error, EventSourceError)
