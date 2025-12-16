"""
Unit tests for TenantDomainEvent class.

Tests cover:
- tenant_id is required (validation fails without it)
- Explicit tenant_id works
- with_tenant_context() uses current context
- with_tenant_context() raises when no context
- Explicit tenant_id overrides context
- Event inherits DomainEvent functionality
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.multitenancy import (
    TenantContextNotSetError,
    TenantDomainEvent,
    clear_tenant_context,
    set_current_tenant,
    tenant_scope,
    tenant_scope_sync,
)


class OrderCreated(TenantDomainEvent):
    """Test event class for testing TenantDomainEvent."""

    aggregate_type: str = "Order"
    customer_id: UUID
    total_amount: float = 0.0


class ProductAdded(TenantDomainEvent):
    """Another test event class."""

    aggregate_type: str = "Catalog"
    product_name: str
    price: float


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

        # Check that tenant_id is mentioned in the error
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
        # ProductAdded sets aggregate_type as class default
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


class TestWithTenantContext:
    """Tests for with_tenant_context class method."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    def test_with_tenant_context_uses_context(self) -> None:
        """with_tenant_context uses current tenant context."""
        tenant_id = uuid4()
        set_current_tenant(tenant_id)

        event = OrderCreated.with_tenant_context(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
        )

        assert event.tenant_id == tenant_id

    def test_with_tenant_context_raises_without_context(self) -> None:
        """with_tenant_context raises when no context is set."""
        with pytest.raises(TenantContextNotSetError):
            OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )

    def test_explicit_tenant_id_overrides_context(self) -> None:
        """Explicit tenant_id in kwargs overrides context."""
        context_tenant = uuid4()
        explicit_tenant = uuid4()

        set_current_tenant(context_tenant)

        event = OrderCreated.with_tenant_context(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            tenant_id=explicit_tenant,
        )

        assert event.tenant_id == explicit_tenant
        assert event.tenant_id != context_tenant

    def test_all_other_fields_work(self) -> None:
        """All other event fields can be passed through kwargs."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        customer_id = uuid4()

        set_current_tenant(tenant_id)

        event = OrderCreated.with_tenant_context(
            aggregate_id=aggregate_id,
            customer_id=customer_id,
            total_amount=199.99,
            aggregate_version=5,
            actor_id="user-123",
        )

        assert event.tenant_id == tenant_id
        assert event.aggregate_id == aggregate_id
        assert event.customer_id == customer_id
        assert event.total_amount == 199.99
        assert event.aggregate_version == 5
        assert event.actor_id == "user-123"


class TestWithTenantContextAndScope:
    """Tests for with_tenant_context within tenant_scope context managers."""

    async def test_works_with_async_scope(self) -> None:
        """with_tenant_context works inside async tenant_scope."""
        tenant_id = uuid4()

        async with tenant_scope(tenant_id):
            event = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            assert event.tenant_id == tenant_id

    async def test_works_with_nested_async_scopes(self) -> None:
        """with_tenant_context uses innermost scope tenant."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        async with tenant_scope(tenant1):
            event1 = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            assert event1.tenant_id == tenant1

            async with tenant_scope(tenant2):
                event2 = OrderCreated.with_tenant_context(
                    aggregate_id=uuid4(),
                    customer_id=uuid4(),
                )
                assert event2.tenant_id == tenant2

            # Back to tenant1
            event3 = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            assert event3.tenant_id == tenant1

    def test_works_with_sync_scope(self) -> None:
        """with_tenant_context works inside sync tenant_scope_sync."""
        tenant_id = uuid4()

        with tenant_scope_sync(tenant_id):
            event = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            assert event.tenant_id == tenant_id

    async def test_raises_after_scope_exits(self) -> None:
        """with_tenant_context raises after scope exits."""
        tenant_id = uuid4()

        async with tenant_scope(tenant_id):
            # Works inside scope
            event = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            assert event.tenant_id == tenant_id

        # Raises outside scope
        with pytest.raises(TenantContextNotSetError):
            OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )


class TestTenantMismatchError:
    """Tests for TenantMismatchError exception."""

    def test_error_attributes(self) -> None:
        """TenantMismatchError has expected attributes."""
        from eventsource.multitenancy import TenantMismatchError

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
        from eventsource.multitenancy import TenantMismatchError

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
        from eventsource.multitenancy import TenantMismatchError

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
        from eventsource.exceptions import EventSourceError
        from eventsource.multitenancy import TenantMismatchError

        error = TenantMismatchError(
            expected=uuid4(),
            actual=uuid4(),
            event_ids=[uuid4()],
        )
        assert isinstance(error, EventSourceError)


class TestMultipleTenantEventTypes:
    """Tests to ensure multiple event types work correctly."""

    async def test_different_event_types_same_tenant(self) -> None:
        """Different event types can use the same tenant context."""
        tenant_id = uuid4()

        async with tenant_scope(tenant_id):
            order_event = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            product_event = ProductAdded.with_tenant_context(
                aggregate_id=uuid4(),
                product_name="Widget",
                price=9.99,
            )

            assert order_event.tenant_id == tenant_id
            assert product_event.tenant_id == tenant_id
            assert order_event.event_type == "OrderCreated"
            assert product_event.event_type == "ProductAdded"
