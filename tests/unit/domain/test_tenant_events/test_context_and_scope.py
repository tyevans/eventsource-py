"""Tests for TenantDomainEvent.with_tenant_context and tenant_scope."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import (
    TenantContextNotSetError,
    clear_tenant_context,
    set_current_tenant,
    tenant_scope,
    tenant_scope_sync,
)
from tests.unit.domain.test_tenant_events._fixtures import OrderCreated, ProductAdded


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
            event = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )
            assert event.tenant_id == tenant_id

        with pytest.raises(TenantContextNotSetError):
            OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )


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
