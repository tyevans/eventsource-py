"""Contract for an explicit tenant_id passed to TenantDomainEvent.with_tenant_context.

An explicit tenant_id is accepted when no tenant scope is active or when it
equals the active scope. A different tenant raises TenantMismatchError, the same
rule the aggregate applies in create_event().
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import (
    TenantMismatchError,
    clear_tenant_context,
    set_current_tenant,
    tenant_scope,
)
from tests.unit.domain.test_tenant_events._fixtures import OrderCreated


class TestExplicitTenantContract:
    """An explicit tenant_id must agree with the active scope."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    async def test_explicit_tenant_different_from_scope_raises(self) -> None:
        """An explicit tenant_id that differs from the active scope is rejected."""
        context_tenant = uuid4()
        explicit_tenant = uuid4()

        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                OrderCreated.with_tenant_context(
                    aggregate_id=uuid4(),
                    customer_id=uuid4(),
                    tenant_id=explicit_tenant,
                )

        assert exc_info.value.expected == context_tenant
        assert exc_info.value.actual == explicit_tenant

    def test_explicit_tenant_different_from_set_context_raises(self) -> None:
        """The rule holds for a tenant set with set_current_tenant, not only scopes."""
        set_current_tenant(uuid4())

        with pytest.raises(TenantMismatchError):
            OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
                tenant_id=uuid4(),
            )

    async def test_explicit_tenant_equal_to_scope_is_allowed(self) -> None:
        """An explicit tenant_id equal to the active scope is accepted."""
        tenant_id = uuid4()

        async with tenant_scope(tenant_id):
            event = OrderCreated.with_tenant_context(
                aggregate_id=uuid4(),
                customer_id=uuid4(),
                tenant_id=tenant_id,
            )

        assert event.tenant_id == tenant_id

    def test_explicit_tenant_without_scope_is_allowed(self) -> None:
        """With no active scope, an explicit tenant_id is used as given."""
        explicit_tenant = uuid4()

        event = OrderCreated.with_tenant_context(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            tenant_id=explicit_tenant,
        )

        assert event.tenant_id == explicit_tenant
