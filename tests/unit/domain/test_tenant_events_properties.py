"""
Property-based tests for TenantDomainEvent.

Covers the invariant that a TenantDomainEvent's tenant_id survives a
to_dict()/from_dict() serialization round-trip unchanged, for arbitrary
tenant IDs and arbitrary payloads -- not just the one or two fixed UUIDs
the example-based tests in test_events.py happen to use.
"""

from __future__ import annotations

from uuid import UUID

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from eventsource import TenantDomainEvent, clear_tenant_context, tenant_scope_sync


class Widget(TenantDomainEvent):
    """Test event class with a mix of field types for round-trip testing."""

    aggregate_type: str = "Widget"
    name: str
    quantity: int
    price: float


uuid_strategy = st.uuids()


@given(
    tenant_id=uuid_strategy,
    aggregate_id=uuid_strategy,
    name=st.text(min_size=0, max_size=50),
    quantity=st.integers(min_value=-1_000_000, max_value=1_000_000),
    price=st.floats(allow_nan=False, allow_infinity=False, width=32),
)
@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_tenant_id_survives_dict_roundtrip(
    tenant_id: UUID,
    aggregate_id: UUID,
    name: str,
    quantity: int,
    price: float,
) -> None:
    event = Widget(
        aggregate_id=aggregate_id,
        tenant_id=tenant_id,
        name=name,
        quantity=quantity,
        price=price,
    )
    assert event.tenant_id == tenant_id

    data = event.to_dict()
    restored = Widget.from_dict(data)

    assert restored.tenant_id == tenant_id
    assert restored.tenant_id == event.tenant_id
    assert restored.aggregate_id == aggregate_id


@given(tenant_id=uuid_strategy, aggregate_id=uuid_strategy)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_with_tenant_context_uses_scope_tenant_for_arbitrary_ids(
    tenant_id: UUID, aggregate_id: UUID
) -> None:
    """with_tenant_context() must populate tenant_id from whatever tenant is
    active in the scope, for arbitrary tenant IDs -- not just a couple of
    fixed example UUIDs."""
    clear_tenant_context()
    with tenant_scope_sync(tenant_id):
        event = Widget.with_tenant_context(
            aggregate_id=aggregate_id,
            name="thing",
            quantity=1,
            price=1.0,
        )
    assert event.tenant_id == tenant_id

    data = event.to_dict()
    restored = Widget.from_dict(data)
    assert restored.tenant_id == tenant_id
    clear_tenant_context()


@given(scope_tenant=uuid_strategy, explicit_tenant=uuid_strategy, aggregate_id=uuid_strategy)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_explicit_tenant_id_always_overrides_scope(
    scope_tenant: UUID, explicit_tenant: UUID, aggregate_id: UUID
) -> None:
    """An explicitly-passed tenant_id must win over the scope's tenant for
    ANY pair of tenant IDs, including when they happen to be equal."""
    clear_tenant_context()
    with tenant_scope_sync(scope_tenant):
        event = Widget.with_tenant_context(
            aggregate_id=aggregate_id,
            tenant_id=explicit_tenant,
            name="thing",
            quantity=1,
            price=1.0,
        )
    assert event.tenant_id == explicit_tenant
    clear_tenant_context()
