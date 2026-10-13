"""Tests for tenant scope boundaries, clearing interactions, and required tenant checks."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import (
    TenantContextNotSetError,
    TenantContextResetError,
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    tenant_scope,
    tenant_scope_sync,
)

pytestmark = pytest.mark.timeout(60)


@pytest.fixture(autouse=True)
def _clear_context() -> None:
    """Ensure a clean slate before and after every test in this module."""
    clear_tenant_context()
    yield
    clear_tenant_context()


def test_clear_inside_scope_makes_scope_exit_raise() -> None:
    """clear_tenant_context() inside an active scope invalidates the scope's
    restore token. On exit, tenant_scope_sync will raise TenantContextResetError
    because the token it tries to reset was invalidated by clear_tenant_context().
    Calling clear inside an active scope is a discipline violation; the scope
    must fail loudly to detect the violation."""
    outer = uuid4()
    with pytest.raises(TenantContextResetError), tenant_scope_sync(outer):
        assert get_current_tenant() == outer
        clear_tenant_context()
        assert get_current_tenant() is None
    assert get_current_tenant() is None


def test_clear_inside_nested_scope_makes_inner_scope_exit_raise() -> None:
    """clear_tenant_context() inside an inner nested scope invalidates that
    scope's token. When the inner scope exits, it will raise TenantContextResetError
    because its token was invalidated. Calling clear inside an active scope is
    a discipline violation."""
    outer = uuid4()
    inner = uuid4()
    with pytest.raises(TenantContextResetError), tenant_scope_sync(outer), tenant_scope_sync(inner):
        assert get_current_tenant() == inner
        clear_tenant_context()
        assert get_current_tenant() is None
    assert get_current_tenant() is None


async def test_clear_inside_async_scope_makes_scope_exit_raise() -> None:
    """clear_tenant_context() inside an active async scope invalidates the
    scope's restore token. On exit, tenant_scope will raise TenantContextResetError.
    Calling clear inside an active scope is a discipline violation."""
    outer = uuid4()
    with pytest.raises(TenantContextResetError):
        async with tenant_scope(outer):
            assert get_current_tenant() == outer
            clear_tenant_context()
            assert get_current_tenant() is None
    assert get_current_tenant() is None


def test_get_required_tenant_raises_after_scope_exit() -> None:
    tenant_id = uuid4()
    with tenant_scope_sync(tenant_id):
        assert get_required_tenant() == tenant_id
    with pytest.raises(TenantContextNotSetError):
        get_required_tenant()
