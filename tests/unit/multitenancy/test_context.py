"""
Unit tests for tenant context management.

Tests cover:
- Basic context functions (get/set/clear)
- Async context manager (tenant_scope)
- Sync context manager (tenant_scope_sync)
- Context isolation between concurrent tasks
- Nested scope behavior
- Error handling
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.multitenancy import (
    TenantContextNotSetError,
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    set_current_tenant,
    tenant_scope,
    tenant_scope_sync,
)
from eventsource.multitenancy.context import tenant_context


class TestBasicContextFunctions:
    """Tests for basic context get/set/clear functions."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    def test_get_current_tenant_returns_none_by_default(self) -> None:
        """get_current_tenant returns None when no tenant is set."""
        assert get_current_tenant() is None

    def test_set_and_get_tenant(self) -> None:
        """Can set and retrieve tenant ID."""
        tenant_id = uuid4()
        set_current_tenant(tenant_id)
        assert get_current_tenant() == tenant_id

    def test_set_returns_token(self) -> None:
        """set_current_tenant returns a token for restoration."""
        tenant_id = uuid4()
        token = set_current_tenant(tenant_id)
        assert token is not None
        # Can use token to restore
        tenant_context.reset(token)
        assert get_current_tenant() is None

    def test_clear_tenant_context(self) -> None:
        """clear_tenant_context removes the current tenant."""
        tenant_id = uuid4()
        set_current_tenant(tenant_id)
        clear_tenant_context()
        assert get_current_tenant() is None

    def test_multiple_sets_overwrite(self) -> None:
        """Setting tenant multiple times overwrites previous value."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        set_current_tenant(tenant1)
        assert get_current_tenant() == tenant1

        set_current_tenant(tenant2)
        assert get_current_tenant() == tenant2


class TestGetRequiredTenant:
    """Tests for get_required_tenant function."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    def test_get_required_tenant_raises_when_not_set(self) -> None:
        """get_required_tenant raises TenantContextNotSetError when no context."""
        with pytest.raises(TenantContextNotSetError) as exc_info:
            get_required_tenant()

        # Verify error message is helpful
        assert "set_current_tenant" in str(exc_info.value)
        assert "tenant_scope" in str(exc_info.value)

    def test_get_required_tenant_returns_when_set(self) -> None:
        """get_required_tenant returns tenant when context is set."""
        tenant_id = uuid4()
        set_current_tenant(tenant_id)
        assert get_required_tenant() == tenant_id

    def test_get_required_tenant_after_clear_raises(self) -> None:
        """get_required_tenant raises after context is cleared."""
        tenant_id = uuid4()
        set_current_tenant(tenant_id)
        clear_tenant_context()

        with pytest.raises(TenantContextNotSetError):
            get_required_tenant()


class TestAsyncTenantScope:
    """Tests for async tenant_scope context manager."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    async def test_scope_sets_context(self) -> None:
        """tenant_scope sets context during scope."""
        tenant_id = uuid4()
        async with tenant_scope(tenant_id):
            assert get_current_tenant() == tenant_id

    async def test_scope_yields_tenant_id(self) -> None:
        """tenant_scope yields the tenant ID."""
        tenant_id = uuid4()
        async with tenant_scope(tenant_id) as yielded:
            assert yielded == tenant_id

    async def test_scope_clears_on_exit(self) -> None:
        """tenant_scope restores previous context after scope."""
        tenant_id = uuid4()
        async with tenant_scope(tenant_id):
            pass
        assert get_current_tenant() is None

    async def test_scope_clears_on_exception(self) -> None:
        """tenant_scope restores context even when exception occurs."""
        tenant_id = uuid4()
        with pytest.raises(ValueError, match="test error"):
            async with tenant_scope(tenant_id):
                raise ValueError("test error")
        assert get_current_tenant() is None

    async def test_nested_scopes(self) -> None:
        """Nested scopes work correctly with proper restoration."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        assert get_current_tenant() is None

        async with tenant_scope(tenant1):
            assert get_current_tenant() == tenant1

            async with tenant_scope(tenant2):
                assert get_current_tenant() == tenant2

            # tenant1 is restored after inner scope
            assert get_current_tenant() == tenant1

        # None is restored after outer scope
        assert get_current_tenant() is None

    async def test_triple_nested_scopes(self) -> None:
        """Triple nested scopes restore correctly."""
        tenant1 = uuid4()
        tenant2 = uuid4()
        tenant3 = uuid4()

        async with tenant_scope(tenant1):
            async with tenant_scope(tenant2):
                async with tenant_scope(tenant3):
                    assert get_current_tenant() == tenant3
                assert get_current_tenant() == tenant2
            assert get_current_tenant() == tenant1
        assert get_current_tenant() is None

    async def test_nested_scope_exception_in_inner(self) -> None:
        """Exception in inner scope still restores outer context."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        async with tenant_scope(tenant1):
            with pytest.raises(RuntimeError):
                async with tenant_scope(tenant2):
                    assert get_current_tenant() == tenant2
                    raise RuntimeError("inner error")
            # Outer scope is restored
            assert get_current_tenant() == tenant1


class TestSyncTenantScope:
    """Tests for sync tenant_scope_sync context manager."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    def test_sync_scope_sets_context(self) -> None:
        """tenant_scope_sync sets context during scope."""
        tenant_id = uuid4()
        with tenant_scope_sync(tenant_id):
            assert get_current_tenant() == tenant_id

    def test_sync_scope_yields_tenant_id(self) -> None:
        """tenant_scope_sync yields the tenant ID."""
        tenant_id = uuid4()
        with tenant_scope_sync(tenant_id) as yielded:
            assert yielded == tenant_id

    def test_sync_scope_clears_on_exit(self) -> None:
        """tenant_scope_sync restores previous context after scope."""
        tenant_id = uuid4()
        with tenant_scope_sync(tenant_id):
            pass
        assert get_current_tenant() is None

    def test_sync_scope_clears_on_exception(self) -> None:
        """tenant_scope_sync restores context even when exception occurs."""
        tenant_id = uuid4()
        with pytest.raises(ValueError, match="test error"), tenant_scope_sync(tenant_id):
            raise ValueError("test error")
        assert get_current_tenant() is None

    def test_sync_nested_scopes(self) -> None:
        """Sync nested scopes work correctly."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        with tenant_scope_sync(tenant1):
            assert get_current_tenant() == tenant1
            with tenant_scope_sync(tenant2):
                assert get_current_tenant() == tenant2
            assert get_current_tenant() == tenant1
        assert get_current_tenant() is None


class TestContextIsolation:
    """Tests for context isolation between concurrent async tasks."""

    def setup_method(self) -> None:
        """Clear context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear context after each test."""
        clear_tenant_context()

    async def test_concurrent_tasks_have_isolated_context(self) -> None:
        """Concurrent async tasks don't share tenant context."""
        results: dict[str, uuid4] = {}

        async def task_with_tenant(name: str, tenant_id: uuid4) -> None:
            async with tenant_scope(tenant_id):
                # Simulate some async work
                await asyncio.sleep(0.01)
                # Record what tenant this task sees
                results[name] = get_current_tenant()

        tenant1 = uuid4()
        tenant2 = uuid4()

        # Run tasks concurrently
        await asyncio.gather(
            task_with_tenant("task1", tenant1),
            task_with_tenant("task2", tenant2),
        )

        # Each task should have seen its own tenant
        assert results["task1"] == tenant1
        assert results["task2"] == tenant2

    async def test_many_concurrent_tasks_isolated(self) -> None:
        """Many concurrent tasks maintain proper isolation."""
        num_tasks = 20
        results: dict[int, uuid4] = {}
        tenant_ids = [uuid4() for _ in range(num_tasks)]

        async def task_with_tenant(task_num: int, tenant_id: uuid4) -> None:
            async with tenant_scope(tenant_id):
                await asyncio.sleep(0.001 * (task_num % 5))  # Variable delay
                results[task_num] = get_current_tenant()

        await asyncio.gather(*[task_with_tenant(i, tenant_ids[i]) for i in range(num_tasks)])

        # All tasks should have seen their own tenant
        for i in range(num_tasks):
            assert results[i] == tenant_ids[i]

    async def test_spawned_task_inherits_context(self) -> None:
        """Tasks created within a scope inherit the context."""
        tenant_id = uuid4()
        result = None

        async def inner_task() -> None:
            nonlocal result
            # This task should see the parent's context
            result = get_current_tenant()

        async with tenant_scope(tenant_id):
            # Create and await a task within the scope
            task = asyncio.create_task(inner_task())
            await task

        assert result == tenant_id

    async def test_context_not_leaked_after_task(self) -> None:
        """Context doesn't leak between sequential tasks."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        async with tenant_scope(tenant1):
            assert get_current_tenant() == tenant1

        # Context should be None now
        assert get_current_tenant() is None

        async with tenant_scope(tenant2):
            assert get_current_tenant() == tenant2

        # Context should be None again
        assert get_current_tenant() is None


class TestTenantContextNotSetError:
    """Tests for TenantContextNotSetError exception."""

    def test_error_message_is_helpful(self) -> None:
        """Error message contains helpful information."""
        error = TenantContextNotSetError()
        message = str(error)

        assert "set_current_tenant" in message
        assert "tenant_scope" in message
        assert "No tenant context set" in message

    def test_error_is_eventsource_error(self) -> None:
        """TenantContextNotSetError inherits from EventSourceError."""
        from eventsource.exceptions import EventSourceError

        error = TenantContextNotSetError()
        assert isinstance(error, EventSourceError)
