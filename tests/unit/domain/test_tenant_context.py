"""Unit tests for tenant context management."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from uuid import uuid4

import pytest

from eventsource import (
    TenantContextNotSetError,
    TenantContextResetError,
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    reset_tenant_context,
    set_current_tenant,
    tenant_scope,
    tenant_scope_sync,
)
from eventsource.domain.exceptions import EventSourceError


@pytest.fixture(autouse=True)
def _clean_tenant_context() -> Iterator[None]:
    clear_tenant_context()
    yield
    clear_tenant_context()


class TestBasicContextFunctions:
    def test_get_current_tenant_returns_none_by_default(self) -> None:
        assert get_current_tenant() is None

    def test_set_and_get_tenant(self) -> None:
        tenant_id = uuid4()
        set_current_tenant(tenant_id)
        assert get_current_tenant() == tenant_id

    def test_set_returns_token(self) -> None:
        token = set_current_tenant(uuid4())
        assert token is not None
        reset_tenant_context(token)
        assert get_current_tenant() is None

    def test_clear_tenant_context(self) -> None:
        set_current_tenant(uuid4())
        clear_tenant_context()
        assert get_current_tenant() is None

    def test_multiple_sets_overwrite(self) -> None:
        tenant1, tenant2 = uuid4(), uuid4()
        set_current_tenant(tenant1)
        assert get_current_tenant() == tenant1
        set_current_tenant(tenant2)
        assert get_current_tenant() == tenant2


class TestGetRequiredTenant:
    def test_get_required_tenant_raises_when_not_set(self) -> None:
        with pytest.raises(TenantContextNotSetError) as exc_info:
            get_required_tenant()
        assert "set_current_tenant" in str(exc_info.value)
        assert "tenant_scope" in str(exc_info.value)

    def test_get_required_tenant_returns_when_set(self) -> None:
        tenant_id = uuid4()
        set_current_tenant(tenant_id)
        assert get_required_tenant() == tenant_id

    def test_get_required_tenant_after_clear_raises(self) -> None:
        set_current_tenant(uuid4())
        clear_tenant_context()
        with pytest.raises(TenantContextNotSetError):
            get_required_tenant()


class TestAsyncTenantScope:
    async def test_scope_sets_context(self) -> None:
        tenant_id = uuid4()
        async with tenant_scope(tenant_id):
            assert get_current_tenant() == tenant_id

    async def test_scope_yields_tenant_id(self) -> None:
        tenant_id = uuid4()
        async with tenant_scope(tenant_id) as yielded:
            assert yielded == tenant_id

    async def test_scope_clears_on_exit(self) -> None:
        async with tenant_scope(uuid4()):
            pass
        assert get_current_tenant() is None

    async def test_scope_clears_on_exception(self) -> None:
        with pytest.raises(ValueError, match="test error"):
            async with tenant_scope(uuid4()):
                raise ValueError("test error")
        assert get_current_tenant() is None

    async def test_nested_scopes(self) -> None:
        tenant1, tenant2 = uuid4(), uuid4()
        assert get_current_tenant() is None
        async with tenant_scope(tenant1):
            assert get_current_tenant() == tenant1
            async with tenant_scope(tenant2):
                assert get_current_tenant() == tenant2
            assert get_current_tenant() == tenant1
        assert get_current_tenant() is None

    async def test_triple_nested_scopes(self) -> None:
        tenant1, tenant2, tenant3 = uuid4(), uuid4(), uuid4()
        async with tenant_scope(tenant1):
            async with tenant_scope(tenant2):
                async with tenant_scope(tenant3):
                    assert get_current_tenant() == tenant3
                assert get_current_tenant() == tenant2
            assert get_current_tenant() == tenant1
        assert get_current_tenant() is None

    async def test_nested_scope_exception_in_inner(self) -> None:
        tenant1, tenant2 = uuid4(), uuid4()
        async with tenant_scope(tenant1):
            with pytest.raises(RuntimeError):
                async with tenant_scope(tenant2):
                    assert get_current_tenant() == tenant2
                    raise RuntimeError("inner error")
            assert get_current_tenant() == tenant1


class TestSyncTenantScope:
    def test_sync_scope_sets_context(self) -> None:
        tenant_id = uuid4()
        with tenant_scope_sync(tenant_id):
            assert get_current_tenant() == tenant_id

    def test_sync_scope_yields_tenant_id(self) -> None:
        tenant_id = uuid4()
        with tenant_scope_sync(tenant_id) as yielded:
            assert yielded == tenant_id

    def test_sync_scope_clears_on_exit(self) -> None:
        with tenant_scope_sync(uuid4()):
            pass
        assert get_current_tenant() is None

    def test_sync_scope_clears_on_exception(self) -> None:
        with pytest.raises(ValueError, match="test error"), tenant_scope_sync(uuid4()):
            raise ValueError("test error")
        assert get_current_tenant() is None

    def test_sync_nested_scopes(self) -> None:
        tenant1, tenant2 = uuid4(), uuid4()
        with tenant_scope_sync(tenant1):
            assert get_current_tenant() == tenant1
            with tenant_scope_sync(tenant2):
                assert get_current_tenant() == tenant2
            assert get_current_tenant() == tenant1
        assert get_current_tenant() is None


class TestContextIsolation:
    async def test_concurrent_tasks_have_isolated_context(self) -> None:
        results: dict[str, uuid4] = {}

        async def task_with_tenant(name: str, tenant_id: uuid4) -> None:
            async with tenant_scope(tenant_id):
                await asyncio.sleep(0.01)
                results[name] = get_current_tenant()

        tenant1, tenant2 = uuid4(), uuid4()
        await asyncio.gather(task_with_tenant("task1", tenant1), task_with_tenant("task2", tenant2))
        assert results["task1"] == tenant1
        assert results["task2"] == tenant2

    async def test_many_concurrent_tasks_isolated(self) -> None:
        num_tasks = 20
        results: dict[int, uuid4] = {}
        tenant_ids = [uuid4() for _ in range(num_tasks)]

        async def task_with_tenant(task_num: int, tenant_id: uuid4) -> None:
            async with tenant_scope(tenant_id):
                await asyncio.sleep(0.001 * (task_num % 5))
                results[task_num] = get_current_tenant()

        await asyncio.gather(*[task_with_tenant(i, tenant_ids[i]) for i in range(num_tasks)])
        for i in range(num_tasks):
            assert results[i] == tenant_ids[i]

    async def test_spawned_task_inherits_context(self) -> None:
        tenant_id = uuid4()
        result = None

        async def inner_task() -> None:
            nonlocal result
            result = get_current_tenant()

        async with tenant_scope(tenant_id):
            await asyncio.create_task(inner_task())
        assert result == tenant_id

    async def test_context_not_leaked_after_task(self) -> None:
        tenant1, tenant2 = uuid4(), uuid4()
        async with tenant_scope(tenant1):
            assert get_current_tenant() == tenant1
        assert get_current_tenant() is None
        async with tenant_scope(tenant2):
            assert get_current_tenant() == tenant2
        assert get_current_tenant() is None


class TestTenantContextNotSetError:
    def test_error_message_is_helpful(self) -> None:
        message = str(TenantContextNotSetError())
        assert "set_current_tenant" in message
        assert "tenant_scope" in message
        assert "No tenant context set" in message

    def test_error_is_eventsource_error(self) -> None:
        assert isinstance(TenantContextNotSetError(), EventSourceError)


class TestClearIsFinal:
    def test_clear_invalidates_outstanding_tokens(self) -> None:
        token = set_current_tenant(uuid4())
        clear_tenant_context()
        assert get_current_tenant() is None
        with pytest.raises(TenantContextResetError):
            reset_tenant_context(token)
        assert get_current_tenant() is None

    def test_clear_inside_scope_raises_on_scope_exit(self) -> None:
        with pytest.raises(TenantContextResetError), tenant_scope_sync(uuid4()):
            clear_tenant_context()
        assert get_current_tenant() is None
