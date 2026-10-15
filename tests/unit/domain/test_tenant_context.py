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


async def _scoped_tenant(tid: object, delay: float) -> object:
    async with tenant_scope(tid):  # type: ignore[arg-type]
        await asyncio.sleep(delay)
        return get_current_tenant()


class TestBasicContextFunctions:
    def test_get_current_tenant_returns_none_by_default(self) -> None:
        assert get_current_tenant() is None

    def test_set_and_get_tenant(self) -> None:
        set_current_tenant(tid := uuid4())
        assert get_current_tenant() == tid

    def test_set_returns_token(self) -> None:
        assert (token := set_current_tenant(uuid4())) is not None
        reset_tenant_context(token)
        assert get_current_tenant() is None

    def test_clear_tenant_context(self) -> None:
        set_current_tenant(uuid4())
        clear_tenant_context()
        assert get_current_tenant() is None

    def test_multiple_sets_overwrite(self) -> None:
        for tid in (uuid4(), uuid4()):
            set_current_tenant(tid)
            assert get_current_tenant() == tid


class TestGetRequiredTenant:
    def test_get_required_tenant_raises_when_not_set(self) -> None:
        with pytest.raises(TenantContextNotSetError) as exc_info:
            get_required_tenant()
        assert "set_current_tenant" in str(exc_info.value) and "tenant_scope" in str(exc_info.value)

    def test_get_required_tenant_returns_when_set(self) -> None:
        set_current_tenant(tid := uuid4())
        assert get_required_tenant() == tid

    def test_get_required_tenant_after_clear_raises(self) -> None:
        set_current_tenant(uuid4())
        clear_tenant_context()
        with pytest.raises(TenantContextNotSetError):
            get_required_tenant()


class TestAsyncTenantScope:
    async def test_scope_sets_context(self) -> None:
        async with tenant_scope(tid := uuid4()):
            assert get_current_tenant() == tid

    async def test_scope_yields_tenant_id(self) -> None:
        async with tenant_scope(tid := uuid4()) as yielded:
            assert yielded == tid

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
        t1, t2 = uuid4(), uuid4()
        async with tenant_scope(t1):
            assert get_current_tenant() == t1
            async with tenant_scope(t2):
                assert get_current_tenant() == t2
            assert get_current_tenant() == t1
        assert get_current_tenant() is None

    async def test_triple_nested_scopes(self) -> None:
        t1, t2, t3 = uuid4(), uuid4(), uuid4()
        async with tenant_scope(t1), tenant_scope(t2):
            async with tenant_scope(t3):
                assert get_current_tenant() == t3
            assert get_current_tenant() == t2
        assert get_current_tenant() is None

    async def test_nested_scope_exception_in_inner(self) -> None:
        t1, t2 = uuid4(), uuid4()
        async with tenant_scope(t1):
            with pytest.raises(RuntimeError):
                async with tenant_scope(t2):
                    raise RuntimeError("inner error")
            assert get_current_tenant() == t1


class TestSyncTenantScope:
    def test_sync_scope_sets_context(self) -> None:
        with tenant_scope_sync(tid := uuid4()):
            assert get_current_tenant() == tid

    def test_sync_scope_yields_tenant_id(self) -> None:
        with tenant_scope_sync(tid := uuid4()) as yielded:
            assert yielded == tid

    def test_sync_scope_clears_on_exit(self) -> None:
        with tenant_scope_sync(uuid4()):
            pass
        assert get_current_tenant() is None

    def test_sync_scope_clears_on_exception(self) -> None:
        with pytest.raises(ValueError, match="test error"), tenant_scope_sync(uuid4()):
            raise ValueError("test error")
        assert get_current_tenant() is None

    def test_sync_nested_scopes(self) -> None:
        t1, t2 = uuid4(), uuid4()
        with tenant_scope_sync(t1):
            with tenant_scope_sync(t2):
                assert get_current_tenant() == t2
            assert get_current_tenant() == t1
        assert get_current_tenant() is None


class TestContextIsolation:
    async def test_concurrent_tasks_have_isolated_context(self) -> None:
        t1, t2 = uuid4(), uuid4()
        assert await asyncio.gather(_scoped_tenant(t1, 0.01), _scoped_tenant(t2, 0.01)) == [t1, t2]

    async def test_many_concurrent_tasks_isolated(self) -> None:
        tids = [uuid4() for _ in range(20)]
        tasks = [_scoped_tenant(tids[i], 0.001 * (i % 5)) for i in range(20)]
        assert await asyncio.gather(*tasks) == tids

    async def test_spawned_task_inherits_context(self) -> None:
        async def inner() -> object:
            return get_current_tenant()

        async with tenant_scope(tid := uuid4()):
            assert await asyncio.create_task(inner()) == tid

    async def test_context_not_leaked_after_task(self) -> None:
        for tid in (uuid4(), uuid4()):
            async with tenant_scope(tid):
                assert get_current_tenant() == tid
            assert get_current_tenant() is None


class TestTenantContextNotSetError:
    def test_error_message_is_helpful(self) -> None:
        msg = str(TenantContextNotSetError())
        assert all(s in msg for s in ("set_current_tenant", "tenant_scope", "No tenant context"))

    def test_error_is_eventsource_error(self) -> None:
        assert isinstance(TenantContextNotSetError(), EventSourceError)


class TestClearIsFinal:
    def test_clear_invalidates_outstanding_tokens(self) -> None:
        token = set_current_tenant(uuid4())
        clear_tenant_context()
        with pytest.raises(TenantContextResetError):
            reset_tenant_context(token)
        assert get_current_tenant() is None

    def test_clear_inside_scope_raises_on_scope_exit(self) -> None:
        with pytest.raises(TenantContextResetError), tenant_scope_sync(uuid4()):
            clear_tenant_context()
        assert get_current_tenant() is None
