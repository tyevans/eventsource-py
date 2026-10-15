"""Property-based tests for tenant context restoration and concurrency isolation."""

from __future__ import annotations

import asyncio
import contextvars
from uuid import UUID, uuid4

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from eventsource import (
    TenantContextNotSetError,
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    set_current_tenant,
    tenant_scope,
    tenant_scope_sync,
)

pytestmark = pytest.mark.timeout(60)

_POOL = [uuid4() for _ in range(5)]
tenant_id_strategy = st.sampled_from(_POOL)


@pytest.fixture(autouse=True)
def _clear_context() -> None:
    """Ensure a clean slate before and after every test in this module."""
    clear_tenant_context()
    yield
    clear_tenant_context()


@given(tenant_ids=st.lists(tenant_id_strategy, min_size=0, max_size=8))
@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_sync_nesting_restores_exactly(tenant_ids: list[UUID]) -> None:
    """Nested tenant_scope_sync entries always restore the prior tenant on exit."""
    clear_tenant_context()

    def recurse(remaining: list[UUID]) -> None:
        if not remaining:
            return
        before = get_current_tenant()
        head, *rest = remaining
        with tenant_scope_sync(head):
            assert get_current_tenant() == head
            recurse(rest)
        assert get_current_tenant() == before

    recurse(tenant_ids)
    assert get_current_tenant() is None


@given(tenant_ids=st.lists(tenant_id_strategy, min_size=0, max_size=8))
@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_async_nesting_restores_exactly(tenant_ids: list[UUID]) -> None:
    """Nested async tenant_scope entries always restore the prior tenant on exit."""
    clear_tenant_context()

    async def recurse(remaining: list[UUID]) -> None:
        if not remaining:
            return
        before = get_current_tenant()
        head, *rest = remaining
        async with tenant_scope(head):
            assert get_current_tenant() == head
            await recurse(rest)
        assert get_current_tenant() == before

    await recurse(tenant_ids)
    assert get_current_tenant() is None


@given(tenant_ids=st.lists(tenant_id_strategy, min_size=2, max_size=12))
@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
    deadline=None,
)
async def test_concurrent_tasks_never_observe_others_tenant(tenant_ids: list[UUID]) -> None:
    """Concurrent tasks scoped to their own tenant never observe another task's tenant."""
    clear_tenant_context()
    observed: dict[int, list[UUID | None]] = {}

    async def worker(idx: int, tenant_id: UUID) -> None:
        samples: list[UUID | None] = []
        async with tenant_scope(tenant_id):
            samples.append(get_current_tenant())
            await asyncio.sleep(0)
            samples.append(get_current_tenant())
            await asyncio.sleep(0)
            samples.append(get_current_tenant())
        observed[idx] = samples

    await asyncio.gather(*(worker(i, t) for i, t in enumerate(tenant_ids)))

    for idx, tenant_id in enumerate(tenant_ids):
        assert all(sample == tenant_id for sample in observed[idx]), (
            f"task {idx} (tenant {tenant_id}) observed foreign tenant(s): {observed[idx]}"
        )

    assert get_current_tenant() is None


async def test_concurrency_isolation_test_actually_detects_breakage() -> None:
    """Sanity check: verify the concurrency property test detects shared-state contamination."""
    shared_state: dict[str, UUID | None] = {"tenant": None}

    async def broken_worker(
        idx: int, tenant_id: UUID, samples: dict[int, list[UUID | None]]
    ) -> None:
        shared_state["tenant"] = tenant_id
        collected: list[UUID | None] = [shared_state["tenant"]]
        await asyncio.sleep(0)
        collected.append(shared_state["tenant"])
        await asyncio.sleep(0)
        collected.append(shared_state["tenant"])
        samples[idx] = collected

    tenant_ids = [uuid4() for _ in range(6)]
    samples: dict[int, list[UUID | None]] = {}
    await asyncio.gather(*(broken_worker(i, t, samples) for i, t in enumerate(tenant_ids)))

    contaminated = any(
        any(sample != tenant_ids[idx] for sample in samples[idx]) for idx in range(len(tenant_ids))
    )
    assert contaminated, (
        "expected the shared-global stand-in to leak tenant context across "
        "tasks; if it didn't, the property test above may not be sensitive "
        "enough to catch a real regression"
    )


class _BoomError(Exception):
    pass


@given(depth=st.integers(min_value=1, max_value=6), fail_at=st.integers(min_value=0))
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_sync_exception_at_any_depth_restores_context(depth: int, fail_at: int) -> None:
    """An exception at any depth within nested sync scopes restores context at every level."""
    clear_tenant_context()
    fail_at = fail_at % depth
    tenant_ids = [uuid4() for _ in range(depth)]

    def recurse(level: int) -> None:
        with tenant_scope_sync(tenant_ids[level]):
            assert get_current_tenant() == tenant_ids[level]
            if level == fail_at:
                raise _BoomError(level)
            recurse(level + 1)

    with pytest.raises(_BoomError):
        recurse(0)

    assert get_current_tenant() is None


@given(depth=st.integers(min_value=1, max_value=6), fail_at=st.integers(min_value=0))
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_async_exception_at_any_depth_restores_context(depth: int, fail_at: int) -> None:
    """An exception at any depth within nested async scopes restores context at every level."""
    clear_tenant_context()
    fail_at = fail_at % depth
    tenant_ids = [uuid4() for _ in range(depth)]

    async def recurse(level: int) -> None:
        async with tenant_scope(tenant_ids[level]):
            assert get_current_tenant() == tenant_ids[level]
            if level == fail_at:
                raise _BoomError(level)
            await recurse(level + 1)

    with pytest.raises(_BoomError):
        await recurse(0)

    assert get_current_tenant() is None


def test_get_required_tenant_raises_documented_exception_type_and_message() -> None:
    clear_tenant_context()
    with pytest.raises(TenantContextNotSetError) as exc_info:
        get_required_tenant()

    message = str(exc_info.value)
    assert message == (
        "No tenant context set. Use set_current_tenant() or tenant_scope() "
        "before performing multi-tenant operations."
    )


async def test_abandoned_async_scope_mid_await_still_restores_on_cancellation() -> None:
    """Cancelling a task mid-await inside tenant_scope resets the token without leaking."""
    clear_tenant_context()
    tenant_id = uuid4()
    saw_tenant_before_cancel = None

    async def scoped_worker() -> None:
        nonlocal saw_tenant_before_cancel
        async with tenant_scope(tenant_id):
            saw_tenant_before_cancel = get_current_tenant()
            await asyncio.sleep(10)

    task = asyncio.create_task(scoped_worker())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert saw_tenant_before_cancel == tenant_id
    assert get_current_tenant() is None


def test_copy_context_run_is_isolated_from_caller() -> None:
    """contextvars.copy_context() isolates mutations inside the copy from the caller."""
    clear_tenant_context()
    caller_tenant = uuid4()
    set_current_tenant(caller_tenant)

    ctx = contextvars.copy_context()

    def mutate_in_copy() -> UUID | None:
        set_current_tenant(uuid4())
        return get_current_tenant()

    result = ctx.run(mutate_in_copy)
    assert result is not None
    assert get_current_tenant() == caller_tenant
    clear_tenant_context()
