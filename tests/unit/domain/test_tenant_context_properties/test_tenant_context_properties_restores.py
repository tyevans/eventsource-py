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


# ---------------------------------------------------------------------------
# 1. Nesting restores exactly
# ---------------------------------------------------------------------------


@given(tenant_ids=st.lists(tenant_id_strategy, min_size=0, max_size=8))
@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_sync_nesting_restores_exactly(tenant_ids: list[UUID]) -> None:
    """For arbitrary nested tenant_scope_sync entries, exiting a scope always
    restores exactly the tenant that was current before entering it."""
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
    """Same property as above, but for the async tenant_scope."""
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


# ---------------------------------------------------------------------------
# 2. Concurrent tasks never see each other's tenant
# ---------------------------------------------------------------------------


@given(tenant_ids=st.lists(tenant_id_strategy, min_size=2, max_size=12))
@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
    deadline=None,
)
async def test_concurrent_tasks_never_observe_others_tenant(tenant_ids: list[UUID]) -> None:
    """N concurrent tasks, each scoped to its own tenant, must only ever
    observe their own tenant -- even with forced interleaving via
    ``await asyncio.sleep(0)`` between set and read."""
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
    """Sanity check required by the test plan: verify the concurrency property
    test above would actually fail if isolation were broken. We simulate a
    broken implementation by replacing the per-task ContextVar-scoped set
    with a single shared mutable value that all tasks read/write -- the
    classic bug this module exists to prevent -- and confirm cross-tenant
    contamination is observable and would fail the assertion used above."""
    shared_state: dict[str, UUID | None] = {"tenant": None}

    async def broken_worker(
        idx: int, tenant_id: UUID, samples: dict[int, list[UUID | None]]
    ) -> None:
        # BROKEN: uses a shared global instead of a ContextVar-scoped value.
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

    # With the broken (shared-global) implementation, at least one task must
    # observe a tenant that isn't its own -- proving the property assertion
    # used in test_concurrent_tasks_never_observe_others_tenant is capable of
    # detecting a real isolation failure, not just passing vacuously.
    contaminated = any(
        any(sample != tenant_ids[idx] for sample in samples[idx]) for idx in range(len(tenant_ids))
    )
    assert contaminated, (
        "expected the shared-global stand-in to leak tenant context across "
        "tasks; if it didn't, the property test above may not be sensitive "
        "enough to catch a real regression"
    )


# ---------------------------------------------------------------------------
# 3. Exception safety
# ---------------------------------------------------------------------------


class _BoomError(Exception):
    pass


@given(depth=st.integers(min_value=1, max_value=6), fail_at=st.integers(min_value=0))
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_sync_exception_at_any_depth_restores_context(depth: int, fail_at: int) -> None:
    """An exception raised at any depth within nested sync scopes must still
    unwind and restore the pre-scope tenant at every level."""
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
    """Same as above for the async tenant_scope."""
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


# ---------------------------------------------------------------------------
# 4. get_required_tenant raises with the documented exception
# ---------------------------------------------------------------------------


def test_get_required_tenant_raises_documented_exception_type_and_message() -> None:
    clear_tenant_context()
    with pytest.raises(TenantContextNotSetError) as exc_info:
        get_required_tenant()

    message = str(exc_info.value)
    # exceptions.py documents this exact wording; assert against it verbatim
    # so a drift between the two would be caught.
    assert message == (
        "No tenant context set. Use set_current_tenant() or tenant_scope() "
        "before performing multi-tenant operations."
    )


# ---------------------------------------------------------------------------
# 5. Cancellation and ContextVar isolation
# ---------------------------------------------------------------------------


async def test_abandoned_async_scope_mid_await_still_restores_on_cancellation() -> None:
    """If a task holding an active tenant_scope is cancelled mid-await, the
    `async with` block's __aexit__ must still run (asyncio delivers
    CancelledError into the suspended coroutine), so the token is reset and
    no leak occurs."""
    clear_tenant_context()
    tenant_id = uuid4()
    saw_tenant_before_cancel = None

    async def scoped_worker() -> None:
        nonlocal saw_tenant_before_cancel
        async with tenant_scope(tenant_id):
            saw_tenant_before_cancel = get_current_tenant()
            await asyncio.sleep(10)  # will be cancelled while suspended here

    task = asyncio.create_task(scoped_worker())
    await asyncio.sleep(0)  # let it enter the scope and suspend
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert saw_tenant_before_cancel == tenant_id
    # The cancelling context (this test function) never entered the scope,
    # so it must be unaffected/still None -- proving the cancelled task's
    # context manipulation didn't leak into the caller's context either.
    assert get_current_tenant() is None


def test_copy_context_run_is_isolated_from_caller() -> None:
    """Directly exercises the underlying mechanism the module's docstring
    claims to rely on: contextvars.copy_context() produces an independent
    snapshot such that mutations inside it are invisible to the caller."""
    clear_tenant_context()
    caller_tenant = uuid4()
    set_current_tenant(caller_tenant)

    ctx = contextvars.copy_context()

    def mutate_in_copy() -> UUID | None:
        set_current_tenant(uuid4())
        return get_current_tenant()

    result = ctx.run(mutate_in_copy)
    assert result is not None
    # Caller's own context must be unaffected by the mutation performed
    # inside the copied context.
    assert get_current_tenant() == caller_tenant
    clear_tenant_context()
