"""
Property-based tests for tenant context isolation.

Tenant isolation is a universally-quantified invariant: for ANY sequence of
operations, under ANY interleaving, no operation may observe or mutate
another tenant's data. Example-based tests only ever check the tenant pairs
and orderings someone thought of up front. These tests use Hypothesis to
generate arbitrary nestings, interleavings, and failure points to attack
that invariant directly.

Covers:
- Nesting restores exactly, for arbitrary depth and arbitrary (possibly
  repeated) tenant IDs.
- Concurrent asyncio tasks never observe each other's tenant context.
- Exceptions (including inside nested scopes) still restore prior context.
- clear_tenant_context() inside an active scope does not corrupt restore-on-exit.
- get_required_tenant() raises when unset.
- Generator/early-exit scenarios and token-reset edge cases.
"""

from __future__ import annotations

import asyncio
import contextvars
from uuid import UUID, uuid4

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

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

pytestmark = pytest.mark.timeout(60)

# A handful of stable tenant UUIDs so Hypothesis can generate repeats
# (repeated tenant IDs at different nesting depths are exactly the case
# most likely to expose an off-by-one in token bookkeeping).
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
    contamination is observable and would fail the assertion used above.
    """
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
# 4. clear_tenant_context() interaction with active scopes
# ---------------------------------------------------------------------------


def test_clear_inside_scope_is_overridden_by_scope_exit() -> None:
    """clear_tenant_context() inside an active scope only mutates the
    *current* value; it does not invalidate the scope's restore token. On
    exit, tenant_scope_sync restores whatever tenant was current *before*
    the scope was entered, even though clear_tenant_context() set it to
    None in between. This is the actual (and, we believe, intentional)
    behavior -- documented here as a property so a future change to the
    restore mechanism can't silently alter it without a test noticing."""
    outer = uuid4()
    with tenant_scope_sync(outer):
        assert get_current_tenant() == outer
        clear_tenant_context()
        assert get_current_tenant() is None
        # Even though we cleared, exiting the scope restores pre-scope state,
        # NOT the just-cleared None-as-if-set-by-us state.
    assert get_current_tenant() is None  # pre-scope state actually was None


def test_clear_inside_nested_scope_does_not_leak_to_outer_restore() -> None:
    """clear_tenant_context() inside an inner nested scope must not corrupt
    the outer scope's eventual restore value."""
    outer = uuid4()
    inner = uuid4()
    with tenant_scope_sync(outer):
        with tenant_scope_sync(inner):
            assert get_current_tenant() == inner
            clear_tenant_context()
            assert get_current_tenant() is None
        # Inner scope restores to `outer`, regardless of the clear() above.
        assert get_current_tenant() == outer
    assert get_current_tenant() is None


async def test_clear_inside_async_scope_is_overridden_by_scope_exit() -> None:
    outer = uuid4()
    async with tenant_scope(outer):
        assert get_current_tenant() == outer
        clear_tenant_context()
        assert get_current_tenant() is None
    assert get_current_tenant() is None


# ---------------------------------------------------------------------------
# 5. get_required_tenant raises with the documented exception
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


def test_get_required_tenant_raises_after_scope_exit() -> None:
    tenant_id = uuid4()
    with tenant_scope_sync(tenant_id):
        assert get_required_tenant() == tenant_id
    with pytest.raises(TenantContextNotSetError):
        get_required_tenant()


# ---------------------------------------------------------------------------
# 6. Generator / early-exit / token-reset edge cases
# ---------------------------------------------------------------------------


def test_unclosed_sync_generator_leaves_context_set() -> None:
    """tenant_scope_sync is a @contextmanager (generator-based). If it is
    driven manually via next() rather than `with`, and never closed, the
    `finally: tenant_context.reset(token)` never runs -- so the tenant
    context leaks. This is inherent to contextlib generator-based context
    managers in general (not specific to this module), but we pin the
    behavior down explicitly: an abandoned, unclosed generator DOES leak.
    Calling .close() on it, however, triggers the finally block via
    GeneratorExit and correctly restores the context.
    """
    clear_tenant_context()
    tenant_id = uuid4()

    cm = tenant_scope_sync(tenant_id)
    # tenant_scope_sync returns a _GeneratorContextManager; drive it directly
    # via its __enter__ to simulate a caller that never reaches __exit__.
    entered = cm.__enter__()
    assert entered == tenant_id
    assert get_current_tenant() == tenant_id

    # Simulate abandonment: neither __exit__ nor close() is called here.
    # (We do NOT assert leakage persists forever -- only that it is not
    # magically cleaned up by anything other than closing/exiting.)
    assert get_current_tenant() == tenant_id

    # Now explicitly close -- this is what a well-behaved caller (or `with`)
    # would do, and it does restore correctly.
    cm.__exit__(None, None, None)
    assert get_current_tenant() is None


def test_token_reset_out_of_lifo_order_silently_corrupts_state() -> None:
    """contextvars.Token.reset() does NOT enforce LIFO discipline and does
    NOT raise when tokens are reset out of order -- contrary to what the
    test plan hypothesized. Each token simply remembers the value that was
    current *at the moment it was created* and restores exactly that value,
    unconditionally. This means resetting an *older* token first, then a
    *newer* one, silently resurrects a stale value instead of ending up at
    the true "nothing set" state.

    This is a real footgun: tenant_scope/tenant_scope_sync are safe only
    because they always reset in strict LIFO order (guaranteed by Python's
    `with`/`async with` block nesting). Anyone bypassing the context
    managers and calling set_current_tenant()/tenant_context.reset()
    manually, out of order, can silently leave a *stale, wrong tenant*
    active with no exception raised to signal the mistake.
    """
    clear_tenant_context()
    tenant_a = uuid4()
    tenant_b = uuid4()
    token_a = set_current_tenant(tenant_a)  # old value: None
    token_b = set_current_tenant(tenant_b)  # old value: tenant_a

    # Reset out of order: token_a first (no exception).
    tenant_context.reset(token_a)
    assert get_current_tenant() is None

    # Now reset token_b -- it restores tenant_a (its captured "old value"),
    # NOT None. The context has silently gone from "cleared" back to a
    # tenant that was never re-entered by any active scope.
    tenant_context.reset(token_b)
    assert get_current_tenant() == tenant_a

    clear_tenant_context()
    assert get_current_tenant() is None


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


# ---------------------------------------------------------------------------
# Round-trip sanity: contextvars.copy_context() isolation (used implicitly by
# asyncio.create_task / gather) really does give each task its own copy.
# ---------------------------------------------------------------------------


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
