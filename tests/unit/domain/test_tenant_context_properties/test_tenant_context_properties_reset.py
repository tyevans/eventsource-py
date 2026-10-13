"""Property-based tests for tenant context reset ordering and token lifecycle."""

from __future__ import annotations

from uuid import uuid4

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from eventsource import (
    TenantContextResetError,
    clear_tenant_context,
    get_current_tenant,
    reset_tenant_context,
    set_current_tenant,
    tenant_context,
    tenant_scope_sync,
)

pytestmark = pytest.mark.timeout(60)


@pytest.fixture(autouse=True)
def _clear_context() -> None:
    """Ensure a clean slate before and after every test in this module."""
    clear_tenant_context()
    yield
    clear_tenant_context()


def test_unclosed_sync_generator_leaves_context_set() -> None:
    """tenant_scope_sync is a @contextmanager (generator-based). If it is
    driven manually via next() rather than `with`, and never closed, the
    `finally: tenant_context.reset(token)` never runs -- so the tenant
    context leaks. This is inherent to contextlib generator-based context
    managers in general (not specific to this module), but we pin the
    behavior down explicitly: an abandoned, unclosed generator DOES leak.
    Calling .close() on it, however, triggers the finally block via
    GeneratorExit and correctly restores the context."""
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


def test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state() -> None:
    """Pins down the underlying primitive's actual behavior, which is WHY
    reset_tenant_context() exists: raw contextvars.Token.reset() does NOT
    enforce LIFO discipline and does NOT raise when tokens are reset out of
    order. Each token simply remembers the value that was current *at the
    moment it was created* and restores exactly that value, unconditionally.
    Resetting an *older* token first, then a *newer* one, silently
    resurrects a stale value instead of ending up at the true "nothing set"
    state.

    This test operates on tenant_context/tenant_context.reset() directly
    (bypassing set_current_tenant()/reset_tenant_context() entirely) to
    confirm the raw primitive's danger still exists at the contextvars
    layer -- it always will, since we can't change contextvars itself. The
    module's hardened public API (set_current_tenant() +
    reset_tenant_context()) is what actually prevents this now; see
    test_reset_tenant_context_rejects_out_of_lifo_order below."""
    clear_tenant_context()
    tenant_a = uuid4()
    tenant_b = uuid4()
    token_a = tenant_context.set(tenant_a)  # old value: None
    token_b = tenant_context.set(tenant_b)  # old value: tenant_a

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


def test_reset_tenant_context_rejects_out_of_lifo_order() -> None:
    """The hardened public API must refuse what the raw primitive silently
    allows: resetting an older token while a newer one is still active
    raises TenantContextResetError instead of resurrecting a stale tenant."""
    clear_tenant_context()
    tenant_a = uuid4()
    tenant_b = uuid4()
    token_a = set_current_tenant(tenant_a)
    token_b = set_current_tenant(tenant_b)

    with pytest.raises(TenantContextResetError):
        reset_tenant_context(token_a)

    # State must be unaffected by the rejected reset attempt.
    assert get_current_tenant() == tenant_b

    # Resetting in the correct LIFO order works and ends at None.
    reset_tenant_context(token_b)
    assert get_current_tenant() == tenant_a
    reset_tenant_context(token_a)
    assert get_current_tenant() is None


def test_reset_tenant_context_rejects_double_reset() -> None:
    """Resetting the same token twice must raise, not silently no-op or
    resurrect a stale value."""
    clear_tenant_context()
    tenant_id = uuid4()
    token = set_current_tenant(tenant_id)
    reset_tenant_context(token)
    assert get_current_tenant() is None

    with pytest.raises(TenantContextResetError):
        reset_tenant_context(token)

    assert get_current_tenant() is None


@given(data=st.data())
@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_reset_sequence_is_lifo_or_raises(data: st.DataObject) -> None:
    """Core invariant of the hardened API: for an arbitrary sequence of
    set_current_tenant() calls followed by reset_tenant_context() calls in
    an arbitrary order, EITHER the reset order is exactly the reverse
    (LIFO) of the set order and every reset succeeds, ending back at the
    starting state -- OR some reset call in the sequence raises
    TenantContextResetError. There is no third outcome (a non-LIFO
    sequence completing "successfully" with a wrong final state)."""
    clear_tenant_context()
    n = data.draw(st.integers(min_value=1, max_value=6))
    reset_order = data.draw(st.permutations(list(range(n))))

    tenant_ids = [uuid4() for _ in range(n)]
    tokens = [set_current_tenant(t) for t in tenant_ids]
    is_lifo = list(reset_order) == list(reversed(range(n)))

    raised = False
    for idx in reset_order:
        try:
            reset_tenant_context(tokens[idx])
        except TenantContextResetError:
            raised = True
            break

    if is_lifo:
        assert not raised, "strict LIFO reset order must never raise"
        assert get_current_tenant() is None
    else:
        assert raised, (
            f"non-LIFO reset order {list(reset_order)} for {n} tokens "
            "completed without raising -- the hardened API must reject this"
        )

    # Best-effort cleanup so a broken run doesn't poison later tests; any
    # tokens not already reset are simply abandoned (same as documented
    # clear_tenant_context() behavior for manual usage).
    clear_tenant_context()
