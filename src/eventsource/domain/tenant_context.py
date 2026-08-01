"""
Tenant context management for multi-tenant applications.

This module provides tools for managing tenant context in async and sync code:
- tenant_context: ContextVar for tenant ID propagation
- get_current_tenant(): Get current tenant ID (returns None if not set)
- get_required_tenant(): Get current tenant ID (raises if not set)
- set_current_tenant(): Set the current tenant ID (returns a TenantContextToken)
- reset_tenant_context(): Restore the tenant set by a matching set_current_tenant()
- clear_tenant_context(): Clear the tenant context
- tenant_scope(): Async context manager for scoped tenant context
- tenant_scope_sync(): Sync context manager for scoped tenant context

The ContextVar mechanism ensures proper context isolation between concurrent
async tasks and threads, making it safe for use in multi-tenant applications
-- PROVIDED that context is entered and exited via tenant_scope() /
tenant_scope_sync(). Those context managers restore state in the strict
LIFO order that `with`/`async with` block nesting guarantees.

set_current_tenant()/reset_tenant_context() are the manual counterparts for
code that cannot use a context manager. Unlike raw contextvars.Token.reset()
(which has no LIFO enforcement and would silently resurrect a stale tenant
on out-of-order reset), reset_tenant_context() actively tracks ordering via
a per-context token stack and raises TenantContextResetError if a token is
reset out of order or more than once. tenant_scope()/tenant_scope_sync()
route through the same mechanism internally, so scoped and manual usage can
never disagree about ordering rules. See the warning on set_current_tenant()
for details -- prefer tenant_scope() / tenant_scope_sync() regardless, since
they make LIFO ordering structural rather than something you have to get
right by hand.

Example:
    >>> import asyncio
    >>> from uuid import uuid4
    >>> from eventsource import tenant_scope, get_current_tenant
    >>>
    >>> async def main():
    ...     tenant_id = uuid4()
    ...     async with tenant_scope(tenant_id):
    ...         current = get_current_tenant()
    ...         assert current == tenant_id
    ...     # Context automatically cleared here
    ...     assert get_current_tenant() is None
"""

from __future__ import annotations

import itertools
import logging
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.domain.exceptions import (
    TenantContextNotSetError,
    TenantContextResetError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

logger = logging.getLogger(__name__)

# Context variable for tenant propagation across async boundaries
# Default is None, indicating no tenant context is set
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)

# Monotonically increasing id generator used only to give each
# TenantContextToken a unique identity for diagnostics/equality; itertools.count()
# is safe to share across threads/tasks because each call to next() is a single
# atomic C-level operation under the GIL, and the counter itself carries no
# tenant data -- only the *stack* of tokens (below) needs to be context-local.
_token_ids = itertools.count()


class TenantContextToken:
    """
    Opaque handle returned by set_current_tenant(), required by
    reset_tenant_context() to restore the prior tenant.

    Wraps the raw contextvars.Token so that reset_tenant_context() can
    enforce strict LIFO ordering (see TenantContextResetError) instead of
    silently permitting out-of-order or repeated resets the way
    contextvars.Token.reset() does. Treat this as opaque; do not construct
    it directly or reset the wrapped token yourself.
    """

    __slots__ = ("_id", "_raw")

    def __init__(self, raw: Token[UUID | None]) -> None:
        self._raw = raw
        self._id = next(_token_ids)

    def __repr__(self) -> str:
        return f"TenantContextToken(id={self._id})"


# Stack of currently-active TenantContextTokens, most-recent last. This MUST
# live in a ContextVar (holding an immutable tuple) rather than a plain
# module-level list: a module-level list would be shared mutable state across
# every concurrent asyncio task and thread, silently reintroducing the exact
# cross-tenant leakage this module exists to prevent. A ContextVar gives each
# context (each task's copy_context()) its own independent stack, just like
# tenant_context itself.
_token_stack: ContextVar[tuple[TenantContextToken, ...]] = ContextVar(
    "_tenant_context_token_stack", default=()
)


def get_current_tenant() -> UUID | None:
    """
    Get the current tenant ID from context.

    Returns the current tenant UUID if set, or None if no tenant context
    has been established. This function is safe to call at any time and
    will never raise an exception.

    Returns:
        The current tenant UUID, or None if not set

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import (
        ...     set_current_tenant,
        ...     get_current_tenant,
        ...     clear_tenant_context,
        ... )
        >>> tenant_id = uuid4()
        >>> set_current_tenant(tenant_id)
        >>> assert get_current_tenant() == tenant_id
        >>> clear_tenant_context()
        >>> assert get_current_tenant() is None
    """
    return tenant_context.get()


def get_required_tenant() -> UUID:
    """
    Get the current tenant ID, raising if not set.

    Use this function when tenant context is required for an operation.
    It provides clear error messaging when the context is missing.

    Returns:
        The current tenant UUID

    Raises:
        TenantContextNotSetError: If no tenant context is set

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import (
        ...     set_current_tenant,
        ...     get_required_tenant,
        ...     clear_tenant_context,
        ...     TenantContextNotSetError,
        ... )
        >>> tenant_id = uuid4()
        >>> set_current_tenant(tenant_id)
        >>> assert get_required_tenant() == tenant_id
        >>> clear_tenant_context()
        >>> try:
        ...     get_required_tenant()
        ... except TenantContextNotSetError:
        ...     print("Tenant required but not set")
        Tenant required but not set
    """
    tenant_id = tenant_context.get()
    if tenant_id is None:
        raise TenantContextNotSetError()
    return tenant_id


def set_current_tenant(tenant_id: UUID) -> TenantContextToken:
    """
    Set the current tenant ID in context.

    This sets the tenant context for the current execution context.
    The returned token must be passed to reset_tenant_context() to restore
    the previous context.

    Args:
        tenant_id: The tenant UUID to set

    Returns:
        Token that can be used to restore previous context via
        reset_tenant_context()

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import (
        ...     set_current_tenant,
        ...     get_current_tenant,
        ...     clear_tenant_context,
        ... )
        >>> from eventsource.domain.tenant_context import reset_tenant_context
        >>> tenant_id = uuid4()
        >>> token = set_current_tenant(tenant_id)
        >>> assert get_current_tenant() == tenant_id
        >>> # Manual restoration -- must be strict LIFO (see Warning below).
        >>> # Prefer tenant_scope() for automatic, structurally-LIFO cleanup
        >>> # instead of doing this.
        >>> reset_tenant_context(token)

    Note:
        For scoped tenant context with automatic cleanup, prefer using
        tenant_scope() or tenant_scope_sync() context managers instead.

    Warning:
        Manual set/reset is the UNSAFE-BY-CONVENTION path. The Token
        returned here MUST be passed to reset_tenant_context() in strict
        LIFO order relative to any other outstanding token in the same
        context (i.e. reset the most-recently-created token first, and
        reset each token at most once) -- exactly the order
        tenant_scope()/tenant_scope_sync() already guarantee via
        `with`/`async with` block nesting.

        Unlike raw contextvars.Token.reset() (which enforces nothing and
        would silently resurrect a stale tenant on out-of-order reset),
        reset_tenant_context() actively checks ordering and raises
        TenantContextResetError if you get it wrong:

            token_a = set_current_tenant(tenant_a)
            token_b = set_current_tenant(tenant_b)
            reset_tenant_context(token_a)  # raises TenantContextResetError:
                                            # token_b is still on top
            reset_tenant_context(token_b)  # correct: -> tenant_a
            reset_tenant_context(token_a)  # correct: -> None

        Prefer tenant_scope() / tenant_scope_sync() so LIFO ordering is
        enforced structurally and you never have to think about this.
    """
    logger.debug("Tenant context set: %s", tenant_id)
    raw_token = tenant_context.set(tenant_id)
    token = TenantContextToken(raw_token)
    _token_stack.set((*_token_stack.get(), token))
    return token


def reset_tenant_context(token: TenantContextToken) -> None:
    """
    Restore the tenant context to what it was before the matching
    set_current_tenant() call, enforcing strict LIFO ordering.

    This is the required counterpart to set_current_tenant() for manual
    (non-scope) usage. tenant_scope() and tenant_scope_sync() call this
    internally, so scoped and manual usage can never disagree about
    ordering rules.

    Args:
        token: The token returned by the matching set_current_tenant() call

    Raises:
        TenantContextResetError: If token is not the most-recently-set,
            still-active token in the current context -- i.e. it was
            already reset, or a token set after it hasn't been reset yet.

    Example:
        >>> from uuid import uuid4
        >>> from eventsource.domain.tenant_context import (
        ...     set_current_tenant,
        ...     reset_tenant_context,
        ... )
        >>> tenant_id = uuid4()
        >>> token = set_current_tenant(tenant_id)
        >>> reset_tenant_context(token)
    """
    stack = _token_stack.get()
    if not stack or stack[-1] is not token:
        if any(t is token for t in stack):
            raise TenantContextResetError(
                f"{token!r} is not the most-recently-set active token "
                f"(a token set after it is still active)."
            )
        raise TenantContextResetError(
            f"{token!r} was already reset, or does not belong to the current context."
        )
    tenant_context.reset(token._raw)  # noqa: SLF001
    _token_stack.set(stack[:-1])


def clear_tenant_context() -> None:
    """
    Unconditionally clear the tenant context for the current execution
    context, invalidating ALL outstanding TenantContextTokens.

    This is the hard-reset escape hatch for request/task boundaries
    ("no tenant may survive past this point"). After calling it,
    get_current_tenant() returns None and any reset_tenant_context()
    call with a previously issued token raises TenantContextResetError
    — including the implicit reset performed when an enclosing
    tenant_scope()/tenant_scope_sync() exits. Never call this inside a
    tenant scope unless you want that scope's exit to fail loudly.

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import (
        ...     set_current_tenant,
        ...     clear_tenant_context,
        ...     get_current_tenant,
        ... )
        >>> tenant_id = uuid4()
        >>> set_current_tenant(tenant_id)  # doctest: +ELLIPSIS
        <...TenantContextToken...>
        >>> clear_tenant_context()
        >>> assert get_current_tenant() is None

    Warning:
        In async code with multiple concurrent tasks, be careful with
        clear_tenant_context() as it only affects the current execution
        context. Use tenant_scope() for proper context isolation.
    """
    logger.debug("Tenant context cleared")
    tenant_context.set(None)
    _token_stack.set(())


@asynccontextmanager
async def tenant_scope(tenant_id: UUID) -> AsyncGenerator[UUID, None]:
    """
    Async context manager for scoped tenant context.

    Automatically sets tenant context on entry and restores the previous
    context on exit, ensuring proper cleanup even if exceptions occur.
    This is the recommended way to manage tenant context in async code.

    Args:
        tenant_id: The tenant UUID to set for this scope

    Yields:
        The tenant ID

    Example:
        >>> import asyncio
        >>> from uuid import uuid4
        >>> from eventsource import tenant_scope, get_current_tenant
        >>>
        >>> async def process_tenant_request():
        ...     tenant_id = uuid4()
        ...     async with tenant_scope(tenant_id):
        ...         # All code here runs with tenant_id as context
        ...         current = get_current_tenant()
        ...         assert current == tenant_id
        ...     # Context automatically restored here
        >>>
        >>> asyncio.run(process_tenant_request())

    Note:
        This properly handles nested async contexts and task propagation
        through Python's contextvars mechanism. The token-based reset
        ensures that nested scopes work correctly.

    Example with nesting:
        >>> async def nested_example():
        ...     tenant1 = uuid4()
        ...     tenant2 = uuid4()
        ...     async with tenant_scope(tenant1):
        ...         assert get_current_tenant() == tenant1
        ...         async with tenant_scope(tenant2):
        ...             assert get_current_tenant() == tenant2
        ...         # tenant1 is restored
        ...         assert get_current_tenant() == tenant1
        >>>
        >>> asyncio.run(nested_example())
    """
    token = set_current_tenant(tenant_id)
    logger.debug("Tenant scope entered: %s", tenant_id)
    try:
        yield tenant_id
    finally:
        reset_tenant_context(token)
        logger.debug("Tenant scope exited: %s", tenant_id)


@contextmanager
def tenant_scope_sync(tenant_id: UUID) -> Generator[UUID, None, None]:
    """
    Sync context manager for scoped tenant context.

    Same as tenant_scope but for synchronous code. Automatically sets
    tenant context on entry and restores the previous context on exit.

    Args:
        tenant_id: The tenant UUID to set for this scope

    Yields:
        The tenant ID

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import (
        ...     tenant_scope_sync,
        ...     get_current_tenant,
        ... )
        >>> tenant_id = uuid4()
        >>> with tenant_scope_sync(tenant_id):
        ...     assert get_current_tenant() == tenant_id
        >>> assert get_current_tenant() is None

    Note:
        For async code, use tenant_scope() instead.
    """
    token = set_current_tenant(tenant_id)
    logger.debug("Tenant scope (sync) entered: %s", tenant_id)
    try:
        yield tenant_id
    finally:
        reset_tenant_context(token)
        logger.debug("Tenant scope (sync) exited: %s", tenant_id)


__all__ = [
    "tenant_context",
    "TenantContextToken",
    "get_current_tenant",
    "get_required_tenant",
    "set_current_tenant",
    "reset_tenant_context",
    "clear_tenant_context",
    "tenant_scope",
    "tenant_scope_sync",
]
