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
    has been established. Safe to call at any time and never raises.
    """
    return tenant_context.get()


def get_required_tenant() -> UUID:
    """
    Get the current tenant ID, raising if not set.

    Returns:
        The current tenant UUID

    Raises:
        TenantContextNotSetError: If no tenant context is set
    """
    tenant_id = tenant_context.get()
    if tenant_id is None:
        raise TenantContextNotSetError()
    return tenant_id


def set_current_tenant(tenant_id: UUID) -> TenantContextToken:
    """
    Set the current tenant ID in context.

    The returned token must be passed to reset_tenant_context() in strict
    LIFO order to restore the previous context. Prefer tenant_scope() or
    tenant_scope_sync() for automatic, structurally-LIFO cleanup.

    Args:
        tenant_id: The tenant UUID to set

    Returns:
        Token that can be used to restore previous context via
        reset_tenant_context()
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

    Args:
        token: The token returned by the matching set_current_tenant() call

    Raises:
        TenantContextResetError: If token is not the most-recently-set,
            still-active token in the current context.
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

    After calling it, get_current_tenant() returns None and any
    reset_tenant_context() call with a previously issued token raises
    TenantContextResetError.
    """
    logger.debug("Tenant context cleared")
    tenant_context.set(None)
    _token_stack.set(())


@asynccontextmanager
async def tenant_scope(tenant_id: UUID) -> AsyncGenerator[UUID]:
    """
    Async context manager for scoped tenant context.

    Automatically sets tenant context on entry and restores the previous
    context on exit, ensuring proper cleanup even if exceptions occur.

    Args:
        tenant_id: The tenant UUID to set for this scope

    Yields:
        The tenant ID
    """
    token = set_current_tenant(tenant_id)
    logger.debug("Tenant scope entered: %s", tenant_id)
    try:
        yield tenant_id
    finally:
        reset_tenant_context(token)
        logger.debug("Tenant scope exited: %s", tenant_id)


@contextmanager
def tenant_scope_sync(tenant_id: UUID) -> Generator[UUID]:
    """
    Sync context manager for scoped tenant context.

    Automatically sets tenant context on entry and restores the previous
    context on exit.

    Args:
        tenant_id: The tenant UUID to set for this scope

    Yields:
        The tenant ID
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
