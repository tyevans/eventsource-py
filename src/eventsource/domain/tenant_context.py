"""
Tenant context management for multi-tenant applications.

Provides ContextVar-backed tenant ID propagation (`tenant_context`), lookup
helpers (`get_current_tenant`, `get_required_tenant`), manual token management
(`set_current_tenant`, `reset_tenant_context`, `clear_tenant_context`), and
LIFO-enforced context managers (`tenant_scope`, `tenant_scope_sync`).
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
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)

# Monotonic ID generator giving each TenantContextToken a unique diagnostic identity.
_token_ids = itertools.count()


class TenantContextToken:
    """
    Opaque handle returned by set_current_tenant(), required by
    reset_tenant_context() to restore the prior tenant in strict LIFO order.
    """

    __slots__ = ("_id", "_raw")

    def __init__(self, raw: Token[UUID | None]) -> None:
        self._raw = raw
        self._id = next(_token_ids)

    def __repr__(self) -> str:
        return f"TenantContextToken(id={self._id})"


# Context-local stack of active TenantContextTokens (most-recent last) to enforce LIFO.
_token_stack: ContextVar[tuple[TenantContextToken, ...]] = ContextVar(
    "_tenant_context_token_stack", default=()
)


def get_current_tenant() -> UUID | None:
    """Get the current tenant ID from context, or None if not set."""
    return tenant_context.get()


def get_required_tenant() -> UUID:
    """Get the current tenant ID, raising TenantContextNotSetError if not set."""
    tenant_id = tenant_context.get()
    if tenant_id is None:
        raise TenantContextNotSetError()
    return tenant_id


def set_current_tenant(tenant_id: UUID) -> TenantContextToken:
    """
    Set the current tenant ID in context and return a LIFO restoration token.

    Prefer tenant_scope() or tenant_scope_sync() for automatic LIFO cleanup.
    """
    logger.debug("Tenant context set: %s", tenant_id)
    raw_token = tenant_context.set(tenant_id)
    token = TenantContextToken(raw_token)
    _token_stack.set((*_token_stack.get(), token))
    return token


def reset_tenant_context(token: TenantContextToken) -> None:
    """
    Restore the tenant context to the state before the matching set_current_tenant() call.

    Raises TenantContextResetError if token is not the most-recently-set active token.
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
    """Clear the tenant context and invalidate all outstanding TenantContextTokens."""
    logger.debug("Tenant context cleared")
    tenant_context.set(None)
    _token_stack.set(())


@asynccontextmanager
async def tenant_scope(tenant_id: UUID) -> AsyncGenerator[UUID]:
    """Async context manager for scoped tenant context with LIFO restoration."""
    token = set_current_tenant(tenant_id)
    logger.debug("Tenant scope entered: %s", tenant_id)
    try:
        yield tenant_id
    finally:
        reset_tenant_context(token)
        logger.debug("Tenant scope exited: %s", tenant_id)


@contextmanager
def tenant_scope_sync(tenant_id: UUID) -> Generator[UUID]:
    """Sync context manager for scoped tenant context with LIFO restoration."""
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
