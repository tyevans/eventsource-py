"""
Tenant context management for multi-tenant applications.

This module provides tools for managing tenant context in async and sync code:
- tenant_context: ContextVar for tenant ID propagation
- get_current_tenant(): Get current tenant ID (returns None if not set)
- get_required_tenant(): Get current tenant ID (raises if not set)
- set_current_tenant(): Set the current tenant ID
- clear_tenant_context(): Clear the tenant context
- tenant_scope(): Async context manager for scoped tenant context
- tenant_scope_sync(): Sync context manager for scoped tenant context

The ContextVar mechanism ensures proper context isolation between concurrent
async tasks and threads, making it safe for use in multi-tenant applications.

Example:
    >>> import asyncio
    >>> from uuid import uuid4
    >>> from eventsource.multitenancy import tenant_scope, get_current_tenant
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

import logging
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.multitenancy.exceptions import TenantContextNotSetError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

logger = logging.getLogger(__name__)

# Context variable for tenant propagation across async boundaries
# Default is None, indicating no tenant context is set
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)


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
        >>> from eventsource.multitenancy import (
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
        >>> from eventsource.multitenancy import (
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


def set_current_tenant(tenant_id: UUID) -> Token[UUID | None]:
    """
    Set the current tenant ID in context.

    This sets the tenant context for the current execution context.
    The returned token can be used to restore the previous context
    using tenant_context.reset(token).

    Args:
        tenant_id: The tenant UUID to set

    Returns:
        Token that can be used to restore previous context

    Example:
        >>> from uuid import uuid4
        >>> from eventsource.multitenancy import (
        ...     set_current_tenant,
        ...     get_current_tenant,
        ...     clear_tenant_context,
        ... )
        >>> tenant_id = uuid4()
        >>> token = set_current_tenant(tenant_id)
        >>> assert get_current_tenant() == tenant_id
        >>> # Manual restoration (prefer tenant_scope for automatic cleanup)
        >>> from eventsource.multitenancy.context import tenant_context
        >>> tenant_context.reset(token)

    Note:
        For scoped tenant context with automatic cleanup, prefer using
        tenant_scope() or tenant_scope_sync() context managers instead.
    """
    logger.debug("Tenant context set: %s", tenant_id)
    return tenant_context.set(tenant_id)


def clear_tenant_context() -> None:
    """
    Clear the tenant context.

    This should be called after request processing to prevent tenant leakage
    between requests. In most cases, prefer using tenant_scope() which
    handles cleanup automatically.

    Example:
        >>> from uuid import uuid4
        >>> from eventsource.multitenancy import (
        ...     set_current_tenant,
        ...     clear_tenant_context,
        ...     get_current_tenant,
        ... )
        >>> tenant_id = uuid4()
        >>> set_current_tenant(tenant_id)
        >>> clear_tenant_context()
        >>> assert get_current_tenant() is None

    Warning:
        In async code with multiple concurrent tasks, be careful with
        clear_tenant_context() as it only affects the current execution
        context. Use tenant_scope() for proper context isolation.
    """
    logger.debug("Tenant context cleared")
    tenant_context.set(None)


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
        >>> from eventsource.multitenancy import tenant_scope, get_current_tenant
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
    token = tenant_context.set(tenant_id)
    logger.debug("Tenant scope entered: %s", tenant_id)
    try:
        yield tenant_id
    finally:
        tenant_context.reset(token)
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
        >>> from eventsource.multitenancy import (
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
    token = tenant_context.set(tenant_id)
    logger.debug("Tenant scope (sync) entered: %s", tenant_id)
    try:
        yield tenant_id
    finally:
        tenant_context.reset(token)
        logger.debug("Tenant scope (sync) exited: %s", tenant_id)


__all__ = [
    "tenant_context",
    "get_current_tenant",
    "get_required_tenant",
    "set_current_tenant",
    "clear_tenant_context",
    "tenant_scope",
    "tenant_scope_sync",
]
