"""
Multi-tenancy support for eventsource-py.

This module provides tools for building multi-tenant event-sourced applications:

- **Context Management**: tenant_context ContextVar and helper functions for
  managing tenant context across async boundaries
- **TenantDomainEvent**: Event base class with required tenant_id field
- **TenantAwareRepository**: Repository wrapper that enforces tenant isolation
- **Exceptions**: TenantContextNotSetError and TenantMismatchError for clear
  error handling

Example usage with async context manager:
    >>> import asyncio
    >>> from uuid import uuid4, UUID
    >>> from eventsource.multitenancy import (
    ...     tenant_scope,
    ...     TenantDomainEvent,
    ...     get_current_tenant,
    ... )
    >>>
    >>> class OrderCreated(TenantDomainEvent):
    ...     aggregate_type: str = "Order"
    ...     customer_id: UUID
    ...
    >>> async def process_order(tenant_id: UUID):
    ...     async with tenant_scope(tenant_id):
    ...         order_id = uuid4()
    ...         customer_id = uuid4()
    ...         # Event automatically gets tenant_id from context
    ...         event = OrderCreated.with_tenant_context(
    ...             aggregate_id=order_id,
    ...             customer_id=customer_id,
    ...         )
    ...         assert event.tenant_id == tenant_id
    ...         return True
    >>>
    >>> asyncio.run(process_order(uuid4()))
    True

Example with manual context management:
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

For synchronous code:
    >>> from eventsource.multitenancy import tenant_scope_sync
    >>> tenant_id = uuid4()
    >>> with tenant_scope_sync(tenant_id):
    ...     assert get_current_tenant() == tenant_id
    >>> assert get_current_tenant() is None

Example with TenantAwareRepository:
    >>> from eventsource.multitenancy import TenantAwareRepository, tenant_scope
    >>> from eventsource.aggregates import AggregateRepository
    >>>
    >>> # Wrap an existing repository for tenant isolation
    >>> tenant_repo = TenantAwareRepository(order_repository)
    >>>
    >>> async def save_order(tenant_id: UUID, order):
    ...     async with tenant_scope(tenant_id):
    ...         await tenant_repo.save(order)  # Validates tenant_id on events

Future extensions (DX-012):
    - Projection tenant filtering via tenant_filter parameter
"""

from eventsource.multitenancy.context import (
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    set_current_tenant,
    tenant_context,
    tenant_scope,
    tenant_scope_sync,
)
from eventsource.multitenancy.events import TenantDomainEvent
from eventsource.multitenancy.exceptions import (
    TenantContextNotSetError,
    TenantMismatchError,
)
from eventsource.multitenancy.repository import TenantAwareRepository

__all__ = [
    # Context management
    "tenant_context",
    "get_current_tenant",
    "get_required_tenant",
    "set_current_tenant",
    "clear_tenant_context",
    "tenant_scope",
    "tenant_scope_sync",
    # Events
    "TenantDomainEvent",
    # Repository
    "TenantAwareRepository",
    # Exceptions
    "TenantContextNotSetError",
    "TenantMismatchError",
]
