"""
Multi-tenant domain event classes.

This module provides TenantDomainEvent, a base class for domain events
that require a tenant_id field. Unlike the base DomainEvent where
tenant_id is optional, TenantDomainEvent makes it required.

Example:
    >>> from uuid import uuid4
    >>> from eventsource.multitenancy import TenantDomainEvent, tenant_scope
    >>>
    >>> class OrderCreated(TenantDomainEvent):
    ...     aggregate_type: str = "Order"
    ...     customer_id: UUID
    ...
    >>> # With explicit tenant_id
    >>> tenant_id = uuid4()
    >>> order_id = uuid4()
    >>> customer_id = uuid4()
    >>> event = OrderCreated(
    ...     aggregate_id=order_id,
    ...     tenant_id=tenant_id,
    ...     customer_id=customer_id,
    ... )
    >>> assert event.tenant_id == tenant_id
"""

from __future__ import annotations

from typing import Any, Self
from uuid import UUID

from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.multitenancy.context import get_required_tenant


class TenantDomainEvent(DomainEvent):
    """
    Domain event with required tenant_id for multi-tenant applications.

    Unlike DomainEvent where tenant_id is optional, this class makes it
    required. Use with_tenant_context() to auto-populate from context.

    The tenant_id field is validated to be a non-None UUID, ensuring that
    all events created from this class are properly associated with a tenant.

    Attributes:
        tenant_id: Required tenant UUID this event belongs to

    Example with explicit tenant_id:
        >>> from uuid import uuid4, UUID
        >>>
        >>> class ProductAdded(TenantDomainEvent):
        ...     aggregate_type: str = "Catalog"
        ...     product_name: str
        ...     price: float
        ...
        >>> tenant_id = uuid4()
        >>> catalog_id = uuid4()
        >>> event = ProductAdded(
        ...     aggregate_id=catalog_id,
        ...     tenant_id=tenant_id,
        ...     product_name="Widget",
        ...     price=9.99,
        ... )
        >>> assert event.tenant_id == tenant_id

    Example with context (preferred in request handlers):
        >>> import asyncio
        >>> from eventsource.multitenancy import tenant_scope
        >>>
        >>> async def process_request():
        ...     tenant_id = uuid4()
        ...     catalog_id = uuid4()
        ...     async with tenant_scope(tenant_id):
        ...         event = ProductAdded.with_tenant_context(
        ...             aggregate_id=catalog_id,
        ...             product_name="Gadget",
        ...             price=19.99,
        ...         )
        ...         assert event.tenant_id == tenant_id
        >>>
        >>> asyncio.run(process_request())

    Note:
        Unlike DomainEvent, validation will fail if tenant_id is not
        provided either explicitly or via with_tenant_context().
    """

    # Override to make required (removes None from type and adds required marker)
    tenant_id: UUID = Field(
        ...,
        description="Tenant this event belongs to (required)",
    )

    @classmethod
    def with_tenant_context(cls, **kwargs: Any) -> Self:
        """
        Create event with tenant_id automatically populated from context.

        Uses the current tenant context to populate tenant_id. If tenant_id
        is explicitly provided in kwargs, that value is used instead.

        This is the preferred way to create TenantDomainEvent instances
        when working within a tenant scope, as it ensures consistency
        between the context and the event.

        Args:
            **kwargs: Event field values (all fields except tenant_id
                can be provided; tenant_id is taken from context)

        Returns:
            Event instance with tenant_id set

        Raises:
            TenantContextNotSetError: If no tenant context is set and
                tenant_id not explicitly provided

        Example with tenant_scope:
            >>> import asyncio
            >>> from uuid import uuid4, UUID
            >>> from eventsource.multitenancy import tenant_scope
            >>>
            >>> class InvoiceCreated(TenantDomainEvent):
            ...     aggregate_type: str = "Invoice"
            ...     amount: float
            ...     currency: str
            ...
            >>> async def create_invoice():
            ...     tenant_id = uuid4()
            ...     invoice_id = uuid4()
            ...     async with tenant_scope(tenant_id):
            ...         event = InvoiceCreated.with_tenant_context(
            ...             aggregate_id=invoice_id,
            ...             amount=100.00,
            ...             currency="USD",
            ...         )
            ...         return event.tenant_id == tenant_id
            >>>
            >>> asyncio.run(create_invoice())
            True

        Example with explicit override:
            >>> # Even within a scope, explicit tenant_id takes precedence
            >>> async def explicit_override():
            ...     scope_tenant = uuid4()
            ...     explicit_tenant = uuid4()
            ...     invoice_id = uuid4()
            ...     async with tenant_scope(scope_tenant):
            ...         event = InvoiceCreated.with_tenant_context(
            ...             aggregate_id=invoice_id,
            ...             amount=50.00,
            ...             currency="EUR",
            ...             tenant_id=explicit_tenant,  # Overrides context
            ...         )
            ...         return event.tenant_id == explicit_tenant
            >>>
            >>> asyncio.run(explicit_override())
            True
        """
        # Don't override explicit tenant_id
        if "tenant_id" not in kwargs:
            kwargs["tenant_id"] = get_required_tenant()

        return cls(**kwargs)


__all__ = ["TenantDomainEvent"]
