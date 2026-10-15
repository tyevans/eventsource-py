"""Multi-tenant domain event classes requiring a tenant_id field."""

from __future__ import annotations

from typing import Any, Self
from uuid import UUID

from pydantic import Field

from eventsource.domain.event import DomainEvent
from eventsource.domain.tenant_context import get_required_tenant


class TenantDomainEvent(DomainEvent):
    """Domain event with required tenant_id for multi-tenant applications."""

    # Override to make required (removes None from type and adds required marker)
    tenant_id: UUID = Field(
        ...,
        description="Tenant this event belongs to (required)",
    )

    @classmethod
    def with_tenant_context(cls, **kwargs: Any) -> Self:
        """Create event with tenant_id automatically populated from current context if not provided."""
        if "tenant_id" not in kwargs:
            kwargs["tenant_id"] = get_required_tenant()

        return cls(**kwargs)


__all__ = ["TenantDomainEvent"]
