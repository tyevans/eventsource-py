"""Multi-tenant domain event classes requiring a tenant_id field."""

from __future__ import annotations

from typing import Any, Self
from uuid import UUID

from pydantic import Field

from eventsource.domain.event import DomainEvent
from eventsource.domain.tenant_context import (
    get_current_tenant,
    get_required_tenant,
    reject_tenant_contradiction,
)


class TenantDomainEvent(DomainEvent):
    """Domain event with required tenant_id for multi-tenant applications."""

    # Override to make required (removes None from type and adds required marker)
    tenant_id: UUID = Field(
        ...,
        description="Tenant this event belongs to (required)",
    )

    @classmethod
    def with_tenant_context(cls, **kwargs: Any) -> Self:
        """Create event with tenant_id from the active scope, or an explicit tenant_id.

        Without an explicit tenant_id the active scope is required. An explicit
        tenant_id is accepted when no scope is active or when it equals the
        active scope; a different tenant raises TenantMismatchError.
        """
        if "tenant_id" not in kwargs:
            kwargs["tenant_id"] = get_required_tenant()
            return cls(**kwargs)

        event = cls(**kwargs)
        reject_tenant_contradiction(
            get_current_tenant(),
            event.tenant_id,
            event_ids=[event.event_id],
        )
        return event


__all__ = ["TenantDomainEvent"]
