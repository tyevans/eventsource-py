"""
Multi-tenancy exceptions.

This module provides exceptions specific to multi-tenant operations:
- TenantContextNotSetError: Raised when tenant context is required but not set
- TenantMismatchError: Raised when event tenant_id doesn't match expected context
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.exceptions import EventSourceError

if TYPE_CHECKING:
    from collections.abc import Sequence


class TenantContextNotSetError(EventSourceError):
    """
    Raised when tenant context is required but not set.

    This typically occurs when:
    1. Using TenantDomainEvent.with_tenant_context() without context
    2. Using TenantAwareRepository without tenant context
    3. Using get_required_tenant() without context

    Solution: Use set_current_tenant() or tenant_scope() before
    operations requiring tenant context.

    Example:
        >>> from eventsource.multitenancy import get_required_tenant
        >>> try:
        ...     tenant = get_required_tenant()
        ... except TenantContextNotSetError:
        ...     print("No tenant context set")
        No tenant context set
    """

    def __init__(self) -> None:
        super().__init__(
            "No tenant context set. Use set_current_tenant() or tenant_scope() "
            "before performing multi-tenant operations."
        )


class TenantMismatchError(EventSourceError):
    """
    Raised when event tenant_id doesn't match expected tenant context.

    This typically occurs when saving events with a different tenant_id
    than the current context. This error prevents cross-tenant data leakage
    and ensures tenant isolation in multi-tenant applications.

    Attributes:
        expected: The expected tenant UUID from context
        actual: The actual tenant UUID found in the event(s)
        event_ids: List of event IDs that have mismatched tenant

    Example:
        >>> from uuid import uuid4
        >>> from eventsource.multitenancy import TenantMismatchError
        >>> tenant_a = uuid4()
        >>> tenant_b = uuid4()
        >>> event_id = uuid4()
        >>> error = TenantMismatchError(
        ...     expected=tenant_a,
        ...     actual=tenant_b,
        ...     event_ids=[event_id],
        ... )
        >>> print(error)  # doctest: +ELLIPSIS
        Tenant mismatch: expected ..., got .... Affected events: [...]
    """

    def __init__(
        self,
        expected: UUID,
        actual: UUID,
        event_ids: Sequence[UUID],
    ) -> None:
        self.expected = expected
        self.actual = actual
        self.event_ids = list(event_ids)

        # Format event IDs for message (limit to 5 for readability)
        event_list = ", ".join(str(eid) for eid in self.event_ids[:5])
        if len(self.event_ids) > 5:
            event_list += f"... and {len(self.event_ids) - 5} more"

        super().__init__(
            f"Tenant mismatch: expected {expected}, got {actual}. Affected events: [{event_list}]"
        )


__all__ = ["TenantContextNotSetError", "TenantMismatchError"]
