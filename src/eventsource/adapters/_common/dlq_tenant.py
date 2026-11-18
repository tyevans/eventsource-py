"""Tenant scope rule for dead letter queue entries (ADR-0114).

The memory DLQ adapter calls these functions directly. The SQL DLQ adapter calls
refuse_cleanup_under_scope() and writes visible_under_scope() as SQL in
adapters/sql/dlq_sql.py scope_clause(). DLQTenantScopeConformance pins the two
to the same results. The context-reading wrappers call get_current_tenant()
once. Pure callers can use visible_under_scope() with an explicit active tenant.
"""

from __future__ import annotations

from uuid import UUID

from eventsource.domain.exceptions.tenant import TenantScopeOperationError
from eventsource.domain.tenant_context import get_current_tenant
from eventsource.ports.dlq import DLQEntry


def visible_under_scope(entry_tenant: UUID | None, active_tenant: UUID | None) -> bool:
    """Return True when an entry is visible to the caller.

    With no scope, every entry is visible. Under a scope, only entries owned by
    the active tenant are visible. A null entry tenant never matches a scope.
    """
    if active_tenant is None:
        return True
    return entry_tenant == active_tenant


def entry_visible_now(entry: DLQEntry) -> bool:
    """Return True when the active tenant scope may see the entry."""
    return visible_under_scope(entry.tenant_id, get_current_tenant())


def refuse_cleanup_under_scope() -> None:
    """Raise when a cleanup would delete entries of every tenant from within a scope."""
    if get_current_tenant() is not None:
        raise TenantScopeOperationError("delete_resolved_events")


__all__ = ["entry_visible_now", "refuse_cleanup_under_scope", "visible_under_scope"]
