"""Tenant routing persistence protocol."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)


@runtime_checkable
class TenantRoutingRepository(Protocol):
    """Protocol for tenant routing persistence and migration routing state."""

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """Get routing configuration for a tenant, or None if not configured."""
        ...

    async def get_or_default(
        self,
        tenant_id: UUID,
        default_store_id: str,
    ) -> TenantRouting:
        """Get routing configuration, atomically creating default via UPSERT if absent."""
        ...

    async def set_routing(
        self,
        tenant_id: UUID,
        store_id: str,
    ) -> None:
        """Set or update the store for a tenant via UPSERT, resetting state to NORMAL."""
        ...

    async def set_migration_state(
        self,
        tenant_id: UUID,
        state: TenantMigrationState,
        migration_id: UUID | None = None,
    ) -> None:
        """Update the migration state for routing decisions."""
        ...

    async def switch_routing(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.MIGRATED,
        migration_id: UUID | None = None,
    ) -> None:
        """Atomically update store_id and migration_state for a tenant."""
        ...

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        """Reset migration state to NORMAL."""
        ...

    async def list_by_state(
        self,
        state: TenantMigrationState,
    ) -> list[TenantRouting]:
        """List tenants in a specific migration state."""
        ...

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """List tenants routed to a specific store."""
        ...


__all__ = ["TenantRoutingRepository"]
