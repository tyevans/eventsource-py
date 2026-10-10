"""
Tenant routing persistence protocol.

Defines persistence contracts for tenant routing configuration, migration states,
and cutover state switching.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)


@runtime_checkable
class TenantRoutingRepository(Protocol):
    """
    Protocol for tenant routing persistence.

    Manages tenant-to-store mappings and tracks migration routing state.
    Implementations must handle concurrent access safely and provide
    cache-friendly access patterns.
    """

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """
        Get routing configuration for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            TenantRouting instance or None if not configured
        """
        ...

    async def get_or_default(
        self,
        tenant_id: UUID,
        default_store_id: str,
    ) -> TenantRouting:
        """
        Get routing configuration, creating default if not exists.

        Uses UPSERT semantics to atomically create a default routing
        if one doesn't exist for the tenant.

        Args:
            tenant_id: Tenant UUID
            default_store_id: Default store ID if not configured

        Returns:
            TenantRouting instance (existing or newly created)
        """
        ...

    async def set_routing(
        self,
        tenant_id: UUID,
        store_id: str,
    ) -> None:
        """
        Set or update the store for a tenant.

        Uses UPSERT semantics to create or update the routing.
        Resets migration state to NORMAL if updating an existing routing.

        Args:
            tenant_id: Tenant UUID
            store_id: Target store identifier
        """
        ...

    async def set_migration_state(
        self,
        tenant_id: UUID,
        state: TenantMigrationState,
        migration_id: UUID | None = None,
    ) -> None:
        """
        Update the migration state for routing decisions.

        This method updates the routing state that determines how
        the TenantStoreRouter handles operations for this tenant.

        Args:
            tenant_id: Tenant UUID
            state: New migration state
            migration_id: Active migration ID (if applicable)
        """
        ...

    async def switch_routing(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.MIGRATED,
        migration_id: UUID | None = None,
    ) -> None:
        """
        Atomically update store_id and migration_state for a tenant.

        Executes the route switch and state transition within a single atomic
        transaction boundary, guaranteeing all-or-nothing cutover semantics.

        Args:
            tenant_id: Tenant UUID
            store_id: Target store identifier
            state: Target migration state (default MIGRATED)
            migration_id: Active migration ID (if applicable)
        """
        ...

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        """
        Reset migration state to NORMAL.

        Called after migration completes or aborts to return
        the tenant to normal routing behavior.

        Args:
            tenant_id: Tenant UUID
        """
        ...

    async def list_by_state(
        self,
        state: TenantMigrationState,
    ) -> list[TenantRouting]:
        """
        List tenants in a specific migration state.

        Useful for finding all tenants currently in dual-write mode
        or other migration states.

        Args:
            state: Migration state to filter by

        Returns:
            List of TenantRouting instances
        """
        ...

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """
        List tenants routed to a specific store.

        Useful for finding all tenants on a shared store before
        planning migrations.

        Args:
            store_id: Store identifier

        Returns:
            List of TenantRouting instances
        """
        ...


__all__ = [
    "TenantRoutingRepository",
]
