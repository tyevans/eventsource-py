"""
Tenant routing resolution and store lookup helpers.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.router_types import StoreNotFoundError
from eventsource.application.migration.write_pause import WritePausedError, WritePauseManager
from eventsource.domain.event import DomainEvent
from eventsource.ports import FullEventStore
from eventsource.ports.migration.models import TenantMigrationState

if TYPE_CHECKING:
    from eventsource.ports.migration.repositories import TenantRoutingRepository

logger = logging.getLogger(__name__)


class RouterResolutionMixin:
    """Mixin providing tenant store resolution and routing lookups."""

    _default_store: FullEventStore
    _stores: dict[str, FullEventStore]
    _routing_repo: TenantRoutingRepository
    _dual_write_interceptors: dict[UUID, FullEventStore]
    _write_pause_manager: WritePauseManager
    _write_pause_timeout: float

    async def get_store_for_tenant(self, tenant_id: UUID) -> FullEventStore:
        """
        Get the read store for a specific tenant.

        This is a convenience method for external callers who need
        direct access to a tenant's store.

        Args:
            tenant_id: Tenant UUID

        Returns:
            FullEventStore for the tenant
        """
        return await self._get_read_store(tenant_id)

    async def get_write_stores_for_tenant(self, tenant_id: UUID) -> list[FullEventStore]:
        """
        Get all write stores for a tenant during migration.

        During DUAL_WRITE phase, returns both source and target stores.
        Otherwise, returns just the single write store.

        Args:
            tenant_id: Tenant UUID

        Returns:
            List of FullEventStore instances for writing

        Raises:
            StoreNotFoundError: If the routing record names a
                target_store_id that has not been registered. Unlike the
                other store lookups in this class, there is no sensible
                default to fall back to here -- a dual-write phase that
                silently dropped its target would make writes disappear
                without any signal.
        """
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return [self._default_store]

        state = routing.migration_state

        if state == TenantMigrationState.DUAL_WRITE:
            # In dual-write, return both stores
            stores: list[FullEventStore] = []
            source_store = self._stores.get(routing.store_id, self._default_store)
            stores.append(source_store)

            if routing.target_store_id:
                target_store = self._stores.get(routing.target_store_id)
                if target_store is None:
                    raise StoreNotFoundError(routing.target_store_id)
                stores.append(target_store)

            return stores

        # Otherwise, return single write store
        write_store = await self._get_write_store(tenant_id)
        return [write_store]

    def _extract_tenant_id(self, events: Sequence[DomainEvent]) -> UUID | None:
        """
        Extract tenant_id from events.

        Assumes all events in a batch have the same tenant_id.
        Returns None if no tenant_id is set.

        Args:
            events: The events being appended

        Returns:
            Tenant UUID or None
        """
        if events and events[0].tenant_id:
            return events[0].tenant_id
        return None

    async def _get_write_store(self, tenant_id: UUID | None) -> FullEventStore:
        """
        Get the store for write operations based on tenant and migration state.

        Args:
            tenant_id: Tenant UUID or None

        Returns:
            FullEventStore for writing

        Raises:
            WritePausedError: If tenant is in CUTOVER_PAUSED state
        """
        if tenant_id is None:
            return self._default_store

        # Check for dual-write interceptor first
        if tenant_id in self._dual_write_interceptors:
            return self._dual_write_interceptors[tenant_id]

        # Get routing configuration
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return self._default_store

        # Route based on migration state
        state = routing.migration_state

        if state == TenantMigrationState.NORMAL:
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.BULK_COPY:
            # During bulk copy, writes still go to source
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.DUAL_WRITE:
            # Should have interceptor set; fall back to source if not
            interceptor = self._dual_write_interceptors.get(tenant_id)
            if interceptor:
                return interceptor
            logger.warning(f"Dual-write state but no interceptor for tenant {tenant_id}")
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.CUTOVER_PAUSED:
            # Writes should be paused; this shouldn't be reached normally
            # as _wait_if_paused should have blocked
            raise WritePausedError(tenant_id, self._write_pause_timeout)

        elif state == TenantMigrationState.MIGRATED:
            # Route to new store (which is now the store_id after cutover)
            return self._stores.get(routing.store_id, self._default_store)

        return self._default_store

    async def _get_read_store(self, tenant_id: UUID) -> FullEventStore:
        """
        Get the store for read operations based on tenant and migration state.

        Args:
            tenant_id: Tenant UUID

        Returns:
            FullEventStore for reading
        """
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return self._default_store

        # During migration phases, reads go to source until cutover completes
        state = routing.migration_state

        if state in (
            TenantMigrationState.NORMAL,
            TenantMigrationState.BULK_COPY,
            TenantMigrationState.DUAL_WRITE,
            TenantMigrationState.CUTOVER_PAUSED,
        ):
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.MIGRATED:
            # After migration, store_id has been updated to target
            return self._stores.get(routing.store_id, self._default_store)

        return self._default_store

    async def _wait_if_paused(self, tenant_id: UUID | None) -> None:
        """
        Wait if writes are paused for this tenant.

        Delegates to WritePauseManager for efficient waiting with
        metrics tracking.

        Args:
            tenant_id: Tenant UUID or None

        Raises:
            WritePausedError: If timeout exceeded while waiting
        """
        # WritePauseManager handles None tenant_id gracefully
        await self._write_pause_manager.wait_if_paused(tenant_id)


__all__ = ["RouterResolutionMixin"]
