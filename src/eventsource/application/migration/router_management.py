"""
Store registry, dual-write interceptor, and write pause management mixin.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.write_pause import PauseMetrics, WritePauseManager
from eventsource.ports import FullEventStore

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class RouterManagementMixin:
    """Mixin for managing stores, interceptors, and pause states."""

    _stores: dict[str, FullEventStore]
    _default_store_id: str
    _dual_write_interceptors: dict[UUID, FullEventStore]
    _write_pause_manager: WritePauseManager

    def register_store(self, store_id: str, store: FullEventStore) -> None:
        """
        Register a store for routing.

        Args:
            store_id: Unique identifier for the store
            store: FullEventStore instance
        """
        self._stores[store_id] = store
        logger.debug(f"Registered store: {store_id}")

    def unregister_store(self, store_id: str) -> None:
        """
        Unregister a store.

        Args:
            store_id: Store identifier to remove

        Raises:
            ValueError: If attempting to unregister default store
        """
        if store_id == self._default_store_id:
            raise ValueError("Cannot unregister default store")
        self._stores.pop(store_id, None)
        logger.debug(f"Unregistered store: {store_id}")

    def get_store(self, store_id: str) -> FullEventStore | None:
        """
        Get a registered store by ID.

        Args:
            store_id: Store identifier

        Returns:
            FullEventStore instance or None if not registered
        """
        return self._stores.get(store_id)

    def list_stores(self) -> list[str]:
        """
        List all registered store IDs.

        Returns:
            List of store identifiers
        """
        return list(self._stores.keys())

    def set_dual_write_interceptor(
        self,
        tenant_id: UUID,
        interceptor: FullEventStore,
    ) -> None:
        """
        Set dual-write interceptor for a tenant.

        Called by MigrationCoordinator when entering dual-write phase.

        Args:
            tenant_id: Tenant UUID
            interceptor: DualWriteInterceptor instance
        """
        self._dual_write_interceptors[tenant_id] = interceptor
        logger.debug(f"Set dual-write interceptor for tenant {tenant_id}")

    def clear_dual_write_interceptor(self, tenant_id: UUID) -> None:
        """
        Remove dual-write interceptor for a tenant.

        Called after cutover completes or migration aborts.

        Args:
            tenant_id: Tenant UUID
        """
        self._dual_write_interceptors.pop(tenant_id, None)
        logger.debug(f"Cleared dual-write interceptor for tenant {tenant_id}")

    def has_dual_write_interceptor(self, tenant_id: UUID) -> bool:
        """
        Check if tenant has a dual-write interceptor set.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if interceptor is set
        """
        return tenant_id in self._dual_write_interceptors

    async def pause_writes(self, tenant_id: UUID) -> bool:
        """
        Pause writes for a tenant during cutover.

        Writers will block until resume_writes() is called or timeout.
        This operation is idempotent - calling it multiple times for the
        same tenant has no additional effect.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if a new pause was created, False if already paused.
        """
        return await self._write_pause_manager.pause_writes(tenant_id)

    async def resume_writes(self, tenant_id: UUID) -> PauseMetrics | None:
        """
        Resume writes for a tenant after cutover.

        Unblocks any waiting writers and returns metrics about the pause.

        Args:
            tenant_id: Tenant UUID

        Returns:
            PauseMetrics if tenant was paused, None if not paused.
        """
        return await self._write_pause_manager.resume_writes(tenant_id)

    def is_paused(self, tenant_id: UUID) -> bool:
        """
        Check if writes are paused for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if writes are paused
        """
        return self._write_pause_manager.is_paused(tenant_id)

    @property
    def write_pause_manager(self) -> WritePauseManager:
        """
        Get the WritePauseManager for advanced pause operations.

        Use this for accessing advanced features like:
        - Pause metrics history
        - Detailed pause state
        - Force resume all

        Returns:
            The WritePauseManager instance.
        """
        return self._write_pause_manager


__all__ = ["RouterManagementMixin"]
