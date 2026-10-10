"""Cache management mixin for PostgreSQLTenantRoutingRepository."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.ports.migration.models import TenantRouting

if TYPE_CHECKING:
    pass


class PostgreSQLTenantRoutingCacheMixin:
    """Mixin providing in-memory caching capabilities for routing repository."""

    _enable_cache: bool
    _cache_ttl: float
    _cache: dict[UUID, tuple[TenantRouting, float]]
    _cache_lock: asyncio.Lock

    async def _get_from_cache(self, tenant_id: UUID) -> TenantRouting | None:
        """Get routing from cache if not expired.

        Args:
            tenant_id: Tenant UUID

        Returns:
            TenantRouting if cached and not expired, None otherwise
        """
        async with self._cache_lock:
            if tenant_id not in self._cache:
                return None

            routing, cached_at = self._cache[tenant_id]
            if time.monotonic() - cached_at > self._cache_ttl:
                del self._cache[tenant_id]
                return None

            return routing

    async def _set_cache(self, tenant_id: UUID, routing: TenantRouting) -> None:
        """Add routing to cache."""
        async with self._cache_lock:
            self._cache[tenant_id] = (routing, time.monotonic())

    async def _invalidate_cache(self, tenant_id: UUID) -> None:
        """Remove routing from cache."""
        async with self._cache_lock:
            self._cache.pop(tenant_id, None)

    async def clear_cache(self) -> None:
        """Clear all cached routing entries."""
        async with self._cache_lock:
            self._cache.clear()


__all__ = ["PostgreSQLTenantRoutingCacheMixin"]
