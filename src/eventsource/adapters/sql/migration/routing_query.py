"""Query operations mixin for PostgreSQLTenantRoutingRepository."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters.sql.migration.routing_helpers import (
    _get_sql_connection,
    row_to_routing,
)
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM, ATTR_TENANT_ID
from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)

if TYPE_CHECKING:
    pass


class PostgreSQLTenantRoutingQueryMixin:
    """Mixin providing query operations for tenant routing."""

    _tracer: Tracer
    _conn: AsyncConnection | AsyncEngine
    _enable_cache: bool

    async def _get_from_cache(self, tenant_id: UUID) -> TenantRouting | None: ...
    async def _set_cache(self, tenant_id: UUID, routing: TenantRouting) -> None: ...

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """Get routing configuration for a tenant.

        Checks the cache first (if enabled) before querying the database.
        Cache hits are recorded in the span for observability.

        Args:
            tenant_id: Tenant UUID

        Returns:
            TenantRouting instance or None if not configured
        """
        with self._tracer.span(
            "eventsource.routing_repo.get_routing",
            {
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ) as span:
            # Check cache first
            if self._enable_cache:
                cached = await self._get_from_cache(tenant_id)
                if cached is not None:
                    if span:
                        span.set_attribute("cache.hit", True)
                    return cached
                if span:
                    span.set_attribute("cache.hit", False)

            query = text("""
                SELECT
                    tenant_id, store_id, migration_state,
                    active_migration_id, created_at, updated_at
                FROM tenant_routing
                WHERE tenant_id = :tenant_id
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"tenant_id": tenant_id})
                row = result.fetchone()

            if row is None:
                return None

            routing = row_to_routing(row)

            # Update cache
            if self._enable_cache:
                await self._set_cache(tenant_id, routing)

            return routing

    async def list_by_state(
        self,
        state: TenantMigrationState,
    ) -> list[TenantRouting]:
        """List tenants in a specific migration state.

        Results are ordered by updated_at DESC to show most recently
        updated tenants first.

        Args:
            state: Migration state to filter by

        Returns:
            List of TenantRouting instances
        """
        with self._tracer.span(
            "eventsource.routing_repo.list_by_state",
            {
                "migration_state": state.value,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    tenant_id, store_id, migration_state,
                    active_migration_id, created_at, updated_at
                FROM tenant_routing
                WHERE migration_state = :state
                ORDER BY updated_at DESC
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"state": state.value})
                rows = result.fetchall()

            return [row_to_routing(row) for row in rows]

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """List tenants routed to a specific store.

        Results are ordered by created_at ASC to show oldest tenants first.
        This is useful for planning migrations as older tenants may have
        more historical data.

        Args:
            store_id: Store identifier

        Returns:
            List of TenantRouting instances
        """
        with self._tracer.span(
            "eventsource.routing_repo.list_by_store",
            {
                "store_id": store_id,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    tenant_id, store_id, migration_state,
                    active_migration_id, created_at, updated_at
                FROM tenant_routing
                WHERE store_id = :store_id
                ORDER BY created_at ASC
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"store_id": store_id})
                rows = result.fetchall()

            return [row_to_routing(row) for row in rows]


__all__ = ["PostgreSQLTenantRoutingQueryMixin"]
