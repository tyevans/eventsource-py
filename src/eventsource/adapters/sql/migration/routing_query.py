"""Query operations mixin for PostgreSQLTenantRoutingRepository."""

from __future__ import annotations

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

_SELECT_ROUTING = (
    "SELECT tenant_id, store_id, migration_state, active_migration_id, created_at, updated_at "
    "FROM tenant_routing"
)


class PostgreSQLTenantRoutingQueryMixin:
    """Mixin providing query operations for tenant routing."""

    _tracer: Tracer
    _conn: AsyncConnection | AsyncEngine
    _enable_cache: bool

    async def _get_from_cache(self, tenant_id: UUID) -> TenantRouting | None: ...
    async def _set_cache(self, tenant_id: UUID, routing: TenantRouting) -> None: ...

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """Get routing configuration for a tenant, checking cache first when enabled."""
        attrs = {ATTR_TENANT_ID: str(tenant_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.routing_repo.get_routing", attrs) as span:
            if self._enable_cache:
                cached = await self._get_from_cache(tenant_id)
                if cached is not None:
                    if span:
                        span.set_attribute("cache.hit", True)
                    return cached
                if span:
                    span.set_attribute("cache.hit", False)

            query = text(f"{_SELECT_ROUTING} WHERE tenant_id = :tenant_id")  # nosec B608
            async with _get_sql_connection()(self._conn, write=False) as conn:
                row = (await conn.execute(query, {"tenant_id": tenant_id})).fetchone()

            if row is None:
                return None

            routing = row_to_routing(row)
            if self._enable_cache:
                await self._set_cache(tenant_id, routing)
            return routing

    async def list_by_state(self, state: TenantMigrationState) -> list[TenantRouting]:
        """List tenants in a specific migration state ordered by updated_at DESC."""
        attrs = {"migration_state": state.value, ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.routing_repo.list_by_state", attrs):
            query = text(
                f"{_SELECT_ROUTING} WHERE migration_state = :state ORDER BY updated_at DESC"  # nosec B608
            )
            async with _get_sql_connection()(self._conn, write=False) as conn:
                rows = (await conn.execute(query, {"state": state.value})).fetchall()
            return [row_to_routing(row) for row in rows]

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """List tenants routed to a specific store ordered by created_at ASC."""
        attrs = {"store_id": store_id, ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.routing_repo.list_by_store", attrs):
            query = text(
                f"{_SELECT_ROUTING} WHERE store_id = :store_id ORDER BY created_at ASC"  # nosec B608
            )
            async with _get_sql_connection()(self._conn, write=False) as conn:
                rows = (await conn.execute(query, {"store_id": store_id})).fetchall()
            return [row_to_routing(row) for row in rows]


__all__ = ["PostgreSQLTenantRoutingQueryMixin"]
