"""Mutation operations mixin for PostgreSQLTenantRoutingRepository."""

from __future__ import annotations

from datetime import UTC, datetime
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


class PostgreSQLTenantRoutingMutationMixin:
    """Mixin providing mutation operations for tenant routing."""

    _tracer: Tracer
    _conn: AsyncConnection | AsyncEngine
    _enable_cache: bool

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None: ...
    async def _set_cache(self, tenant_id: UUID, routing: TenantRouting) -> None: ...
    async def _invalidate_cache(self, tenant_id: UUID) -> None: ...

    async def get_or_default(
        self,
        tenant_id: UUID,
        default_store_id: str,
    ) -> TenantRouting:
        """Get routing configuration, creating default if not exists.

        Uses PostgreSQL's INSERT ... ON CONFLICT DO NOTHING with
        RETURNING to atomically create or fetch existing routing.

        Args:
            tenant_id: Tenant UUID
            default_store_id: Default store ID if not configured

        Returns:
            TenantRouting instance (existing or newly created)
        """
        with self._tracer.span(
            "eventsource.routing_repo.get_or_default",
            {
                ATTR_TENANT_ID: str(tenant_id),
                "store_id": default_store_id,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            # Check for existing routing first
            existing = await self.get_routing(tenant_id)
            if existing is not None:
                return existing

            # Create default routing
            now = datetime.now(UTC)

            query = text("""
                INSERT INTO tenant_routing (
                    tenant_id, store_id, migration_state,
                    created_at, updated_at
                ) VALUES (
                    :tenant_id, :store_id, :state,
                    :created_at, :updated_at
                )
                ON CONFLICT (tenant_id) DO NOTHING
                RETURNING tenant_id, store_id, migration_state,
                          active_migration_id, created_at, updated_at
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                result = await conn.execute(
                    query,
                    {
                        "tenant_id": tenant_id,
                        "store_id": default_store_id,
                        "state": TenantMigrationState.NORMAL.value,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                row = result.fetchone()

            # If ON CONFLICT hit, fetch existing
            if row is None:
                # Another process inserted concurrently, fetch it
                existing_routing = await self.get_routing(tenant_id)
                if existing_routing is None:
                    # Should not happen, but handle defensively
                    raise RuntimeError(f"Failed to get or create routing for tenant {tenant_id}")
                return existing_routing

            routing = row_to_routing(row)

            if self._enable_cache:
                await self._set_cache(tenant_id, routing)

            return routing

    async def set_routing(
        self,
        tenant_id: UUID,
        store_id: str,
    ) -> None:
        """Set or update the store for a tenant.

        Uses UPSERT semantics. If the tenant already has routing,
        the store_id is updated and migration_state is reset to NORMAL.
        If no routing exists, creates a new one with NORMAL state.

        Args:
            tenant_id: Tenant UUID
            store_id: Target store identifier
        """
        with self._tracer.span(
            "eventsource.routing_repo.set_routing",
            {
                ATTR_TENANT_ID: str(tenant_id),
                "store_id": store_id,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            query = text("""
                INSERT INTO tenant_routing (
                    tenant_id, store_id, migration_state,
                    created_at, updated_at
                ) VALUES (
                    :tenant_id, :store_id, :state,
                    :created_at, :updated_at
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET store_id = EXCLUDED.store_id,
                    updated_at = EXCLUDED.updated_at
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(
                    query,
                    {
                        "tenant_id": tenant_id,
                        "store_id": store_id,
                        "state": TenantMigrationState.NORMAL.value,
                        "created_at": now,
                        "updated_at": now,
                    },
                )

            # Invalidate cache
            await self._invalidate_cache(tenant_id)

    async def set_migration_state(
        self,
        tenant_id: UUID,
        state: TenantMigrationState,
        migration_id: UUID | None = None,
    ) -> None:
        """Update the migration state for routing decisions.

        Updates only the migration_state and active_migration_id fields.
        The routing must already exist for this tenant.

        Args:
            tenant_id: Tenant UUID
            state: New migration state
            migration_id: Active migration ID (if applicable)
        """
        with self._tracer.span(
            "eventsource.routing_repo.set_migration_state",
            {
                ATTR_TENANT_ID: str(tenant_id),
                "migration_state": state.value,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            query = text("""
                UPDATE tenant_routing
                SET migration_state = :state,
                    active_migration_id = :migration_id,
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(
                    query,
                    {
                        "tenant_id": tenant_id,
                        "state": state.value,
                        "migration_id": migration_id,
                        "updated_at": now,
                    },
                )

            # Invalidate cache
            await self._invalidate_cache(tenant_id)

    async def switch_routing(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.MIGRATED,
        migration_id: UUID | None = None,
    ) -> None:
        """Atomically update both store_id and migration_state in a single transaction.

        Args:
            tenant_id: Tenant UUID
            store_id: Target store identifier
            state: Target migration state (default MIGRATED)
            migration_id: Active migration ID (if applicable)
        """
        with self._tracer.span(
            "eventsource.routing_repo.switch_routing",
            {
                ATTR_TENANT_ID: str(tenant_id),
                "store_id": store_id,
                "migration_state": state.value,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            query = text("""
                INSERT INTO tenant_routing (
                    tenant_id, store_id, migration_state,
                    active_migration_id, created_at, updated_at
                ) VALUES (
                    :tenant_id, :store_id, :state,
                    :migration_id, :created_at, :updated_at
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET store_id = EXCLUDED.store_id,
                    migration_state = EXCLUDED.migration_state,
                    active_migration_id = EXCLUDED.active_migration_id,
                    updated_at = EXCLUDED.updated_at
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(
                    query,
                    {
                        "tenant_id": tenant_id,
                        "store_id": store_id,
                        "state": state.value,
                        "migration_id": migration_id,
                        "created_at": now,
                        "updated_at": now,
                    },
                )

            # Invalidate cache
            await self._invalidate_cache(tenant_id)

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        """Reset migration state to NORMAL.

        Clears the active_migration_id and sets migration_state to NORMAL.
        This is typically called after migration completes or is aborted.

        Args:
            tenant_id: Tenant UUID
        """
        await self.set_migration_state(
            tenant_id,
            TenantMigrationState.NORMAL,
            migration_id=None,
        )

    async def delete_routing(self, tenant_id: UUID) -> bool:
        """Delete routing configuration for a tenant.

        This is typically used during testing or tenant cleanup.
        In production, routing entries are usually kept for audit purposes.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if a row was deleted, False if no row existed
        """
        with self._tracer.span(
            "eventsource.routing_repo.delete_routing",
            {
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                DELETE FROM tenant_routing
                WHERE tenant_id = :tenant_id
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                result = await conn.execute(query, {"tenant_id": tenant_id})

            # Invalidate cache
            await self._invalidate_cache(tenant_id)

            return bool(result.rowcount and result.rowcount > 0)


__all__ = ["PostgreSQLTenantRoutingMutationMixin"]
