"""
TenantRoutingRepository - Data access for tenant routing configuration.

The TenantRoutingRepository manages tenant-to-store routing entries,
enabling the TenantStoreRouter to determine which store(s) should
handle operations for each tenant.

This module provides a protocol definition and PostgreSQL implementation
for managing tenant routing configuration during migrations.

Responsibilities:
    - Create and update tenant routing entries
    - Atomic routing state transitions
    - Query routing by tenant ID
    - Cache-friendly query patterns
    - Audit trail for routing changes

Database Table:
    Uses the `tenant_routing` table defined in PREREQ-002.

Routing States:
    - NORMAL: Tenant uses configured store (no migration active)
    - BULK_COPY: Route reads to source; writes to source only
    - DUAL_WRITE: Route writes through DualWriteInterceptor
    - CUTOVER_PAUSED: Block writes, await cutover completion
    - MIGRATED: Route all operations to target store

Usage:
    >>> from eventsource.migration.repositories import (
    ...     TenantRoutingRepository,
    ...     PostgreSQLTenantRoutingRepository,
    ... )
    >>>
    >>> repo = PostgreSQLTenantRoutingRepository(conn)
    >>>
    >>> # Get routing for tenant
    >>> routing = await repo.get_routing(tenant_id)
    >>>
    >>> # Update routing state
    >>> await repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE)
    >>>
    >>> # Atomic cutover
    >>> await repo.set_migration_state(
    ...     tenant_id,
    ...     TenantMigrationState.MIGRATED,
    ...     migration_id=migration.id,
    ... )

See Also:
    - Task: P1-004-routing-repository.md
    - Schema: PREREQ-002-migration-schema.md
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.migration.models import (
    TenantMigrationState,
    TenantRouting,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM, ATTR_TENANT_ID
from eventsource.repositories._connection import execute_with_connection


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


class PostgreSQLTenantRoutingRepository:
    """
    PostgreSQL implementation of TenantRoutingRepository.

    Persists tenant routing configuration to the `tenant_routing` table.
    Provides CRUD operations with optional in-memory caching for
    high-frequency routing lookups.

    The cache uses a simple time-based TTL strategy and is process-local.
    Multi-instance deployments should use short TTLs (default 5s) to
    minimize inconsistency windows.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLTenantRoutingRepository(conn)
        ...     routing = await repo.get_or_default(tenant_id, "shared")
        ...
        >>> # Update migration state
        >>> await repo.set_migration_state(
        ...     tenant_id,
        ...     TenantMigrationState.DUAL_WRITE,
        ...     migration.id,
        ... )
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        enable_cache: bool = True,
        cache_ttl_seconds: float = 5.0,
    ):
        """
        Initialize the repository.

        Args:
            conn: Database connection or engine
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
            enable_cache: Whether to cache routing lookups
            cache_ttl_seconds: Cache TTL in seconds (default 5.0)
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn
        self._enable_cache = enable_cache
        self._cache_ttl = cache_ttl_seconds
        self._cache: dict[UUID, tuple[TenantRouting, float]] = {}
        self._cache_lock = asyncio.Lock()

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """
        Get routing configuration for a tenant.

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

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"tenant_id": tenant_id})
                row = result.fetchone()

            if row is None:
                return None

            routing = self._row_to_routing(row)

            # Update cache
            if self._enable_cache:
                await self._set_cache(tenant_id, routing)

            return routing

    async def get_or_default(
        self,
        tenant_id: UUID,
        default_store_id: str,
    ) -> TenantRouting:
        """
        Get routing configuration, creating default if not exists.

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
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

            routing = self._row_to_routing(row)

            if self._enable_cache:
                await self._set_cache(tenant_id, routing)

            return routing

    async def set_routing(
        self,
        tenant_id: UUID,
        store_id: str,
    ) -> None:
        """
        Set or update the store for a tenant.

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
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
        """
        Update the migration state for routing decisions.

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
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

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        """
        Reset migration state to NORMAL.

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

    async def list_by_state(
        self,
        state: TenantMigrationState,
    ) -> list[TenantRouting]:
        """
        List tenants in a specific migration state.

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

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"state": state.value})
                rows = result.fetchall()

            return [self._row_to_routing(row) for row in rows]

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """
        List tenants routed to a specific store.

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

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"store_id": store_id})
                rows = result.fetchall()

            return [self._row_to_routing(row) for row in rows]

    async def delete_routing(self, tenant_id: UUID) -> bool:
        """
        Delete routing configuration for a tenant.

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, {"tenant_id": tenant_id})

            # Invalidate cache
            await self._invalidate_cache(tenant_id)

            return result.rowcount > 0

    # =========================================================================
    # Cache management
    # =========================================================================

    async def _get_from_cache(self, tenant_id: UUID) -> TenantRouting | None:
        """
        Get routing from cache if not expired.

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
        """
        Add routing to cache.

        Args:
            tenant_id: Tenant UUID
            routing: TenantRouting instance to cache
        """
        async with self._cache_lock:
            self._cache[tenant_id] = (routing, time.monotonic())

    async def _invalidate_cache(self, tenant_id: UUID) -> None:
        """
        Remove routing from cache.

        Args:
            tenant_id: Tenant UUID
        """
        async with self._cache_lock:
            self._cache.pop(tenant_id, None)

    async def clear_cache(self) -> None:
        """
        Clear all cached routing entries.

        Useful for testing or when a bulk update has occurred.
        """
        async with self._cache_lock:
            self._cache.clear()

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _row_to_routing(self, row: Sequence[Any]) -> TenantRouting:
        """
        Convert database row to TenantRouting instance.

        Args:
            row: Database row tuple from SELECT query

        Returns:
            TenantRouting instance
        """
        return TenantRouting(
            tenant_id=row[0],
            store_id=row[1],
            migration_state=TenantMigrationState(row[2]),
            active_migration_id=row[3],
            created_at=row[4],
            updated_at=row[5],
        )


# Type alias for backwards compatibility
TenantRoutingRepositoryProtocol = TenantRoutingRepository
