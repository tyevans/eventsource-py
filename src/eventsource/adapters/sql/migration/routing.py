"""TenantRoutingRepository - Data access for tenant routing configuration.

Manages tenant-to-store routing entries and migration state transitions
in PostgreSQL with caching support.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.sql.migration.routing_cache import (
    PostgreSQLTenantRoutingCacheMixin,
)
from eventsource.adapters.sql.migration.routing_helpers import (
    row_to_routing,
)
from eventsource.adapters.sql.migration.routing_mutation import (
    PostgreSQLTenantRoutingMutationMixin,
)
from eventsource.adapters.sql.migration.routing_query import (
    PostgreSQLTenantRoutingQueryMixin,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.ports.migration.models import (
    TenantRouting,
)


class PostgreSQLTenantRoutingRepository(
    PostgreSQLTenantRoutingCacheMixin,
    PostgreSQLTenantRoutingQueryMixin,
    PostgreSQLTenantRoutingMutationMixin,
):
    """PostgreSQL implementation of TenantRoutingRepository.

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
        """Initialize the repository.

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

    def _row_to_routing(self, row: Sequence[Any]) -> TenantRouting:
        """Convert database row tuple to TenantRouting instance."""
        return row_to_routing(row)


__all__ = [
    "PostgreSQLTenantRoutingRepository",
    "sql_connection",
]
