"""Query operations mixin for PostgreSQL migration repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import text

from eventsource.adapters.sql.migration.migration_helpers import row_to_migration
from eventsource.observability.attributes import ATTR_DB_SYSTEM, ATTR_TENANT_ID
from eventsource.ports.migration.models import Migration

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from eventsource.observability import Tracer


def _get_sql_connection() -> Any:
    import sys

    mod = sys.modules.get("eventsource.adapters.sql.migration.migration")
    if mod is not None and hasattr(mod, "sql_connection"):
        return mod.sql_connection
    from eventsource.adapters._sql.connection import sql_connection

    return sql_connection


class PostgreSQLMigrationQueryMixin:
    """Read and query operations for PostgreSQLMigrationRepository."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    async def get(self, migration_id: UUID) -> Migration | None:
        """Get a migration by ID.

        Args:
            migration_id: UUID of the migration

        Returns:
            Migration instance or None if not found
        """
        with self._tracer.span(
            "eventsource.migration_repo.get",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position_token, last_target_position_token,
                    started_at, bulk_copy_started_at, bulk_copy_completed_at,
                    dual_write_started_at, cutover_started_at, completed_at,
                    config, error_count, last_error, last_error_at,
                    is_paused, paused_at, pause_reason,
                    created_at, updated_at, created_by
                FROM tenant_migrations
                WHERE id = :id
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": migration_id})
                row = result.fetchone()

            if row is None:
                return None

            return row_to_migration(row)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        """Get the active migration for a tenant.

        Returns the migration if one is active (not in terminal phase).
        Terminal phases are: COMPLETED, ABORTED, FAILED.

        Args:
            tenant_id: Tenant UUID

        Returns:
            Active Migration instance or None
        """
        with self._tracer.span(
            "eventsource.migration_repo.get_by_tenant",
            {
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position_token, last_target_position_token,
                    started_at, bulk_copy_started_at, bulk_copy_completed_at,
                    dual_write_started_at, cutover_started_at, completed_at,
                    config, error_count, last_error, last_error_at,
                    is_paused, paused_at, pause_reason,
                    created_at, updated_at, created_by
                FROM tenant_migrations
                WHERE tenant_id = :tenant_id
                  AND phase NOT IN ('completed', 'aborted', 'failed')
                LIMIT 1
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"tenant_id": tenant_id})
                row = result.fetchone()

            if row is None:
                return None

            return row_to_migration(row)

    async def list_active(self) -> list[Migration]:
        """List all active migrations.

        Returns all migrations that are not in a terminal phase
        (COMPLETED, ABORTED, FAILED), ordered by creation time.

        Returns:
            List of active Migration instances
        """
        with self._tracer.span(
            "eventsource.migration_repo.list_active",
            {ATTR_DB_SYSTEM: "postgresql"},
        ):
            query = text("""
                SELECT
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position_token, last_target_position_token,
                    started_at, bulk_copy_started_at, bulk_copy_completed_at,
                    dual_write_started_at, cutover_started_at, completed_at,
                    config, error_count, last_error, last_error_at,
                    is_paused, paused_at, pause_reason,
                    created_at, updated_at, created_by
                FROM tenant_migrations
                WHERE phase NOT IN ('completed', 'aborted', 'failed')
                ORDER BY created_at ASC
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {})
                rows = result.fetchall()

            return [row_to_migration(row) for row in rows]


__all__ = ["PostgreSQLMigrationQueryMixin"]
