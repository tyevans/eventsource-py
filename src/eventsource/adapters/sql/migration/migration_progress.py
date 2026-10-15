"""Progress and operational state updates for PostgreSQL migration repository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import text

from eventsource.adapters.sql.migration.migration_helpers import _token
from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.ports.positions import Position

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


class PostgreSQLMigrationProgressMixin:
    """Progress, metrics, and pause state operations for migration repository."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        """Update bulk copy progress."""
        attrs = {
            "migration.id": str(migration_id),
            "migration.events_copied": events_copied,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.migration_repo.update_progress", attrs):
            now = datetime.now(UTC)
            params: dict[str, Any] = {
                "id": migration_id,
                "events_copied": events_copied,
                "last_source_position_token": _token(last_source_position),
                "updated_at": now,
            }
            if last_target_position is not None:
                query = text("""
                    UPDATE tenant_migrations
                    SET events_copied = :events_copied,
                        last_source_position_token = :last_source_position_token,
                        last_target_position_token = :last_target_position_token,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params["last_target_position_token"] = _token(last_target_position)
            else:
                query = text("""
                    UPDATE tenant_migrations
                    SET events_copied = :events_copied,
                        last_source_position_token = :last_source_position_token,
                        updated_at = :updated_at
                    WHERE id = :id
                """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        """Set the total events count."""
        attrs = {
            "migration.id": str(migration_id),
            "migration.events_total": events_total,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.migration_repo.set_events_total", attrs):
            query = text("""
                UPDATE tenant_migrations
                SET events_total = :events_total,
                    updated_at = :updated_at
                WHERE id = :id
            """)
            params = {
                "id": migration_id,
                "events_total": events_total,
                "updated_at": datetime.now(UTC),
            }
            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)

    async def record_error(self, migration_id: UUID, error: str) -> None:
        """Record an error occurrence (truncated to 1000 characters)."""
        with self._tracer.span(
            "eventsource.migration_repo.record_error",
            {"migration.id": str(migration_id), ATTR_DB_SYSTEM: "postgresql"},
        ):
            now = datetime.now(UTC)
            query = text("""
                UPDATE tenant_migrations
                SET error_count = error_count + 1,
                    last_error = :error,
                    last_error_at = :error_at,
                    updated_at = :updated_at
                WHERE id = :id
            """)
            params = {
                "id": migration_id,
                "error": error[:1000],
                "error_at": now,
                "updated_at": now,
            }
            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        """Set migration pause state."""
        attrs = {
            "migration.id": str(migration_id),
            "migration.paused": paused,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.migration_repo.set_paused", attrs):
            now = datetime.now(UTC)
            if paused:
                query = text("""
                    UPDATE tenant_migrations
                    SET is_paused = TRUE,
                        paused_at = :paused_at,
                        pause_reason = :reason,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "paused_at": now,
                    "reason": reason,
                    "updated_at": now,
                }
            else:
                query = text("""
                    UPDATE tenant_migrations
                    SET is_paused = FALSE,
                        paused_at = NULL,
                        pause_reason = NULL,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {"id": migration_id, "updated_at": now}

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)


__all__ = ["PostgreSQLMigrationProgressMixin"]
