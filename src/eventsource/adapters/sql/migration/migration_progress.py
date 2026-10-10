"""Progress and operational state updates for PostgreSQL migration repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

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
        """Update bulk copy progress.

        Updates the progress tracking fields for the migration, including
        the number of events copied and the last processed positions.

        Args:
            migration_id: UUID of the migration
            events_copied: Total events copied so far
            last_source_position: Last source position processed (opaque
                token; None when nothing has been copied yet)
            last_target_position: Last target position written (optional)
        """
        with self._tracer.span(
            "eventsource.migration_repo.update_progress",
            {
                "migration.id": str(migration_id),
                "migration.events_copied": events_copied,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            if last_target_position is not None:
                query = text("""
                    UPDATE tenant_migrations
                    SET events_copied = :events_copied,
                        last_source_position_token = :last_source_position_token,
                        last_target_position_token = :last_target_position_token,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "events_copied": events_copied,
                    "last_source_position_token": _token(last_source_position),
                    "last_target_position_token": _token(last_target_position),
                    "updated_at": now,
                }
            else:
                query = text("""
                    UPDATE tenant_migrations
                    SET events_copied = :events_copied,
                        last_source_position_token = :last_source_position_token,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "events_copied": events_copied,
                    "last_source_position_token": _token(last_source_position),
                    "updated_at": now,
                }

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)

    async def set_events_total(
        self,
        migration_id: UUID,
        events_total: int,
    ) -> None:
        """Set the total events count.

        Sets the total number of events to be migrated. This is typically
        determined at the start of the bulk copy phase.

        Args:
            migration_id: UUID of the migration
            events_total: Total events to migrate
        """
        with self._tracer.span(
            "eventsource.migration_repo.set_events_total",
            {
                "migration.id": str(migration_id),
                "migration.events_total": events_total,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                UPDATE tenant_migrations
                SET events_total = :events_total,
                    updated_at = :updated_at
                WHERE id = :id
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(
                    query,
                    {
                        "id": migration_id,
                        "events_total": events_total,
                        "updated_at": datetime.now(UTC),
                    },
                )

    async def record_error(
        self,
        migration_id: UUID,
        error: str,
    ) -> None:
        """Record an error occurrence.

        Increments the error count and stores the error message.
        Error messages are truncated to 1000 characters.

        Args:
            migration_id: UUID of the migration
            error: Error message
        """
        with self._tracer.span(
            "eventsource.migration_repo.record_error",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
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

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(
                    query,
                    {
                        "id": migration_id,
                        "error": error[:1000],
                        "error_at": now,
                        "updated_at": now,
                    },
                )

    async def set_paused(
        self,
        migration_id: UUID,
        paused: bool,
        reason: str | None = None,
    ) -> None:
        """Set migration pause state.

        Pauses or resumes a migration. When pausing, stores the reason
        and timestamp. When resuming, clears the pause-related fields.

        Args:
            migration_id: UUID of the migration
            paused: Whether to pause or resume
            reason: Reason for pausing (if paused=True)
        """
        with self._tracer.span(
            "eventsource.migration_repo.set_paused",
            {
                "migration.id": str(migration_id),
                "migration.paused": paused,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
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
                params = {
                    "id": migration_id,
                    "updated_at": now,
                }

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)


__all__ = ["PostgreSQLMigrationProgressMixin"]
