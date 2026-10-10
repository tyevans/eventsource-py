"""Write and mutation operations for PostgreSQL position mapping repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import text

from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.ports.migration.models import PositionMapping

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from eventsource.observability import Tracer


def _get_sql_connection() -> Any:
    import sys

    mod = sys.modules.get("eventsource.adapters.sql.migration.position_mapping")
    if mod is not None and hasattr(mod, "sql_connection"):
        return mod.sql_connection
    from eventsource.adapters._sql.connection import sql_connection

    return sql_connection


class PostgreSQLPositionMappingMutationMixin:
    """Create, batch create, and deletion operations for position mapping repository."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    async def create(self, mapping: PositionMapping) -> int:
        """Create a new position mapping.

        Inserts a single position mapping record. Use create_batch for
        bulk operations during bulk copy phase.

        Args:
            mapping: PositionMapping instance to persist

        Returns:
            The database ID of the created mapping
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.create",
            {
                "migration.id": str(mapping.migration_id),
                "source_position": mapping.source_position.to_str(),
                "target_position": mapping.target_position.to_str(),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                ) VALUES (
                    :migration_id, :source_position_token, :target_position_token,
                    :event_id, :mapped_at
                )
                RETURNING id
            """)

            params = {
                "migration_id": mapping.migration_id,
                "source_position_token": mapping.source_position.to_str(),
                "target_position_token": mapping.target_position.to_str(),
                "event_id": mapping.event_id,
                "mapped_at": mapping.mapped_at,
            }

            async with _get_sql_connection()(self._conn, write=True) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()

            if row is None:
                raise RuntimeError("Failed to create position mapping - no row returned")
            return int(row[0])

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        """Create multiple position mappings in a single transaction.

        Uses PostgreSQL's multi-row INSERT for efficiency during bulk copy.
        This is significantly faster than individual inserts when processing
        thousands of events.

        Args:
            mappings: List of PositionMapping instances to persist

        Returns:
            Number of mappings created
        """
        if not mappings:
            return 0

        with self._tracer.span(
            "eventsource.position_mapping_repo.create_batch",
            {
                "batch_size": len(mappings),
                "migration.id": str(mappings[0].migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            values_list: list[str] = []
            params: dict[str, Any] = {}

            for i, mapping in enumerate(mappings):
                values_list.append(
                    f"(:migration_id_{i}, :source_position_token_{i}, "
                    f":target_position_token_{i}, :event_id_{i}, :mapped_at_{i})"
                )
                params[f"migration_id_{i}"] = mapping.migration_id
                params[f"source_position_token_{i}"] = mapping.source_position.to_str()
                params[f"target_position_token_{i}"] = mapping.target_position.to_str()
                params[f"event_id_{i}"] = mapping.event_id
                params[f"mapped_at_{i}"] = mapping.mapped_at

            values_sql = ", ".join(values_list)

            query = text(f"""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                ) VALUES {values_sql}
                ON CONFLICT (migration_id, source_position_token) DO NOTHING
            """)  # nosec B608 - parameterized query construction

            async with _get_sql_connection()(self._conn, write=True) as conn:
                result = await conn.execute(query, params)

            return int(result.rowcount)

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """Delete all mappings for a migration.

        Called during migration cleanup or when re-starting a failed migration.
        Uses the index on migration_id for efficient bulk deletion.

        Args:
            migration_id: UUID of the migration

        Returns:
            Number of mappings deleted
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.delete_by_migration",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                DELETE FROM migration_position_mappings
                WHERE migration_id = :migration_id
            """)

            async with _get_sql_connection()(self._conn, write=True) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})

            return int(result.rowcount)


__all__ = ["PostgreSQLPositionMappingMutationMixin"]
