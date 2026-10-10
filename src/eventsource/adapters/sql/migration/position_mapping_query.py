"""Read and query operations for PostgreSQL position mapping repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import text

from eventsource.adapters.sql.migration.position_mapping_helpers import row_to_mapping
from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position

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


class PostgreSQLPositionMappingQueryMixin:
    """Read and lookup operations for position mapping repository."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    if TYPE_CHECKING:

        async def _get_by_ordinal(
            self, migration_id: UUID, ordinal: int
        ) -> PositionMapping | None: ...
        async def _find_first_ordinal_gte(
            self,
            migration_id: UUID,
            source_position: Position,
            total: int,
        ) -> int | None: ...
        async def _find_last_ordinal_lte(
            self,
            migration_id: UUID,
            source_position: Position,
            total: int,
        ) -> int | None: ...

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """Get a position mapping by its database ID.

        Args:
            mapping_id: Database ID of the mapping

        Returns:
            PositionMapping instance or None if not found
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.get",
            {
                "mapping.id": mapping_id,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE id = :id
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": mapping_id})
                row = result.fetchone()

            if row is None:
                return None

            return row_to_mapping(row)

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """Find mapping by exact source position.

        Uses the unique index on (migration_id, source_position_token) for
        efficient O(log n) lookup.

        Args:
            migration_id: UUID of the migration
            source_position: Exact source position to find

        Returns:
            PositionMapping instance or None if not found
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.find_by_source_position",
            {
                "migration.id": str(migration_id),
                "source_position": source_position.to_str(),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND source_position_token = :source_position_token
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "source_position_token": source_position.to_str(),
                    },
                )
                row = result.fetchone()

            if row is None:
                return None

            return row_to_mapping(row)

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: Position,
    ) -> PositionMapping | None:
        """Find mapping by exact target position.

        Args:
            migration_id: UUID of the migration
            target_position: Exact target position to find

        Returns:
            PositionMapping instance or None if not found
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.find_by_target_position",
            {
                "migration.id": str(migration_id),
                "target_position": target_position.to_str(),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND target_position_token = :target_position_token
                LIMIT 1
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "target_position_token": target_position.to_str(),
                    },
                )
                row = result.fetchone()

            if row is None:
                return None

            return row_to_mapping(row)

    async def find_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """Find mapping by event ID.

        Uses the index on event_id for efficient lookup.

        Args:
            migration_id: UUID of the migration
            event_id: UUID of the event

        Returns:
            PositionMapping instance or None if not found
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.find_by_event_id",
            {
                "migration.id": str(migration_id),
                "event.id": str(event_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND event_id = :event_id
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "event_id": event_id,
                    },
                )
                row = result.fetchone()

            if row is None:
                return None

            return row_to_mapping(row)

    async def list_by_migration(
        self,
        migration_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PositionMapping]:
        """List mappings for a migration with pagination.

        Results are ordered by `id` ascending, which is source-position
        order under the class's monotonicity precondition.

        Args:
            migration_id: UUID of the migration
            limit: Maximum number of results (default 100)
            offset: Number of results to skip (default 0)

        Returns:
            List of PositionMapping instances
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.list_by_migration",
            {
                "migration.id": str(migration_id),
                "limit": limit,
                "offset": offset,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                ORDER BY id ASC
                LIMIT :limit OFFSET :offset
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "limit": limit,
                        "offset": offset,
                    },
                )
                rows = result.fetchall()

            return [row_to_mapping(row) for row in rows]

    async def list_in_source_range(
        self,
        migration_id: UUID,
        start_position: Position,
        end_position: Position,
    ) -> list[PositionMapping]:
        """List mappings within a source position range.

        Returns all mappings where start_position <= source_position <= end_position.

        Args:
            migration_id: UUID of the migration
            start_position: Start of source position range (inclusive)
            end_position: End of source position range (inclusive)

        Returns:
            List of PositionMapping instances ordered by source_position
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.list_in_source_range",
            {
                "migration.id": str(migration_id),
                "start_position": start_position.to_str(),
                "end_position": end_position.to_str(),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            total = await self.count_by_migration(migration_id)
            lower_ordinal = await self._find_first_ordinal_gte(migration_id, start_position, total)
            upper_ordinal = await self._find_last_ordinal_lte(migration_id, end_position, total)

            if lower_ordinal is None or upper_ordinal is None or lower_ordinal > upper_ordinal:
                return []

            count = upper_ordinal - lower_ordinal + 1
            return await self.list_by_migration(migration_id, limit=count, offset=lower_ordinal)

    async def count_by_migration(self, migration_id: UUID) -> int:
        """Count total mappings for a migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            Number of mappings
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.count_by_migration",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT COUNT(*)
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
            """)

            async with _get_sql_connection()(self._conn, write=False) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})
                row = result.fetchone()

            return row[0] if row else 0

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[Position, Position] | None:
        """Get the first and last mapping's source positions for a migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            Tuple of (first_source_position, last_source_position) or None
            if no mappings exist
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.get_position_bounds",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            total = await self.count_by_migration(migration_id)
            if total == 0:
                return None

            first = await self._get_by_ordinal(migration_id, 0)
            last = await self._get_by_ordinal(migration_id, total - 1)

            if first is None or last is None:
                return None

            return (first.source_position, last.source_position)


__all__ = ["PostgreSQLPositionMappingQueryMixin"]
