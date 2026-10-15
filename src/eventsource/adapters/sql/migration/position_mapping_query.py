"""Read and query operations for PostgreSQL position mapping repository."""

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

_SELECT_COLUMNS = (
    "id, migration_id, source_position_token, target_position_token, event_id, mapped_at"
)


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
            self, migration_id: UUID, source_position: Position, total: int
        ) -> int | None: ...
        async def _find_last_ordinal_lte(
            self, migration_id: UUID, source_position: Position, total: int
        ) -> int | None: ...

    async def _fetch_one_mapping(
        self, query_str: str, params: dict[str, Any]
    ) -> PositionMapping | None:
        async with _get_sql_connection()(self._conn, write=False) as conn:
            result = await conn.execute(text(query_str), params)
            row = result.fetchone()
        return row_to_mapping(row) if row is not None else None

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """Get a position mapping by its database ID."""
        attrs = {"mapping.id": mapping_id, ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.position_mapping_repo.get", attrs):
            sql = f"SELECT {_SELECT_COLUMNS} FROM migration_position_mappings WHERE id = :id"  # nosec B608
            return await self._fetch_one_mapping(sql, {"id": mapping_id})

    async def find_by_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        """Find mapping by exact source position via unique index."""
        tok = source_position.to_str()
        attrs = {
            "migration.id": str(migration_id),
            "source_position": tok,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.position_mapping_repo.find_by_source_position", attrs):
            sql = (
                f"SELECT {_SELECT_COLUMNS} FROM migration_position_mappings "  # nosec B608
                "WHERE migration_id = :migration_id AND source_position_token = :source_position_token"
            )
            return await self._fetch_one_mapping(
                sql, {"migration_id": migration_id, "source_position_token": tok}
            )

    async def find_by_target_position(
        self, migration_id: UUID, target_position: Position
    ) -> PositionMapping | None:
        """Find mapping by exact target position."""
        tok = target_position.to_str()
        attrs = {
            "migration.id": str(migration_id),
            "target_position": tok,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.position_mapping_repo.find_by_target_position", attrs):
            sql = (
                f"SELECT {_SELECT_COLUMNS} FROM migration_position_mappings "  # nosec B608
                "WHERE migration_id = :migration_id AND target_position_token = :target_position_token LIMIT 1"
            )
            return await self._fetch_one_mapping(
                sql, {"migration_id": migration_id, "target_position_token": tok}
            )

    async def find_by_event_id(self, migration_id: UUID, event_id: UUID) -> PositionMapping | None:
        """Find mapping by event ID."""
        attrs = {
            "migration.id": str(migration_id),
            "event.id": str(event_id),
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.position_mapping_repo.find_by_event_id", attrs):
            sql = (
                f"SELECT {_SELECT_COLUMNS} FROM migration_position_mappings "  # nosec B608
                "WHERE migration_id = :migration_id AND event_id = :event_id"
            )
            return await self._fetch_one_mapping(
                sql, {"migration_id": migration_id, "event_id": event_id}
            )

    async def list_by_migration(
        self, migration_id: UUID, limit: int = 100, offset: int = 0
    ) -> list[PositionMapping]:
        """List mappings for a migration with pagination ordered by id ascending."""
        attrs = {
            "migration.id": str(migration_id),
            "limit": limit,
            "offset": offset,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.position_mapping_repo.list_by_migration", attrs):
            query = text(
                f"SELECT {_SELECT_COLUMNS} FROM migration_position_mappings "  # nosec B608
                "WHERE migration_id = :migration_id ORDER BY id ASC LIMIT :limit OFFSET :offset"
            )
            async with _get_sql_connection()(self._conn, write=False) as conn:
                params = {"migration_id": migration_id, "limit": limit, "offset": offset}
                rows = (await conn.execute(query, params)).fetchall()
            return [row_to_mapping(row) for row in rows]

    async def list_in_source_range(
        self, migration_id: UUID, start_position: Position, end_position: Position
    ) -> list[PositionMapping]:
        """List mappings within an inclusive source position range."""
        attrs = {
            "migration.id": str(migration_id),
            "start_position": start_position.to_str(),
            "end_position": end_position.to_str(),
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.position_mapping_repo.list_in_source_range", attrs):
            total = await self.count_by_migration(migration_id)
            lo = await self._find_first_ordinal_gte(migration_id, start_position, total)
            hi = await self._find_last_ordinal_lte(migration_id, end_position, total)
            if lo is None or hi is None or lo > hi:
                return []
            return await self.list_by_migration(migration_id, limit=hi - lo + 1, offset=lo)

    async def count_by_migration(self, migration_id: UUID) -> int:
        """Count total mappings for a migration."""
        attrs = {"migration.id": str(migration_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.position_mapping_repo.count_by_migration", attrs):
            query = text(
                "SELECT COUNT(*) FROM migration_position_mappings WHERE migration_id = :migration_id"
            )
            async with _get_sql_connection()(self._conn, write=False) as conn:
                row = (await conn.execute(query, {"migration_id": migration_id})).fetchone()
            return row[0] if row else 0

    async def get_position_bounds(self, migration_id: UUID) -> tuple[Position, Position] | None:
        """Get the first and last mapping's source positions for a migration."""
        attrs = {"migration.id": str(migration_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.position_mapping_repo.get_position_bounds", attrs):
            if (total := await self.count_by_migration(migration_id)) == 0:
                return None
            first = await self._get_by_ordinal(migration_id, 0)
            last = await self._get_by_ordinal(migration_id, total - 1)
            return None if not first or not last else (first.source_position, last.source_position)


__all__ = ["PostgreSQLPositionMappingQueryMixin"]
