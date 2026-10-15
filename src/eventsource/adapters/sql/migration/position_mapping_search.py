"""Binary search and nearest position lookup for position mapping repository (ADR-0002, ADR-0007)."""

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


class PostgreSQLPositionMappingSearchMixin:
    """Ordinal navigation and nearest-position binary search for position mappings."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    if TYPE_CHECKING:

        async def count_by_migration(self, migration_id: UUID) -> int: ...

    async def _get_by_ordinal(self, migration_id: UUID, ordinal: int) -> PositionMapping | None:
        """Fetch the mapping at the given zero-based row ordinal, ordered by `id`."""
        query = text("""
            SELECT id, migration_id, source_position_token, target_position_token,
                   event_id, mapped_at
            FROM migration_position_mappings
            WHERE migration_id = :migration_id
            ORDER BY id ASC
            LIMIT 1 OFFSET :offset
        """)
        async with _get_sql_connection()(self._conn, write=False) as conn:
            result = await conn.execute(query, {"migration_id": migration_id, "offset": ordinal})
            row = result.fetchone()
        return None if row is None else row_to_mapping(row)

    async def _find_last_ordinal_lte(
        self,
        migration_id: UUID,
        source_position: Position,
        total: int,
    ) -> int | None:
        """Binary search over the row ordinal for the last mapping <= a position."""
        if total == 0:
            return None

        lo, hi = 0, total - 1
        best: int | None = None
        while lo <= hi:
            mid = (lo + hi) // 2
            candidate = await self._get_by_ordinal(migration_id, mid)
            if candidate is None:
                break
            if candidate.source_position <= source_position:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    async def _find_first_ordinal_gte(
        self,
        migration_id: UUID,
        source_position: Position,
        total: int,
    ) -> int | None:
        """Binary search over the row ordinal for the first mapping >= a position."""
        if total == 0:
            return None

        lo, hi = 0, total - 1
        best: int | None = None
        while lo <= hi:
            mid = (lo + hi) // 2
            candidate = await self._get_by_ordinal(migration_id, mid)
            if candidate is None:
                break
            if candidate.source_position >= source_position:
                best = mid
                hi = mid - 1
            else:
                lo = mid + 1
        return best

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """Find the nearest mapping with source_position <= given position."""
        with self._tracer.span(
            "eventsource.position_mapping_repo.find_nearest_source_position",
            {
                "migration.id": str(migration_id),
                "source_position": source_position.to_str(),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            total = await self.count_by_migration(migration_id)
            ordinal = await self._find_last_ordinal_lte(migration_id, source_position, total)
            if ordinal is None:
                return None
            return await self._get_by_ordinal(migration_id, ordinal)


__all__ = ["PostgreSQLPositionMappingSearchMixin"]
