"""Binary search and nearest position lookup for position mapping repository.

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


class PostgreSQLPositionMappingSearchMixin:
    """Ordinal navigation and nearest-position binary search for position mappings."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    if TYPE_CHECKING:

        async def count_by_migration(self, migration_id: UUID) -> int: ...

    async def _get_by_ordinal(self, migration_id: UUID, ordinal: int) -> PositionMapping | None:
        """Fetch the mapping at the given zero-based row ordinal, ordered by `id`.

        Private helper shared by the binary-search nearest-match lookups.
        Relies on the monotonicity precondition documented on the class:
        `id` order is source-position order because a single writer records
        mappings in ascending source-position order.

        Args:
            migration_id: UUID of the migration.
            ordinal: Zero-based position in `id` order.

        Returns:
            PositionMapping at that ordinal, or None if out of range.
        """
        query = text("""
            SELECT
                id, migration_id, source_position_token, target_position_token,
                event_id, mapped_at
            FROM migration_position_mappings
            WHERE migration_id = :migration_id
            ORDER BY id ASC
            LIMIT 1 OFFSET :offset
        """)

        async with _get_sql_connection()(self._conn, write=False) as conn:
            result = await conn.execute(
                query,
                {"migration_id": migration_id, "offset": ordinal},
            )
            row = result.fetchone()

        if row is None:
            return None

        return row_to_mapping(row)

    async def _find_last_ordinal_lte(
        self,
        migration_id: UUID,
        source_position: Position,
        total: int,
    ) -> int | None:
        """Binary search over the row ordinal for the last mapping <= a position.

        Positions are opaque tokens: they cannot be ordered in SQL, so the
        nearest match is a binary search over `ORDER BY id` (which is
        source-position order under the monotonicity precondition), with
        the `<=` comparison performed in Python on decoded `Position`
        values. Each step is a single-row `LIMIT 1 OFFSET k` read, giving
        O(log n) round trips instead of loading every mapping.

        Args:
            migration_id: UUID of the migration.
            source_position: Source position to find the nearest ordinal for.
            total: Total mapping count for this migration (avoids a
                redundant COUNT when the caller already has it).

        Returns:
            The greatest zero-based ordinal whose source_position <= the
            given position, or None if no such mapping exists.

        Raises:
            PositionForeignError: If source_position is from a different
                store than the recorded mappings.
        """
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
        """Binary search over the row ordinal for the first mapping >= a position.

        Symmetric counterpart to `_find_last_ordinal_lte`, used to resolve
        the lower bound of a source-position range to an ordinal.

        Args:
            migration_id: UUID of the migration.
            source_position: Source position to find the nearest ordinal for.
            total: Total mapping count for this migration.

        Returns:
            The smallest zero-based ordinal whose source_position >= the
            given position, or None if no such mapping exists.
        """
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
        """Find the nearest mapping with source_position <= given position.

        Mappings for a migration are recorded in ascending source-position
        order by a single writer, so `id` order is source-position order.
        Positions are opaque tokens and cannot be ordered in SQL, so the
        nearest match is a binary search over the row ordinal with the
        comparison performed in Python -- see `_find_last_ordinal_lte`.

        Args:
            migration_id: UUID of the migration
            source_position: Source position to find nearest mapping for

        Returns:
            PositionMapping with highest source_position <= given position,
            or None if no such mapping exists
        """
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
