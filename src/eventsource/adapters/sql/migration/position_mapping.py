"""
PositionMappingRepository - Data access for position mappings.

The PositionMappingRepository stores and retrieves mappings between
event positions in source and target stores. These mappings are
essential for translating subscription checkpoints during migration.

Responsibilities:
    - Store position mappings during bulk copy
    - Store position mappings during dual-write
    - Efficient lookup for position translation
    - Support both exact and nearest-position lookups
    - Batch insert support for bulk copy efficiency
    - Cleanup mappings after migration completion

Database Table:
    Uses the `migration_position_mappings` table defined in PREREQ-002.

Position Types:
    - Source positions: opaque `Position` tokens from the source event store
    - Target positions: opaque `Position` tokens from the target event store,
      persisted as their `Position.to_str()` encoding, never as an integer

Usage:
    >>> from eventsource.adapters.sql.migration import (
    ...     PositionMappingRepository,
    ...     PostgreSQLPositionMappingRepository,
    ... )
    >>>
    >>> repo = PostgreSQLPositionMappingRepository(conn)
    >>>
    >>> # Record mapping
    >>> await repo.create(PositionMapping(
    ...     migration_id=migration.id,
    ...     source_position=source_position,  # Position
    ...     target_position=target_position,  # Position
    ...     event_id=event.id,
    ...     mapped_at=datetime.now(UTC),
    ... ))
    >>>
    >>> # Find target position
    >>> mapping = await repo.find_by_source_position(migration.id, source_position)
    >>> print(f"Target position: {mapping.target_position}")
    >>>
    >>> # Find nearest mapping (for checkpoint translation): a binary search
    >>> # over rows ordered by surrogate id, comparing decoded Position values,
    >>> # relying on mappings having been recorded in ascending source order
    >>> mapping = await repo.find_nearest_source_position(migration.id, checkpoint_position)
    >>> # Returns the mapping with the greatest source_position <= checkpoint_position

See Also:
    - Task: P3-001-position-mapping-repository.md
    - Schema: PREREQ-002-migration-schema.md
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.ports.exceptions import PositionDecodeError
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


class PostgreSQLPositionMappingRepository:
    """
    PostgreSQL implementation of PositionMappingRepository.

    Persists position mappings to the `migration_position_mappings` table,
    keyed by opaque position tokens (`Position.to_str()`). Provides CRUD
    operations and efficient range queries for checkpoint translation
    during migration.

    Monotonicity precondition: mappings for a migration must be recorded in
    ascending source-position order; ordering and nearest-match lookups rely
    on it, because opaque position tokens cannot be ordered in SQL. The
    single writer for a migration (`BulkCopier` streaming the source feed,
    or `DualWriteInterceptor` appending in write order) satisfies this by
    construction; a second concurrent writer for the same migration would
    violate it.

    The implementation uses indexed queries optimized for:
    - Exact position lookups (O(log n) via the token index)
    - Nearest position queries (O(log n) binary search over row ordinal)
    - Range queries for batch processing

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLPositionMappingRepository(conn)
        ...     await repo.create(mapping)
        ...
        >>> # Batch insert for bulk copy
        >>> await repo.create_batch(mappings)
        >>>
        >>> # Checkpoint translation
        >>> mapping = await repo.find_nearest_source_position(migration_id, position)
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the repository.

        Args:
            conn: Database connection or engine
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn

    async def create(self, mapping: PositionMapping) -> int:
        """
        Create a new position mapping.

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
            # Only the opaque token columns are written; the legacy BIGINT
            # columns are left NULL (Task 1 dropped their NOT NULL) rather
            # than fabricate an int position from an opaque token.
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

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()

            if row is None:
                raise RuntimeError("Failed to create position mapping - no row returned")
            return int(row[0])

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        """
        Create multiple position mappings in a single transaction.

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
            # Build values for multi-row insert. Only the opaque token
            # columns are written -- see create() for why the legacy BIGINT
            # columns are left unwritten.
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

            # values_sql contains only parameterized placeholders; all values are in params dict
            query = text(f"""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position_token, target_position_token,
                    event_id, mapped_at
                ) VALUES {values_sql}
                ON CONFLICT (migration_id, source_position_token) DO NOTHING
            """)  # nosec B608 - parameterized query construction

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, params)

            return result.rowcount

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """
        Get a position mapping by its database ID.

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

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": mapping_id})
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_mapping(row)

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """
        Find mapping by exact source position.

        Uses the unique index on (migration_id, source_position_token) for
        efficient O(log n) lookup. `Position.to_str()` is deterministic
        (fixed separators, fixed key order), so string equality on the
        canonical token is position equality within a store.

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

            async with sql_connection(self._conn, write=False) as conn:
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

            return self._row_to_mapping(row)

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: Position,
    ) -> PositionMapping | None:
        """
        Find mapping by exact target position.

        Note: This query is less efficient than find_by_source_position
        as there is no unique index on target_position_token. Consider
        adding one if this is called frequently.

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

            async with sql_connection(self._conn, write=False) as conn:
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

            return self._row_to_mapping(row)

    async def _get_by_ordinal(self, migration_id: UUID, ordinal: int) -> PositionMapping | None:
        """
        Fetch the mapping at the given zero-based row ordinal, ordered by `id`.

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

        async with sql_connection(self._conn, write=False) as conn:
            result = await conn.execute(
                query,
                {"migration_id": migration_id, "offset": ordinal},
            )
            row = result.fetchone()

        if row is None:
            return None

        return self._row_to_mapping(row)

    async def _find_last_ordinal_lte(
        self,
        migration_id: UUID,
        source_position: Position,
        total: int,
    ) -> int | None:
        """
        Binary search over the row ordinal for the last mapping <= a position.

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
                store than the recorded mappings; this is not caught here,
                it is the correct failure.
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
        """
        Binary search over the row ordinal for the first mapping >= a position.

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
        """
        Find the nearest mapping with source_position <= given position.

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

    async def find_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """
        Find mapping by event ID.

        Uses the index on event_id for efficient lookup.
        Useful for debugging and verification.

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

            async with sql_connection(self._conn, write=False) as conn:
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

            return self._row_to_mapping(row)

    async def list_by_migration(
        self,
        migration_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PositionMapping]:
        """
        List mappings for a migration with pagination.

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

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "limit": limit,
                        "offset": offset,
                    },
                )
                rows = result.fetchall()

            return [self._row_to_mapping(row) for row in rows]

    async def list_in_source_range(
        self,
        migration_id: UUID,
        start_position: Position,
        end_position: Position,
    ) -> list[PositionMapping]:
        """
        List mappings within a source position range.

        Returns all mappings where start_position <= source_position <= end_position.
        Positions are opaque tokens and cannot be range-filtered in SQL
        (no `BETWEEN` on tokens), so both endpoints are resolved to row
        ordinals via binary search (same technique as
        `find_nearest_source_position`), and the ordinal range between them
        is selected by a single bounded `id`-ordered query -- never a full
        table scan of the migration's mappings.

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
        """
        Count total mappings for a migration.

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

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})
                row = result.fetchone()

            return row[0] if row else 0

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[Position, Position] | None:
        """
        Get the first and last mapping's source positions for a migration.

        `MIN`/`MAX` cannot be used on opaque tokens (lexicographic string
        ordering is not position ordering), so the bounds are the first and
        last mapping by `id` -- source-position order under the class's
        monotonicity precondition.

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

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """
        Delete all mappings for a migration.

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

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})

            return result.rowcount

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _row_to_mapping(self, row: Sequence[Any]) -> PositionMapping:
        """
        Convert database row to PositionMapping instance.

        The row order matches the SELECT queries:
        (id, migration_id, source_position_token, target_position_token,
        event_id, mapped_at)

        Note: The id field is not part of PositionMapping as it's a dataclass
        frozen model. The database ID is only used for internal lookups.

        Args:
            row: Database row tuple from SELECT query

        Returns:
            PositionMapping instance

        Raises:
            PositionDecodeError: If either token column is missing (a row
                carrying only the legacy int position, which this library
                never writes and unreleased software cannot have produced).
        """
        source_token, target_token = row[2], row[3]
        if source_token is None or target_token is None:
            raise PositionDecodeError(
                f"position mapping row {row[0]!r} has no position token "
                "(legacy int-only row is not decodable)"
            )
        return PositionMapping(
            migration_id=row[1],
            source_position=Position.from_str(source_token),
            target_position=Position.from_str(target_token),
            event_id=row[4],
            mapped_at=row[5],
        )
