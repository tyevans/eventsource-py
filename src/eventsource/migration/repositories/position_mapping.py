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
    - Source positions: Position in the source event store
    - Target positions: Corresponding position in the target store

Usage:
    >>> from eventsource.migration.repositories import (
    ...     PositionMappingRepository,
    ...     PostgreSQLPositionMappingRepository,
    ... )
    >>>
    >>> repo = PostgreSQLPositionMappingRepository(conn)
    >>>
    >>> # Record mapping
    >>> await repo.create(PositionMapping(
    ...     migration_id=migration.id,
    ...     source_position=1000,
    ...     target_position=500,
    ...     event_id=event.id,
    ...     mapped_at=datetime.now(UTC),
    ... ))
    >>>
    >>> # Find target position
    >>> mapping = await repo.find_by_source_position(migration.id, 1000)
    >>> print(f"Target position: {mapping.target_position}")
    >>>
    >>> # Find nearest mapping (for checkpoint translation)
    >>> mapping = await repo.find_nearest_source_position(migration.id, 1050)
    >>> # Returns mapping with source_position <= 1050

See Also:
    - Task: P3-001-position-mapping-repository.md
    - Schema: PREREQ-002-migration-schema.md
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.migration.models import PositionMapping
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.repositories._connection import execute_with_connection


@runtime_checkable
class PositionMappingRepository(Protocol):
    """
    Protocol for position mapping persistence.

    Stores and retrieves source-to-target position mappings for
    subscription checkpoint translation during migration.
    Implementations must handle concurrent access safely and
    provide efficient range queries.
    """

    async def create(self, mapping: PositionMapping) -> int:
        """
        Create a new position mapping.

        Args:
            mapping: PositionMapping instance to persist

        Returns:
            The database ID of the created mapping

        Raises:
            IntegrityError: If mapping already exists for this migration/source_position
        """
        ...

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        """
        Create multiple position mappings in a single transaction.

        Optimized for bulk copy operations where many mappings
        need to be recorded efficiently.

        Args:
            mappings: List of PositionMapping instances to persist

        Returns:
            Number of mappings created
        """
        ...

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """
        Get a position mapping by its database ID.

        Args:
            mapping_id: Database ID of the mapping

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: int,
    ) -> PositionMapping | None:
        """
        Find mapping by exact source position.

        Args:
            migration_id: UUID of the migration
            source_position: Exact source position to find

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: int,
    ) -> PositionMapping | None:
        """
        Find mapping by exact target position.

        Args:
            migration_id: UUID of the migration
            target_position: Exact target position to find

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: int,
    ) -> PositionMapping | None:
        """
        Find the nearest mapping with source_position <= given position.

        Used for checkpoint translation when exact position mapping
        doesn't exist. Returns the closest mapping at or before the
        given source position.

        Args:
            migration_id: UUID of the migration
            source_position: Source position to find nearest mapping for

        Returns:
            PositionMapping with highest source_position <= given position,
            or None if no such mapping exists
        """
        ...

    async def find_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """
        Find mapping by event ID.

        Useful for debugging and verification.

        Args:
            migration_id: UUID of the migration
            event_id: UUID of the event

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def list_by_migration(
        self,
        migration_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PositionMapping]:
        """
        List mappings for a migration with pagination.

        Results are ordered by source_position ascending.

        Args:
            migration_id: UUID of the migration
            limit: Maximum number of results (default 100)
            offset: Number of results to skip (default 0)

        Returns:
            List of PositionMapping instances
        """
        ...

    async def list_in_source_range(
        self,
        migration_id: UUID,
        start_position: int,
        end_position: int,
    ) -> list[PositionMapping]:
        """
        List mappings within a source position range.

        Returns all mappings where start_position <= source_position <= end_position.
        Results are ordered by source_position ascending.

        Args:
            migration_id: UUID of the migration
            start_position: Start of source position range (inclusive)
            end_position: End of source position range (inclusive)

        Returns:
            List of PositionMapping instances
        """
        ...

    async def count_by_migration(self, migration_id: UUID) -> int:
        """
        Count total mappings for a migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            Number of mappings
        """
        ...

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[int, int] | None:
        """
        Get min and max source positions for a migration.

        Useful for understanding the range of migrated events.

        Args:
            migration_id: UUID of the migration

        Returns:
            Tuple of (min_source_position, max_source_position) or None if no mappings
        """
        ...

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """
        Delete all mappings for a migration.

        Called during migration cleanup or when re-starting a failed migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            Number of mappings deleted
        """
        ...


class PostgreSQLPositionMappingRepository:
    """
    PostgreSQL implementation of PositionMappingRepository.

    Persists position mappings to the `migration_position_mappings` table.
    Provides CRUD operations and efficient range queries for checkpoint
    translation during migration.

    The implementation uses indexed queries optimized for:
    - Exact position lookups (O(log n))
    - Nearest position queries (O(log n))
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
        >>> mapping = await repo.find_nearest_source_position(migration_id, 1000)
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
                "source_position": mapping.source_position,
                "target_position": mapping.target_position,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position, target_position,
                    event_id, mapped_at
                ) VALUES (
                    :migration_id, :source_position, :target_position,
                    :event_id, :mapped_at
                )
                RETURNING id
            """)

            params = {
                "migration_id": mapping.migration_id,
                "source_position": mapping.source_position,
                "target_position": mapping.target_position,
                "event_id": mapping.event_id,
                "mapped_at": mapping.mapped_at,
            }

            async with execute_with_connection(self._conn, transactional=True) as conn:
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
            # Build values for multi-row insert
            values_list: list[str] = []
            params: dict[str, Any] = {}

            for i, mapping in enumerate(mappings):
                values_list.append(
                    f"(:migration_id_{i}, :source_position_{i}, :target_position_{i}, "
                    f":event_id_{i}, :mapped_at_{i})"
                )
                params[f"migration_id_{i}"] = mapping.migration_id
                params[f"source_position_{i}"] = mapping.source_position
                params[f"target_position_{i}"] = mapping.target_position
                params[f"event_id_{i}"] = mapping.event_id
                params[f"mapped_at_{i}"] = mapping.mapped_at

            values_sql = ", ".join(values_list)

            # values_sql contains only parameterized placeholders; all values are in params dict
            query = text(f"""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position, target_position,
                    event_id, mapped_at
                ) VALUES {values_sql}
                ON CONFLICT (migration_id, source_position) DO NOTHING
            """)  # nosec B608 - parameterized query construction

            async with execute_with_connection(self._conn, transactional=True) as conn:
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
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE id = :id
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"id": mapping_id})
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_mapping(row)

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: int,
    ) -> PositionMapping | None:
        """
        Find mapping by exact source position.

        Uses the unique index on (migration_id, source_position) for
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
                "source_position": source_position,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND source_position = :source_position
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "source_position": source_position,
                    },
                )
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_mapping(row)

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: int,
    ) -> PositionMapping | None:
        """
        Find mapping by exact target position.

        Note: This query is less efficient than find_by_source_position
        as there is no unique index on target_position. Consider adding
        an index if this is called frequently.

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
                "target_position": target_position,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND target_position = :target_position
                LIMIT 1
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "target_position": target_position,
                    },
                )
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_mapping(row)

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: int,
    ) -> PositionMapping | None:
        """
        Find the nearest mapping with source_position <= given position.

        Uses the DESC index on (migration_id, source_position DESC) for
        efficient O(log n) nearest-neighbor lookup. This is the key query
        for checkpoint translation during subscription migration.

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
                "source_position": source_position,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND source_position <= :source_position
                ORDER BY source_position DESC
                LIMIT 1
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "source_position": source_position,
                    },
                )
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_mapping(row)

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
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND event_id = :event_id
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
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

        Results are ordered by source_position ascending for consistent
        iteration through the mapping set.

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
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                ORDER BY source_position ASC
                LIMIT :limit OFFSET :offset
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
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
        start_position: int,
        end_position: int,
    ) -> list[PositionMapping]:
        """
        List mappings within a source position range.

        Returns all mappings where start_position <= source_position <= end_position.
        Uses the index on (migration_id, source_position) for efficient range scan.

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
                "start_position": start_position,
                "end_position": end_position,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, source_position, target_position,
                    event_id, mapped_at
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
                  AND source_position >= :start_position
                  AND source_position <= :end_position
                ORDER BY source_position ASC
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(
                    query,
                    {
                        "migration_id": migration_id,
                        "start_position": start_position,
                        "end_position": end_position,
                    },
                )
                rows = result.fetchall()

            return [self._row_to_mapping(row) for row in rows]

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

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})
                row = result.fetchone()

            return row[0] if row else 0

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[int, int] | None:
        """
        Get min and max source positions for a migration.

        Uses aggregate functions with the index for efficient computation.

        Args:
            migration_id: UUID of the migration

        Returns:
            Tuple of (min_source_position, max_source_position) or None if no mappings
        """
        with self._tracer.span(
            "eventsource.position_mapping_repo.get_position_bounds",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT MIN(source_position), MAX(source_position)
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})
                row = result.fetchone()

            if row is None or row[0] is None:
                return None

            return (row[0], row[1])

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, {"migration_id": migration_id})

            return result.rowcount

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _row_to_mapping(self, row: Sequence[Any]) -> PositionMapping:
        """
        Convert database row to PositionMapping instance.

        The row order matches the SELECT queries:
        (id, migration_id, source_position, target_position, event_id, mapped_at)

        Note: The id field is not part of PositionMapping as it's a dataclass
        frozen model. The database ID is only used for internal lookups.

        Args:
            row: Database row tuple from SELECT query

        Returns:
            PositionMapping instance
        """
        return PositionMapping(
            migration_id=row[1],
            source_position=row[2],
            target_position=row[3],
            event_id=row[4],
            mapped_at=row[5],
        )


# Type alias for backwards compatibility
PositionMappingRepositoryProtocol = PositionMappingRepository
