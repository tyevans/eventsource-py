"""
Position mapping persistence protocol.

Defines persistence contracts for source-to-target position mappings
used for subscription checkpoint translation during live migration.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


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
        source_position: Position,
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
        target_position: Position,
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
        source_position: Position,
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
        start_position: Position,
        end_position: Position,
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
    ) -> tuple[Position, Position] | None:
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


__all__ = [
    "PositionMappingRepository",
]
