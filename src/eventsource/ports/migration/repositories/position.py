"""Position mapping persistence protocol for subscription checkpoint translation."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


@runtime_checkable
class PositionMappingRepository(Protocol):
    """Protocol for source-to-target position mapping persistence."""

    async def create(self, mapping: PositionMapping) -> int:
        """Create a new position mapping and return its database ID."""
        ...

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        """Create multiple position mappings in a single transaction."""
        ...

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """Get a position mapping by its database ID."""
        ...

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """Find mapping by exact source position."""
        ...

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: Position,
    ) -> PositionMapping | None:
        """Find mapping by exact target position."""
        ...

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """Find the nearest mapping with source_position <= given position."""
        ...

    async def find_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """Find mapping by event ID."""
        ...

    async def list_by_migration(
        self,
        migration_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PositionMapping]:
        """List mappings for a migration with pagination ordered by source_position ascending."""
        ...

    async def list_in_source_range(
        self,
        migration_id: UUID,
        start_position: Position,
        end_position: Position,
    ) -> list[PositionMapping]:
        """List mappings within an inclusive source position range."""
        ...

    async def count_by_migration(self, migration_id: UUID) -> int:
        """Count total mappings for a migration."""
        ...

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[Position, Position] | None:
        """Get min and max source positions for a migration."""
        ...

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """Delete all mappings for a migration and return the count deleted."""
        ...


__all__ = [
    "PositionMappingRepository",
]
