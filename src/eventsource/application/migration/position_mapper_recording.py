"""
Position mapping recording, batching, and lifecycle operations.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.observability import Tracer
    from eventsource.ports.migration.repositories import PositionMappingRepository

logger = logging.getLogger(__name__)


class PositionMapperRecordingMixin:
    """Mixin providing position mapping recording, batching, and lifecycle methods."""

    _repo: PositionMappingRepository
    _tracer: Tracer

    async def record_mapping(
        self,
        migration_id: UUID,
        source_position: Position,
        target_position: Position,
        event_id: UUID,
        *,
        mapped_at: datetime | None = None,
    ) -> None:
        """
        Record a position mapping during bulk copy or dual-write.

        Creates a mapping between a source store position and the
        corresponding target store position. These mappings are used
        for checkpoint translation during subscription migration.

        Callers must record mappings in ascending source-position order
        for a given migration; the repository's nearest-match lookups
        depend on that ordering (see `PostgreSQLPositionMappingRepository`).

        Args:
            migration_id: ID of the migration.
            source_position: Position in the source store.
            target_position: Corresponding position in the target store.
            event_id: ID of the event at this position.
            mapped_at: When the mapping was created (defaults to now).

        Raises:
            PositionMappingError: If recording the mapping fails.
        """
        with self._tracer.span(
            "eventsource.position_mapper.record_mapping",
            {
                "migration.id": str(migration_id),
                "source_position": source_position.to_str(),
                "target_position": target_position.to_str(),
                "event.id": str(event_id),
            },
        ):
            mapping = PositionMapping(
                migration_id=migration_id,
                source_position=source_position,
                target_position=target_position,
                event_id=event_id,
                mapped_at=mapped_at or datetime.now(UTC),
            )

            try:
                await self._repo.create(mapping)
                logger.debug(
                    "Recorded position mapping: source=%s -> target=%s for migration %s",
                    source_position.to_str(),
                    target_position.to_str(),
                    migration_id,
                )
            except Exception as e:
                logger.error("Failed to record position mapping: %s", e)
                raise PositionMappingError(
                    f"Failed to record mapping: {e}",
                    migration_id=migration_id,
                    source_position=source_position,
                    reason=str(e),
                ) from e

    async def record_mappings_batch(
        self,
        migration_id: UUID,
        mappings: list[tuple[Position, Position, UUID]],
        *,
        mapped_at: datetime | None = None,
    ) -> int:
        """
        Record multiple position mappings in a single batch.

        Optimized for bulk copy operations where many mappings need
        to be recorded efficiently.

        Args:
            migration_id: ID of the migration.
            mappings: List of (source_position, target_position, event_id) tuples.
            mapped_at: When the mappings were created (defaults to now).

        Returns:
            Number of mappings successfully recorded.

        Raises:
            PositionMappingError: If recording the batch fails.
        """
        if not mappings:
            return 0

        with self._tracer.span(
            "eventsource.position_mapper.record_mappings_batch",
            {
                "migration.id": str(migration_id),
                "batch_size": len(mappings),
            },
        ):
            now = mapped_at or datetime.now(UTC)
            position_mappings = [
                PositionMapping(
                    migration_id=migration_id,
                    source_position=source_pos,
                    target_position=target_pos,
                    event_id=event_id,
                    mapped_at=now,
                )
                for source_pos, target_pos, event_id in mappings
            ]

            try:
                count = await self._repo.create_batch(position_mappings)
                logger.debug(
                    "Recorded %d position mappings for migration %s",
                    count,
                    migration_id,
                )
                return count
            except Exception as e:
                logger.error("Failed to record position mappings batch: %s", e)
                raise PositionMappingError(
                    f"Failed to record batch mappings: {e}",
                    migration_id=migration_id,
                    reason=str(e),
                ) from e

    async def get_mapping_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """
        Get a position mapping by event ID.

        Useful for debugging and verification.

        Args:
            migration_id: ID of the migration.
            event_id: ID of the event.

        Returns:
            PositionMapping if found, None otherwise.
        """
        with self._tracer.span(
            "eventsource.position_mapper.get_mapping_by_event_id",
            {
                "migration.id": str(migration_id),
                "event.id": str(event_id),
            },
        ):
            return await self._repo.find_by_event_id(migration_id, event_id)

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[Position, Position] | None:
        """
        Get the first and last source positions mapped for a migration.

        Useful for understanding the range of positions that have
        been mapped during migration.

        Args:
            migration_id: ID of the migration.

        Returns:
            Tuple of (min_position, max_position) or None if no mappings.
        """
        with self._tracer.span(
            "eventsource.position_mapper.get_position_bounds",
            {"migration.id": str(migration_id)},
        ):
            return await self._repo.get_position_bounds(migration_id)

    async def get_mapping_count(
        self,
        migration_id: UUID,
    ) -> int:
        """
        Get the total number of position mappings for a migration.

        Args:
            migration_id: ID of the migration.

        Returns:
            Number of mappings recorded.
        """
        with self._tracer.span(
            "eventsource.position_mapper.get_mapping_count",
            {"migration.id": str(migration_id)},
        ):
            return await self._repo.count_by_migration(migration_id)

    async def clear_mappings(
        self,
        migration_id: UUID,
    ) -> int:
        """
        Delete all position mappings for a migration.

        Called during migration cleanup or when restarting a failed migration.

        Args:
            migration_id: ID of the migration.

        Returns:
            Number of mappings deleted.
        """
        with self._tracer.span(
            "eventsource.position_mapper.clear_mappings",
            {"migration.id": str(migration_id)},
        ):
            count = await self._repo.delete_by_migration(migration_id)
            logger.info(
                "Cleared %d position mappings for migration %s",
                count,
                migration_id,
            )
            return count


__all__ = ["PositionMapperRecordingMixin"]
