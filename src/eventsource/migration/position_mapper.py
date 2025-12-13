"""
PositionMapper - Maps positions between source and target stores.

The PositionMapper maintains and queries mappings between event positions
in the source store and their corresponding positions in the target store.
This is essential for subscription continuity during migration.

Responsibilities:
    - Record position mappings during bulk copy
    - Record position mappings during dual-write
    - Translate source positions to target positions
    - Translate target positions to source positions
    - Handle gaps and missing mappings gracefully
    - Support batch translation for efficiency

Mapping Strategy:
    - Mappings are recorded during event copy/write
    - Exact lookups are attempted first
    - Nearest-neighbor lookup for positions without exact mappings
    - Interpolation support for estimating positions between recorded mappings

Usage:
    >>> from eventsource.migration import PositionMapper
    >>> from eventsource.migration.repositories import PostgreSQLPositionMappingRepository
    >>>
    >>> mapper = PositionMapper(position_mapping_repo)
    >>>
    >>> # Record mapping during copy
    >>> await mapper.record_mapping(
    ...     migration_id=migration.id,
    ...     source_position=1000,
    ...     target_position=500,
    ...     event_id=event.id,
    ... )
    >>>
    >>> # Translate position for subscription
    >>> result = await mapper.translate_position(
    ...     migration_id=migration.id,
    ...     source_position=1050,
    ... )
    >>> print(f"Target position: {result.target_position}")

See Also:
    - Task: P3-002-position-mapper.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.migration.exceptions import PositionMappingError
from eventsource.migration.models import PositionMapping
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.migration.repositories.position_mapping import (
        PositionMappingRepository,
    )

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TranslationResult:
    """
    Result of a position translation operation.

    Contains the translated position along with metadata about
    how the translation was performed.

    Attributes:
        source_position: The original source position.
        target_position: The translated target position.
        is_exact: Whether the translation was an exact match.
        nearest_source_position: The source position used for nearest match.
        interpolated: Whether interpolation was used.
    """

    source_position: int
    target_position: int
    is_exact: bool
    nearest_source_position: int | None = None
    interpolated: bool = False


@dataclass(frozen=True)
class ReverseTranslationResult:
    """
    Result of a reverse position translation operation (target to source).

    Contains the translated position along with metadata about
    how the translation was performed.

    Attributes:
        target_position: The original target position.
        source_position: The translated source position.
        is_exact: Whether the translation was an exact match.
        nearest_target_position: The target position used for nearest match.
    """

    target_position: int
    source_position: int
    is_exact: bool
    nearest_target_position: int | None = None


class PositionMapper:
    """
    Maps event positions between source and target stores.

    Essential for subscription continuity, allowing subscriptions to
    resume at the correct position in the target store after migration.
    Uses PositionMappingRepository for persistent storage of mappings.

    The mapper supports three translation strategies:
    1. Exact match: Direct lookup of recorded position mapping
    2. Nearest: Find the closest recorded position at or before the query
    3. Interpolation: Estimate position based on surrounding mappings

    Example:
        >>> repo = PostgreSQLPositionMappingRepository(conn)
        >>> mapper = PositionMapper(repo)
        >>>
        >>> # Record mappings during bulk copy
        >>> await mapper.record_mapping(migration_id, 100, 50, event_id)
        >>> await mapper.record_mapping(migration_id, 200, 100, event_id2)
        >>>
        >>> # Translate a checkpoint position
        >>> result = await mapper.translate_position(migration_id, 150)
        >>> # Returns nearest position at 100 -> 50

    Attributes:
        _repo: Position mapping repository for persistence.
    """

    def __init__(
        self,
        position_mapping_repo: PositionMappingRepository,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the position mapper.

        Args:
            position_mapping_repo: Repository for storing/retrieving mappings.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._repo = position_mapping_repo

    async def record_mapping(
        self,
        migration_id: UUID,
        source_position: int,
        target_position: int,
        event_id: UUID,
        *,
        mapped_at: datetime | None = None,
    ) -> None:
        """
        Record a position mapping during bulk copy or dual-write.

        Creates a mapping between a source store position and the
        corresponding target store position. These mappings are used
        for checkpoint translation during subscription migration.

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
                "source_position": source_position,
                "target_position": target_position,
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
                    "Recorded position mapping: source=%d -> target=%d for migration %s",
                    source_position,
                    target_position,
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
        mappings: list[tuple[int, int, UUID]],
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

    async def translate_position(
        self,
        migration_id: UUID,
        source_position: int,
        *,
        use_nearest: bool = True,
    ) -> TranslationResult:
        """
        Translate a source position to target position.

        First attempts an exact match lookup. If not found and use_nearest
        is True, finds the nearest mapping with source_position <= given
        position. This is the primary method for checkpoint translation.

        Args:
            migration_id: ID of the migration.
            source_position: Position in the source store to translate.
            use_nearest: Whether to use nearest-neighbor lookup if exact
                match is not found (default True).

        Returns:
            TranslationResult with translated position and metadata.

        Raises:
            PositionMappingError: If no mapping can be found.
        """
        with self._tracer.span(
            "eventsource.position_mapper.translate_position",
            {
                "migration.id": str(migration_id),
                "source_position": source_position,
                "use_nearest": use_nearest,
            },
        ):
            # Try exact match first
            exact_mapping = await self._repo.find_by_source_position(
                migration_id,
                source_position,
            )

            if exact_mapping is not None:
                logger.debug(
                    "Exact position translation: source=%d -> target=%d",
                    source_position,
                    exact_mapping.target_position,
                )
                return TranslationResult(
                    source_position=source_position,
                    target_position=exact_mapping.target_position,
                    is_exact=True,
                )

            # Try nearest match
            if use_nearest:
                nearest_mapping = await self._repo.find_nearest_source_position(
                    migration_id,
                    source_position,
                )

                if nearest_mapping is not None:
                    logger.debug(
                        "Nearest position translation: source=%d (nearest=%d) -> target=%d",
                        source_position,
                        nearest_mapping.source_position,
                        nearest_mapping.target_position,
                    )
                    return TranslationResult(
                        source_position=source_position,
                        target_position=nearest_mapping.target_position,
                        is_exact=False,
                        nearest_source_position=nearest_mapping.source_position,
                    )

            # No mapping found
            raise PositionMappingError(
                f"No mapping found for source position {source_position}",
                migration_id=migration_id,
                source_position=source_position,
                reason="no_mapping",
            )

    async def translate_position_reverse(
        self,
        migration_id: UUID,
        target_position: int,
    ) -> ReverseTranslationResult:
        """
        Translate a target position back to source position.

        Looks up the mapping by target position. This is useful for
        debugging and verification purposes.

        Args:
            migration_id: ID of the migration.
            target_position: Position in the target store to translate.

        Returns:
            ReverseTranslationResult with translated position and metadata.

        Raises:
            PositionMappingError: If no mapping can be found.
        """
        with self._tracer.span(
            "eventsource.position_mapper.translate_position_reverse",
            {
                "migration.id": str(migration_id),
                "target_position": target_position,
            },
        ):
            mapping = await self._repo.find_by_target_position(
                migration_id,
                target_position,
            )

            if mapping is not None:
                logger.debug(
                    "Reverse position translation: target=%d -> source=%d",
                    target_position,
                    mapping.source_position,
                )
                return ReverseTranslationResult(
                    target_position=target_position,
                    source_position=mapping.source_position,
                    is_exact=True,
                )

            # No exact match found
            raise PositionMappingError(
                f"No mapping found for target position {target_position}",
                migration_id=migration_id,
                reason="no_mapping",
            )

    async def translate_positions_batch(
        self,
        migration_id: UUID,
        source_positions: list[int],
        *,
        use_nearest: bool = True,
    ) -> list[TranslationResult]:
        """
        Translate multiple source positions to target positions.

        Optimizes batch translation by fetching a range of mappings
        once and performing lookups in memory.

        Args:
            migration_id: ID of the migration.
            source_positions: List of source positions to translate.
            use_nearest: Whether to use nearest-neighbor lookup if exact
                match is not found (default True).

        Returns:
            List of TranslationResult for each position.

        Raises:
            PositionMappingError: If any position cannot be translated.
        """
        if not source_positions:
            return []

        with self._tracer.span(
            "eventsource.position_mapper.translate_positions_batch",
            {
                "migration.id": str(migration_id),
                "batch_size": len(source_positions),
                "use_nearest": use_nearest,
            },
        ):
            # Get the range of positions
            min_pos = min(source_positions)
            max_pos = max(source_positions)

            # Fetch all mappings in the range (with some buffer for nearest lookups)
            buffer = 1000  # Extra positions for nearest lookups
            start_pos = max(0, min_pos - buffer)
            end_pos = max_pos

            mappings = await self._repo.list_in_source_range(
                migration_id,
                start_pos,
                end_pos,
            )

            # Build lookup structures
            exact_map: dict[int, int] = {m.source_position: m.target_position for m in mappings}

            # Sort mappings by source position for nearest lookup
            sorted_mappings = sorted(mappings, key=lambda m: m.source_position)

            results: list[TranslationResult] = []

            for source_pos in source_positions:
                # Try exact match
                if source_pos in exact_map:
                    results.append(
                        TranslationResult(
                            source_position=source_pos,
                            target_position=exact_map[source_pos],
                            is_exact=True,
                        )
                    )
                    continue

                # Try nearest match
                if use_nearest and sorted_mappings:
                    nearest = self._find_nearest(sorted_mappings, source_pos)
                    if nearest is not None:
                        results.append(
                            TranslationResult(
                                source_position=source_pos,
                                target_position=nearest.target_position,
                                is_exact=False,
                                nearest_source_position=nearest.source_position,
                            )
                        )
                        continue

                # No mapping found
                raise PositionMappingError(
                    f"No mapping found for source position {source_pos}",
                    migration_id=migration_id,
                    source_position=source_pos,
                    reason="no_mapping",
                )

            logger.debug(
                "Batch translated %d positions for migration %s",
                len(results),
                migration_id,
            )
            return results

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
    ) -> tuple[int, int] | None:
        """
        Get the min and max source positions mapped for a migration.

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

    def _find_nearest(
        self,
        sorted_mappings: list[PositionMapping],
        source_position: int,
    ) -> PositionMapping | None:
        """
        Find the nearest mapping with source_position <= given position.

        Uses binary search for efficiency.

        Args:
            sorted_mappings: List of mappings sorted by source_position.
            source_position: Position to find nearest mapping for.

        Returns:
            Nearest PositionMapping or None if no suitable mapping exists.
        """
        if not sorted_mappings:
            return None

        # Binary search for the nearest position <= source_position
        left = 0
        right = len(sorted_mappings) - 1
        result: PositionMapping | None = None

        while left <= right:
            mid = (left + right) // 2
            if sorted_mappings[mid].source_position <= source_position:
                result = sorted_mappings[mid]
                left = mid + 1
            else:
                right = mid - 1

        return result
