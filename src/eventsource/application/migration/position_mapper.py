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
    >>> from eventsource.application.migration import PositionMapper
    >>> from eventsource.adapters.sql.migration import PostgreSQLPositionMappingRepository
    >>> from eventsource.ports import Position
    >>>
    >>> mapper = PositionMapper(position_mapping_repo)
    >>>
    >>> # Record mapping during copy
    >>> await mapper.record_mapping(
    ...     migration_id=migration.id,
    ...     source_position=Position(store_id="source", key=(1000,)),
    ...     target_position=Position(store_id="target", key=(500,)),
    ...     event_id=event.id,
    ... )
    >>>
    >>> # Translate position for subscription
    >>> result = await mapper.translate_position(
    ...     migration_id=migration.id,
    ...     source_position=Position(store_id="source", key=(1050,)),
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

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.observability import Tracer, create_tracer
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.ports.migration.repositories import PositionMappingRepository

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

    source_position: Position
    target_position: Position
    is_exact: bool
    nearest_source_position: Position | None = None
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

    target_position: Position
    source_position: Position
    is_exact: bool
    nearest_target_position: Position | None = None


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

    async def translate_position(
        self,
        migration_id: UUID,
        source_position: Position,
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
                "source_position": source_position.to_str(),
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
                    "Exact position translation: source=%s -> target=%s",
                    source_position.to_str(),
                    exact_mapping.target_position.to_str(),
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
                        "Nearest position translation: source=%s (nearest=%s) -> target=%s",
                        source_position.to_str(),
                        nearest_mapping.source_position.to_str(),
                        nearest_mapping.target_position.to_str(),
                    )
                    return TranslationResult(
                        source_position=source_position,
                        target_position=nearest_mapping.target_position,
                        is_exact=False,
                        nearest_source_position=nearest_mapping.source_position,
                    )

            # No mapping found
            raise PositionMappingError(
                "No mapping found for source position",
                migration_id=migration_id,
                source_position=source_position,
                reason="no_mapping",
            )

    async def translate_position_reverse(
        self,
        migration_id: UUID,
        target_position: Position,
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
                "target_position": target_position.to_str(),
            },
        ):
            mapping = await self._repo.find_by_target_position(
                migration_id,
                target_position,
            )

            if mapping is not None:
                logger.debug(
                    "Reverse position translation: target=%s -> source=%s",
                    target_position.to_str(),
                    mapping.source_position.to_str(),
                )
                return ReverseTranslationResult(
                    target_position=target_position,
                    source_position=mapping.source_position,
                    is_exact=True,
                )

            # No exact match found
            raise PositionMappingError(
                f"No mapping found for target position {target_position.to_str()}",
                migration_id=migration_id,
                reason="no_mapping",
            )

    async def translate_positions_batch(
        self,
        migration_id: UUID,
        source_positions: list[Position],
        *,
        use_nearest: bool = True,
    ) -> list[TranslationResult]:
        """
        Translate multiple source positions to target positions.

        Delegates to `translate_position` for each position. Positions are
        opaque tokens, so the previous int-range optimization (fetching a
        padded window of mappings and searching it in memory) no longer
        applies -- there is no arithmetic "buffer" to pad a token range
        with. Each lookup is still O(log n) via the repository's binary
        search over the row ordinal.

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
            results = [
                await self.translate_position(
                    migration_id,
                    source_pos,
                    use_nearest=use_nearest,
                )
                for source_pos in source_positions
            ]

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

    def _find_nearest(
        self,
        sorted_mappings: list[PositionMapping],
        source_position: Position,
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
