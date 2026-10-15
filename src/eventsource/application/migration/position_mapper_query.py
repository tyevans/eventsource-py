"""Position translation, batch translation, and nearest-neighbor query operations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper_translation import (
    ReverseTranslationResult,
    TranslationResult,
)
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.observability import Tracer
    from eventsource.ports.migration.repositories import PositionMappingRepository

logger = logging.getLogger(__name__)


class PositionMapperQueryMixin:
    """Mixin providing position translation and query operations."""

    _repo: PositionMappingRepository
    _tracer: Tracer

    async def translate_position(
        self,
        migration_id: UUID,
        source_position: Position,
        *,
        use_nearest: bool = True,
    ) -> TranslationResult:
        """Translate a source position to target position via exact or nearest-neighbor lookup."""
        with self._tracer.span(
            "eventsource.position_mapper.translate_position",
            {
                "migration.id": str(migration_id),
                "source_position": source_position.to_str(),
                "use_nearest": use_nearest,
            },
        ):
            exact_mapping = await self._repo.find_by_source_position(migration_id, source_position)

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
        """Translate a target position back to source position."""
        with self._tracer.span(
            "eventsource.position_mapper.translate_position_reverse",
            {
                "migration.id": str(migration_id),
                "target_position": target_position.to_str(),
            },
        ):
            mapping = await self._repo.find_by_target_position(migration_id, target_position)

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
        """Translate multiple source positions to target positions."""
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

    def _find_nearest(
        self,
        sorted_mappings: list[PositionMapping],
        source_position: Position,
    ) -> PositionMapping | None:
        """Find the nearest mapping with source_position <= given position via binary search."""
        if not sorted_mappings:
            return None

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


__all__ = ["PositionMapperQueryMixin"]
