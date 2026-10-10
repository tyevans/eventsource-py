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

from typing import TYPE_CHECKING

from eventsource.application.migration.position_mapper_query import PositionMapperQueryMixin
from eventsource.application.migration.position_mapper_recording import (
    PositionMapperRecordingMixin,
)
from eventsource.application.migration.position_mapper_translation import (
    ReverseTranslationResult,
    TranslationResult,
)
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.ports.migration.repositories import PositionMappingRepository


class PositionMapper(PositionMapperRecordingMixin, PositionMapperQueryMixin):
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


__all__ = [
    "PositionMapper",
    "ReverseTranslationResult",
    "TranslationResult",
]
