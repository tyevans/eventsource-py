"""PositionMappingRepository - Data access for event position mappings.

The PositionMappingRepository tracks the relationship between source store
positions and target store positions during migration. This enables:
- Checkpoint translation for subscriptions
- Verification of migration completeness
- Bi-directional position lookups during dual-write phase

Governed by:
    - ADR-0002 (<500 lines per module)
    - ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.sql.migration.position_mapping_helpers import row_to_mapping
from eventsource.adapters.sql.migration.position_mapping_mutation import (
    PostgreSQLPositionMappingMutationMixin,
)
from eventsource.adapters.sql.migration.position_mapping_query import (
    PostgreSQLPositionMappingQueryMixin,
)
from eventsource.adapters.sql.migration.position_mapping_search import (
    PostgreSQLPositionMappingSearchMixin,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.ports.migration.models import PositionMapping


class PostgreSQLPositionMappingRepository(
    PostgreSQLPositionMappingMutationMixin,
    PostgreSQLPositionMappingSearchMixin,
    PostgreSQLPositionMappingQueryMixin,
):
    """PostgreSQL implementation of PositionMappingRepository.

    Persists position mappings to the `migration_position_mappings` table.
    Provides efficient batch inserts during bulk copy and optimized lookups
    for dual-write and subscription catch-up phases.
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the repository.

        Args:
            conn: Database connection or engine
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn

    def _row_to_mapping(self, row: Sequence[Any]) -> PositionMapping:
        """Convert database row to PositionMapping instance.

        Delegates to position_mapping_helpers.row_to_mapping.
        """
        return row_to_mapping(row)


__all__ = [
    "PostgreSQLPositionMappingRepository",
    "sql_connection",
]
