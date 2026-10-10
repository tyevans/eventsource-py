"""MigrationRepository - Data access for migration state.

The MigrationRepository provides CRUD operations for Migration entities,
including state management, progress tracking, and query capabilities
for migration management.

This module implements the MigrationRepository protocol and provides
a PostgreSQL implementation for production use.

Governed by:
    - ADR-0002 (<500 lines per module)
    - ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.sql.migration.migration_helpers import (
    VALID_TRANSITIONS,
    _position,
    _token,
    get_phase_timestamp_params,
    get_phase_timestamp_updates,
    row_to_migration,
)
from eventsource.adapters.sql.migration.migration_mutation import (
    PostgreSQLMigrationMutationMixin,
)
from eventsource.adapters.sql.migration.migration_progress import (
    PostgreSQLMigrationProgressMixin,
)
from eventsource.adapters.sql.migration.migration_query import (
    PostgreSQLMigrationQueryMixin,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
)


class PostgreSQLMigrationRepository(
    PostgreSQLMigrationMutationMixin,
    PostgreSQLMigrationProgressMixin,
    PostgreSQLMigrationQueryMixin,
):
    """PostgreSQL implementation of MigrationRepository.

    Persists migration state to the `tenant_migrations` table.
    Provides CRUD operations and state management with full
    observability through OpenTelemetry tracing.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLMigrationRepository(conn)
        ...     migration_id = await repo.create(migration)
        ...
        >>> # Get migration status
        >>> migration = await repo.get(migration_id)
        >>> print(f"Phase: {migration.phase.value}")
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

    def _row_to_migration(self, row: Sequence[Any]) -> Migration:
        """Convert database row to Migration instance.

        Delegates to migration_helpers.row_to_migration.
        """
        return row_to_migration(row)

    def _get_phase_timestamp_updates(self, phase: MigrationPhase) -> str:
        """Get SQL for updating phase-specific timestamps.

        Delegates to migration_helpers.get_phase_timestamp_updates.
        """
        return get_phase_timestamp_updates(phase)

    def _get_phase_timestamp_params(
        self,
        phase: MigrationPhase,
        now: datetime,
    ) -> dict[str, Any]:
        """Get params for phase-specific timestamps.

        Delegates to migration_helpers.get_phase_timestamp_params.
        """
        return get_phase_timestamp_params(phase, now)


__all__ = [
    "VALID_TRANSITIONS",
    "PostgreSQLMigrationRepository",
    "_position",
    "_token",
    "sql_connection",
]
