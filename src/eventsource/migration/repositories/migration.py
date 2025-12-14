"""
MigrationRepository - Data access for migration state.

The MigrationRepository provides CRUD operations for Migration entities,
including state management, progress tracking, and query capabilities
for migration management.

This module implements the MigrationRepository protocol and provides
a PostgreSQL implementation for production use.

Responsibilities:
    - Create and persist new migrations
    - Update migration state and progress
    - Query migrations by tenant, state, or ID
    - Handle concurrent updates safely
    - Validate phase transitions

Database Table:
    Uses the `tenant_migrations` table defined in PREREQ-002.

Usage:
    >>> from eventsource.migration.repositories import (
    ...     MigrationRepository,
    ...     PostgreSQLMigrationRepository,
    ... )
    >>>
    >>> repo = PostgreSQLMigrationRepository(conn)
    >>>
    >>> # Create migration
    >>> migration_id = await repo.create(migration)
    >>>
    >>> # Update state
    >>> await repo.update_phase(migration.id, MigrationPhase.BULK_COPY)
    >>>
    >>> # Find by tenant
    >>> active = await repo.get_by_tenant(tenant_id)

See Also:
    - Task: P1-003-migration-repository.md
    - Schema: PREREQ-002-migration-schema.md
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationNotFoundError,
)
from eventsource.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM, ATTR_TENANT_ID
from eventsource.repositories._connection import execute_with_connection

# Valid phase transitions for the migration state machine
VALID_TRANSITIONS: dict[MigrationPhase, set[MigrationPhase]] = {
    MigrationPhase.PENDING: {
        MigrationPhase.BULK_COPY,
        MigrationPhase.ABORTED,
    },
    MigrationPhase.BULK_COPY: {
        MigrationPhase.DUAL_WRITE,
        MigrationPhase.ABORTED,
        MigrationPhase.FAILED,
    },
    MigrationPhase.DUAL_WRITE: {
        MigrationPhase.CUTOVER,
        MigrationPhase.ABORTED,
        MigrationPhase.FAILED,
    },
    MigrationPhase.CUTOVER: {
        MigrationPhase.COMPLETED,
        MigrationPhase.DUAL_WRITE,  # Rollback
        MigrationPhase.FAILED,
    },
    MigrationPhase.COMPLETED: set(),  # Terminal
    MigrationPhase.ABORTED: set(),  # Terminal
    MigrationPhase.FAILED: set(),  # Terminal
}


@runtime_checkable
class MigrationRepository(Protocol):
    """
    Protocol for migration persistence.

    Provides CRUD operations and state management for tenant migrations.
    Implementations must handle concurrent access safely and validate
    phase transitions according to the migration state machine.
    """

    async def create(self, migration: Migration) -> UUID:
        """
        Create a new migration record.

        Args:
            migration: Migration instance to persist

        Returns:
            The migration ID

        Raises:
            MigrationAlreadyExistsError: If active migration exists for tenant
        """
        ...

    async def get(self, migration_id: UUID) -> Migration | None:
        """
        Get a migration by ID.

        Args:
            migration_id: UUID of the migration

        Returns:
            Migration instance or None if not found
        """
        ...

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        """
        Get the active migration for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            Active Migration instance or None
        """
        ...

    async def update_phase(
        self,
        migration_id: UUID,
        new_phase: MigrationPhase,
    ) -> None:
        """
        Update migration phase with validation.

        Args:
            migration_id: UUID of the migration
            new_phase: New phase to transition to

        Raises:
            MigrationNotFoundError: If migration not found
            InvalidPhaseTransitionError: If transition is invalid
        """
        ...

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: int,
        last_target_position: int | None = None,
    ) -> None:
        """
        Update bulk copy progress.

        Args:
            migration_id: UUID of the migration
            events_copied: Total events copied so far
            last_source_position: Last source position processed
            last_target_position: Last target position written (optional)
        """
        ...

    async def set_events_total(
        self,
        migration_id: UUID,
        events_total: int,
    ) -> None:
        """
        Set the total events count for progress tracking.

        Args:
            migration_id: UUID of the migration
            events_total: Total events to migrate
        """
        ...

    async def record_error(
        self,
        migration_id: UUID,
        error: str,
    ) -> None:
        """
        Record an error occurrence.

        Args:
            migration_id: UUID of the migration
            error: Error message
        """
        ...

    async def set_paused(
        self,
        migration_id: UUID,
        paused: bool,
        reason: str | None = None,
    ) -> None:
        """
        Set migration pause state.

        Args:
            migration_id: UUID of the migration
            paused: Whether to pause or resume
            reason: Reason for pausing (if paused=True)
        """
        ...

    async def list_active(self) -> list[Migration]:
        """
        List all active (non-terminal) migrations.

        Returns:
            List of active Migration instances
        """
        ...


class PostgreSQLMigrationRepository:
    """
    PostgreSQL implementation of MigrationRepository.

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

    async def create(self, migration: Migration) -> UUID:
        """
        Create a new migration record.

        Creates a new migration in PENDING phase. Validates that no
        active migration exists for the tenant to prevent duplicate
        migrations.

        Args:
            migration: Migration instance to persist

        Returns:
            The migration ID

        Raises:
            MigrationAlreadyExistsError: If active migration exists for tenant
        """
        with self._tracer.span(
            "eventsource.migration_repo.create",
            {
                "migration.id": str(migration.id),
                ATTR_TENANT_ID: str(migration.tenant_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            # Check for existing active migration
            existing = await self.get_by_tenant(migration.tenant_id)
            if existing is not None:
                raise MigrationAlreadyExistsError(
                    migration.tenant_id,
                    existing.id,
                )

            now = datetime.now(UTC)

            query = text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position, last_target_position,
                    config, error_count, created_at, updated_at, created_by
                ) VALUES (
                    :id, :tenant_id, :source_store_id, :target_store_id,
                    :phase, :events_total, :events_copied,
                    :last_source_position, :last_target_position,
                    :config, :error_count, :created_at, :updated_at, :created_by
                )
            """)

            params = {
                "id": migration.id,
                "tenant_id": migration.tenant_id,
                "source_store_id": migration.source_store_id,
                "target_store_id": migration.target_store_id,
                "phase": migration.phase.value,
                "events_total": migration.events_total,
                "events_copied": migration.events_copied,
                "last_source_position": migration.last_source_position,
                "last_target_position": migration.last_target_position,
                "config": json.dumps(migration.config.to_dict()),
                "error_count": migration.error_count,
                "created_at": now,
                "updated_at": now,
                "created_by": migration.created_by,
            }

            async with execute_with_connection(self._conn, transactional=True) as conn:
                await conn.execute(query, params)

            return migration.id

    async def get(self, migration_id: UUID) -> Migration | None:
        """
        Get a migration by ID.

        Args:
            migration_id: UUID of the migration

        Returns:
            Migration instance or None if not found
        """
        with self._tracer.span(
            "eventsource.migration_repo.get",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position, last_target_position,
                    started_at, bulk_copy_started_at, bulk_copy_completed_at,
                    dual_write_started_at, cutover_started_at, completed_at,
                    config, error_count, last_error, last_error_at,
                    is_paused, paused_at, pause_reason,
                    created_at, updated_at, created_by
                FROM tenant_migrations
                WHERE id = :id
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"id": migration_id})
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_migration(row)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        """
        Get the active migration for a tenant.

        Returns the migration if one is active (not in terminal phase).
        Terminal phases are: COMPLETED, ABORTED, FAILED.

        Args:
            tenant_id: Tenant UUID

        Returns:
            Active Migration instance or None
        """
        with self._tracer.span(
            "eventsource.migration_repo.get_by_tenant",
            {
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position, last_target_position,
                    started_at, bulk_copy_started_at, bulk_copy_completed_at,
                    dual_write_started_at, cutover_started_at, completed_at,
                    config, error_count, last_error, last_error_at,
                    is_paused, paused_at, pause_reason,
                    created_at, updated_at, created_by
                FROM tenant_migrations
                WHERE tenant_id = :tenant_id
                  AND phase NOT IN ('completed', 'aborted', 'failed')
                LIMIT 1
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"tenant_id": tenant_id})
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_migration(row)

    async def update_phase(
        self,
        migration_id: UUID,
        new_phase: MigrationPhase,
    ) -> None:
        """
        Update migration phase with validation.

        Validates the phase transition against the state machine before
        applying the update. Also updates phase-specific timestamps
        (e.g., started_at, bulk_copy_started_at, etc.).

        Args:
            migration_id: UUID of the migration
            new_phase: New phase to transition to

        Raises:
            MigrationNotFoundError: If migration not found
            InvalidPhaseTransitionError: If transition is invalid
        """
        with self._tracer.span(
            "eventsource.migration_repo.update_phase",
            {
                "migration.id": str(migration_id),
                "migration.new_phase": new_phase.value,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            # Get current phase
            migration = await self.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            # Validate transition
            valid_transitions = VALID_TRANSITIONS.get(migration.phase, set())
            if new_phase not in valid_transitions:
                raise InvalidPhaseTransitionError(
                    migration_id,
                    migration.phase,
                    new_phase,
                )

            # Build update query with phase-specific timestamps
            now = datetime.now(UTC)
            timestamp_updates = self._get_phase_timestamp_updates(new_phase)

            # timestamp_updates is hardcoded SQL from internal enum mapping
            query = text(f"""
                UPDATE tenant_migrations
                SET phase = :phase,
                    {timestamp_updates}
                    updated_at = :updated_at
                WHERE id = :id
            """)  # nosec B608 - all data values are parameterized

            params = {
                "id": migration_id,
                "phase": new_phase.value,
                "updated_at": now,
                **self._get_phase_timestamp_params(new_phase, now),
            }

            async with execute_with_connection(self._conn, transactional=True) as conn:
                await conn.execute(query, params)

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: int,
        last_target_position: int | None = None,
    ) -> None:
        """
        Update bulk copy progress.

        Updates the progress tracking fields for the migration, including
        the number of events copied and the last processed positions.

        Args:
            migration_id: UUID of the migration
            events_copied: Total events copied so far
            last_source_position: Last source position processed
            last_target_position: Last target position written (optional)
        """
        with self._tracer.span(
            "eventsource.migration_repo.update_progress",
            {
                "migration.id": str(migration_id),
                "migration.events_copied": events_copied,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            if last_target_position is not None:
                query = text("""
                    UPDATE tenant_migrations
                    SET events_copied = :events_copied,
                        last_source_position = :last_source_position,
                        last_target_position = :last_target_position,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "events_copied": events_copied,
                    "last_source_position": last_source_position,
                    "last_target_position": last_target_position,
                    "updated_at": now,
                }
            else:
                query = text("""
                    UPDATE tenant_migrations
                    SET events_copied = :events_copied,
                        last_source_position = :last_source_position,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "events_copied": events_copied,
                    "last_source_position": last_source_position,
                    "updated_at": now,
                }

            async with execute_with_connection(self._conn, transactional=True) as conn:
                await conn.execute(query, params)

    async def set_events_total(
        self,
        migration_id: UUID,
        events_total: int,
    ) -> None:
        """
        Set the total events count.

        Sets the total number of events to be migrated. This is typically
        determined at the start of the bulk copy phase.

        Args:
            migration_id: UUID of the migration
            events_total: Total events to migrate
        """
        with self._tracer.span(
            "eventsource.migration_repo.set_events_total",
            {
                "migration.id": str(migration_id),
                "migration.events_total": events_total,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                UPDATE tenant_migrations
                SET events_total = :events_total,
                    updated_at = :updated_at
                WHERE id = :id
            """)

            async with execute_with_connection(self._conn, transactional=True) as conn:
                await conn.execute(
                    query,
                    {
                        "id": migration_id,
                        "events_total": events_total,
                        "updated_at": datetime.now(UTC),
                    },
                )

    async def record_error(
        self,
        migration_id: UUID,
        error: str,
    ) -> None:
        """
        Record an error occurrence.

        Increments the error count and stores the error message.
        Error messages are truncated to 1000 characters.

        Args:
            migration_id: UUID of the migration
            error: Error message
        """
        with self._tracer.span(
            "eventsource.migration_repo.record_error",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            query = text("""
                UPDATE tenant_migrations
                SET error_count = error_count + 1,
                    last_error = :error,
                    last_error_at = :error_at,
                    updated_at = :updated_at
                WHERE id = :id
            """)

            async with execute_with_connection(self._conn, transactional=True) as conn:
                await conn.execute(
                    query,
                    {
                        "id": migration_id,
                        "error": error[:1000],  # Truncate long errors
                        "error_at": now,
                        "updated_at": now,
                    },
                )

    async def set_paused(
        self,
        migration_id: UUID,
        paused: bool,
        reason: str | None = None,
    ) -> None:
        """
        Set migration pause state.

        Pauses or resumes a migration. When pausing, stores the reason
        and timestamp. When resuming, clears the pause-related fields.

        Args:
            migration_id: UUID of the migration
            paused: Whether to pause or resume
            reason: Reason for pausing (if paused=True)
        """
        with self._tracer.span(
            "eventsource.migration_repo.set_paused",
            {
                "migration.id": str(migration_id),
                "migration.paused": paused,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)

            if paused:
                query = text("""
                    UPDATE tenant_migrations
                    SET is_paused = TRUE,
                        paused_at = :paused_at,
                        pause_reason = :reason,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "paused_at": now,
                    "reason": reason,
                    "updated_at": now,
                }
            else:
                query = text("""
                    UPDATE tenant_migrations
                    SET is_paused = FALSE,
                        paused_at = NULL,
                        pause_reason = NULL,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                params = {
                    "id": migration_id,
                    "updated_at": now,
                }

            async with execute_with_connection(self._conn, transactional=True) as conn:
                await conn.execute(query, params)

    async def list_active(self) -> list[Migration]:
        """
        List all active migrations.

        Returns all migrations that are not in a terminal phase
        (COMPLETED, ABORTED, FAILED), ordered by creation time.

        Returns:
            List of active Migration instances
        """
        with self._tracer.span(
            "eventsource.migration_repo.list_active",
            {ATTR_DB_SYSTEM: "postgresql"},
        ):
            query = text("""
                SELECT
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position, last_target_position,
                    started_at, bulk_copy_started_at, bulk_copy_completed_at,
                    dual_write_started_at, cutover_started_at, completed_at,
                    config, error_count, last_error, last_error_at,
                    is_paused, paused_at, pause_reason,
                    created_at, updated_at, created_by
                FROM tenant_migrations
                WHERE phase NOT IN ('completed', 'aborted', 'failed')
                ORDER BY created_at ASC
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {})
                rows = result.fetchall()

            return [self._row_to_migration(row) for row in rows]

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _row_to_migration(self, row: Sequence[Any]) -> Migration:
        """
        Convert database row to Migration instance.

        Handles JSON deserialization for the config field and converts
        string phase values to MigrationPhase enum.

        Args:
            row: Database row tuple

        Returns:
            Migration instance
        """
        config_data = row[15] if isinstance(row[15], dict) else json.loads(row[15])

        return Migration(
            id=row[0],
            tenant_id=row[1],
            source_store_id=row[2],
            target_store_id=row[3],
            phase=MigrationPhase(row[4]),
            events_total=row[5] or 0,
            events_copied=row[6] or 0,
            last_source_position=row[7] or 0,
            last_target_position=row[8] or 0,
            started_at=row[9],
            bulk_copy_started_at=row[10],
            bulk_copy_completed_at=row[11],
            dual_write_started_at=row[12],
            cutover_started_at=row[13],
            completed_at=row[14],
            config=MigrationConfig.from_dict(config_data),
            error_count=row[16] or 0,
            last_error=row[17],
            last_error_at=row[18],
            is_paused=row[19] or False,
            paused_at=row[20],
            pause_reason=row[21],
            created_at=row[22],
            updated_at=row[23],
            created_by=row[24],
        )

    def _get_phase_timestamp_updates(self, phase: MigrationPhase) -> str:
        """
        Get SQL for updating phase-specific timestamps.

        Different phases have different timestamp fields that need
        to be updated when transitioning to that phase.

        Args:
            phase: The target phase

        Returns:
            SQL fragment for timestamp updates
        """
        timestamp_map = {
            MigrationPhase.BULK_COPY: (
                "started_at = :phase_time, bulk_copy_started_at = :phase_time,"
            ),
            MigrationPhase.DUAL_WRITE: (
                "bulk_copy_completed_at = :phase_time, dual_write_started_at = :phase_time,"
            ),
            MigrationPhase.CUTOVER: "cutover_started_at = :phase_time,",
            MigrationPhase.COMPLETED: "completed_at = :phase_time,",
            MigrationPhase.ABORTED: "completed_at = :phase_time,",
            MigrationPhase.FAILED: "completed_at = :phase_time,",
        }
        return timestamp_map.get(phase, "")

    def _get_phase_timestamp_params(
        self,
        phase: MigrationPhase,
        now: datetime,
    ) -> dict[str, Any]:
        """
        Get params for phase-specific timestamps.

        Args:
            phase: The target phase
            now: Current timestamp

        Returns:
            Dictionary of timestamp parameters
        """
        if phase in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
            MigrationPhase.CUTOVER,
            MigrationPhase.COMPLETED,
            MigrationPhase.ABORTED,
            MigrationPhase.FAILED,
        ):
            return {"phase_time": now}
        return {}


# Type alias for backwards compatibility
MigrationRepositoryProtocol = MigrationRepository
