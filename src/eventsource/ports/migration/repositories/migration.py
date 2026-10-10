"""
Migration persistence protocol.

Defines the persistence contract for tenant migration operations and
lifecycle state management.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
)
from eventsource.ports.positions import Position


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
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        """
        Update bulk copy progress.

        Args:
            migration_id: UUID of the migration
            events_copied: Total events copied so far
            last_source_position: Last source position processed (opaque
                token; None when nothing has been copied yet)
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


__all__ = [
    "MigrationRepository",
]
