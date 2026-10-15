"""Migration persistence protocol for tenant migration operations and lifecycle state management."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import Migration, MigrationPhase
from eventsource.ports.positions import Position


@runtime_checkable
class MigrationRepository(Protocol):
    """Protocol for tenant migration CRUD operations and state machine transitions."""

    async def create(self, migration: Migration) -> UUID:
        """Create a new migration record and return its ID."""
        ...

    async def get(self, migration_id: UUID) -> Migration | None:
        """Get a migration by ID, or None if not found."""
        ...

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        """Get the active migration for a tenant, or None."""
        ...

    async def update_phase(self, migration_id: UUID, new_phase: MigrationPhase) -> None:
        """Update migration phase with state machine validation."""
        ...

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        """Update bulk copy progress and last processed positions."""
        ...

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        """Set the total events count for progress tracking."""
        ...

    async def record_error(self, migration_id: UUID, error: str) -> None:
        """Record an error occurrence on the migration."""
        ...

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        """Set migration pause state and optional reason."""
        ...

    async def list_active(self) -> list[Migration]:
        """List all active (non-terminal) migrations."""
        ...


__all__ = ["MigrationRepository"]
