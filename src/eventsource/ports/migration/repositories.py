"""
Protocols for migration persistence.

These Protocols define the persistence contracts used by the live-migration
use cases in `eventsource.application.migration`. They are colocated in the
ports ring (not adapters) because they are pure interfaces over the
migration data models -- no sqlalchemy or other driver dependency appears
here or in `eventsource.ports.migration.models`. Concrete implementations
(`PostgreSQLMigrationRepository`, etc.) live in
`eventsource.adapters.sql.migration`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.migration.models import (
    AuditEventType,
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
    PositionMapping,
    TenantMigrationState,
    TenantRouting,
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


@runtime_checkable
class TenantRoutingRepository(Protocol):
    """
    Protocol for tenant routing persistence.

    Manages tenant-to-store mappings and tracks migration routing state.
    Implementations must handle concurrent access safely and provide
    cache-friendly access patterns.
    """

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """
        Get routing configuration for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            TenantRouting instance or None if not configured
        """
        ...

    async def get_or_default(
        self,
        tenant_id: UUID,
        default_store_id: str,
    ) -> TenantRouting:
        """
        Get routing configuration, creating default if not exists.

        Uses UPSERT semantics to atomically create a default routing
        if one doesn't exist for the tenant.

        Args:
            tenant_id: Tenant UUID
            default_store_id: Default store ID if not configured

        Returns:
            TenantRouting instance (existing or newly created)
        """
        ...

    async def set_routing(
        self,
        tenant_id: UUID,
        store_id: str,
    ) -> None:
        """
        Set or update the store for a tenant.

        Uses UPSERT semantics to create or update the routing.
        Resets migration state to NORMAL if updating an existing routing.

        Args:
            tenant_id: Tenant UUID
            store_id: Target store identifier
        """
        ...

    async def set_migration_state(
        self,
        tenant_id: UUID,
        state: TenantMigrationState,
        migration_id: UUID | None = None,
    ) -> None:
        """
        Update the migration state for routing decisions.

        This method updates the routing state that determines how
        the TenantStoreRouter handles operations for this tenant.

        Args:
            tenant_id: Tenant UUID
            state: New migration state
            migration_id: Active migration ID (if applicable)
        """
        ...

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        """
        Reset migration state to NORMAL.

        Called after migration completes or aborts to return
        the tenant to normal routing behavior.

        Args:
            tenant_id: Tenant UUID
        """
        ...

    async def list_by_state(
        self,
        state: TenantMigrationState,
    ) -> list[TenantRouting]:
        """
        List tenants in a specific migration state.

        Useful for finding all tenants currently in dual-write mode
        or other migration states.

        Args:
            state: Migration state to filter by

        Returns:
            List of TenantRouting instances
        """
        ...

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """
        List tenants routed to a specific store.

        Useful for finding all tenants on a shared store before
        planning migrations.

        Args:
            store_id: Store identifier

        Returns:
            List of TenantRouting instances
        """
        ...


@runtime_checkable
class PositionMappingRepository(Protocol):
    """
    Protocol for position mapping persistence.

    Stores and retrieves source-to-target position mappings for
    subscription checkpoint translation during migration.
    Implementations must handle concurrent access safely and
    provide efficient range queries.
    """

    async def create(self, mapping: PositionMapping) -> int:
        """
        Create a new position mapping.

        Args:
            mapping: PositionMapping instance to persist

        Returns:
            The database ID of the created mapping

        Raises:
            IntegrityError: If mapping already exists for this migration/source_position
        """
        ...

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        """
        Create multiple position mappings in a single transaction.

        Optimized for bulk copy operations where many mappings
        need to be recorded efficiently.

        Args:
            mappings: List of PositionMapping instances to persist

        Returns:
            Number of mappings created
        """
        ...

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """
        Get a position mapping by its database ID.

        Args:
            mapping_id: Database ID of the mapping

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """
        Find mapping by exact source position.

        Args:
            migration_id: UUID of the migration
            source_position: Exact source position to find

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: Position,
    ) -> PositionMapping | None:
        """
        Find mapping by exact target position.

        Args:
            migration_id: UUID of the migration
            target_position: Exact target position to find

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """
        Find the nearest mapping with source_position <= given position.

        Used for checkpoint translation when exact position mapping
        doesn't exist. Returns the closest mapping at or before the
        given source position.

        Args:
            migration_id: UUID of the migration
            source_position: Source position to find nearest mapping for

        Returns:
            PositionMapping with highest source_position <= given position,
            or None if no such mapping exists
        """
        ...

    async def find_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """
        Find mapping by event ID.

        Useful for debugging and verification.

        Args:
            migration_id: UUID of the migration
            event_id: UUID of the event

        Returns:
            PositionMapping instance or None if not found
        """
        ...

    async def list_by_migration(
        self,
        migration_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PositionMapping]:
        """
        List mappings for a migration with pagination.

        Results are ordered by source_position ascending.

        Args:
            migration_id: UUID of the migration
            limit: Maximum number of results (default 100)
            offset: Number of results to skip (default 0)

        Returns:
            List of PositionMapping instances
        """
        ...

    async def list_in_source_range(
        self,
        migration_id: UUID,
        start_position: Position,
        end_position: Position,
    ) -> list[PositionMapping]:
        """
        List mappings within a source position range.

        Returns all mappings where start_position <= source_position <= end_position.
        Results are ordered by source_position ascending.

        Args:
            migration_id: UUID of the migration
            start_position: Start of source position range (inclusive)
            end_position: End of source position range (inclusive)

        Returns:
            List of PositionMapping instances
        """
        ...

    async def count_by_migration(self, migration_id: UUID) -> int:
        """
        Count total mappings for a migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            Number of mappings
        """
        ...

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[Position, Position] | None:
        """
        Get min and max source positions for a migration.

        Useful for understanding the range of migrated events.

        Args:
            migration_id: UUID of the migration

        Returns:
            Tuple of (min_source_position, max_source_position) or None if no mappings
        """
        ...

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """
        Delete all mappings for a migration.

        Called during migration cleanup or when re-starting a failed migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            Number of mappings deleted
        """
        ...


@runtime_checkable
class MigrationAuditLogRepository(Protocol):
    """
    Protocol for migration audit log persistence.

    Provides append-only operations for recording audit events and
    query operations for compliance reporting and debugging.

    Implementations must ensure:
    - Audit entries are immutable once written
    - Timestamps are accurate and use UTC
    - All required fields are properly validated
    """

    async def record(self, entry: MigrationAuditEntry) -> int:
        """
        Record an audit log entry.

        Args:
            entry: The audit entry to record (id field will be ignored)

        Returns:
            The generated ID for the audit entry
        """
        ...

    async def get_by_migration(
        self,
        migration_id: UUID,
        event_types: list[AuditEventType] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[MigrationAuditEntry]:
        """
        Get audit entries for a migration.

        Args:
            migration_id: The migration ID to query
            event_types: Optional filter by event types
            since: Optional filter for entries after this time
            until: Optional filter for entries before this time
            limit: Optional maximum number of entries to return

        Returns:
            List of audit entries, ordered by occurred_at ascending
        """
        ...

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        """
        Get an audit entry by ID.

        Args:
            entry_id: The audit entry ID

        Returns:
            The audit entry or None if not found
        """
        ...

    async def get_latest(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> MigrationAuditEntry | None:
        """
        Get the most recent audit entry for a migration.

        Args:
            migration_id: The migration ID to query
            event_type: Optional filter by event type

        Returns:
            The most recent audit entry or None if none exist
        """
        ...

    async def count_by_migration(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> int:
        """
        Count audit entries for a migration.

        Args:
            migration_id: The migration ID to query
            event_type: Optional filter by event type

        Returns:
            Number of matching audit entries
        """
        ...


__all__ = [
    "MigrationAuditLogRepository",
    "MigrationRepository",
    "PositionMappingRepository",
    "TenantRoutingRepository",
]
