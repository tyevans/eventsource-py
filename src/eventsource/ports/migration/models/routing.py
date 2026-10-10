"""
Routing, position mapping, sync lag, and cutover result data models.

This module defines models for tenant store routing, position mapping
between source and target event stores, lag tracking during dual-write,
and cutover operation outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from eventsource.ports.migration.models.phases import TenantMigrationState
from eventsource.ports.positions import Position


@dataclass
class TenantRouting:
    """
    Tenant routing configuration.

    Determines which store handles operations for a tenant
    and tracks migration state for routing decisions.

    This is a mutable dataclass because routing state changes
    during migration.

    Attributes:
        tenant_id: Tenant identifier.
        store_id: Primary store identifier.
        migration_state: Current migration state.
        active_migration_id: ID of active migration, if any.
        target_store_id: Target store during migration.
        created_at: When routing was created.
        updated_at: When routing was last updated.
    """

    tenant_id: UUID
    store_id: str
    migration_state: TenantMigrationState = TenantMigrationState.NORMAL
    active_migration_id: UUID | None = None
    target_store_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_migrating(self) -> bool:
        """
        Check if tenant is currently migrating.

        Returns:
            True if migration is in progress.
        """
        return self.migration_state.is_migrating

    @property
    def effective_store_id(self) -> str:
        """
        Get the effective store ID for reads.

        After migration completes (MIGRATED state), reads
        should come from the target store.

        Returns:
            The store ID to use for reads.
        """
        if self.migration_state == TenantMigrationState.MIGRATED:
            return self.target_store_id or self.store_id
        return self.store_id

    def can_transition_to(self, target_state: TenantMigrationState) -> bool:
        """
        Check if transition to target state is valid.

        Args:
            target_state: The target state.

        Returns:
            True if the transition is valid.
        """
        return self.migration_state.can_transition_to(target_state)


@dataclass(frozen=True)
class PositionMapping:
    """
    Maps source position to target position.

    Used for checkpoint translation during subscription migration.
    This class is immutable because position mappings should not
    change once created.

    Attributes:
        migration_id: Migration this mapping belongs to.
        source_position: Position in source store.
        target_position: Corresponding position in target store.
        event_id: Event ID at this position.
        mapped_at: When mapping was created.
    """

    migration_id: UUID
    source_position: Position
    target_position: Position
    event_id: UUID
    mapped_at: datetime


@dataclass(frozen=True)
class SyncLag:
    """
    Synchronization lag between source and target stores.

    Tracks how far behind the target store is during dual-write phase.
    This class is immutable because it represents a point-in-time
    measurement.

    Attributes:
        events: Number of source events not yet copied to the target,
            counted exactly up to the sync threshold.
        count_is_bounded: True when the count hit its bound and stopped
            reading, i.e. the real backlog is `events` or more.
        source_position: Current source store position. REPORTING ONLY --
            never compared with `target_position` (they come from
            different stores; ordering them raises `PositionForeignError`).
        target_position: Current target store position. REPORTING ONLY --
            see `source_position`.
        timestamp: When this lag was measured.
    """

    events: int
    """Number of events behind, exact up to the sync threshold."""

    source_position: Position | None
    """Current source store position (reporting only, never compared)."""

    target_position: Position | None
    """Current target store position (reporting only, never compared)."""

    timestamp: datetime
    """When this lag was measured."""

    count_is_bounded: bool = False
    """True when the count stopped at its bound; the real backlog may be larger."""

    @property
    def is_converged(self) -> bool:
        """
        Check if stores are fully synchronized.

        Returns:
            True if lag is zero.
        """
        return self.events == 0

    @property
    def lag_ms(self) -> float:
        """
        Estimate lag in milliseconds (assuming ~1ms per event).

        This is a rough estimate based on typical event processing time.

        Returns:
            Estimated lag in milliseconds.
        """
        return float(self.events)

    def is_within_threshold(self, max_lag: int) -> bool:
        """
        Check if lag is within acceptable threshold for cutover.

        A bounded count never satisfies a threshold: `events` is then a
        lower bound standing for an unknown larger backlog, so answering
        True would be answering on no evidence.

        Args:
            max_lag: Maximum acceptable lag in events.

        Returns:
            True if lag is within threshold and is not a bounded count.
        """
        if self.count_is_bounded:
            return False
        return self.events <= max_lag


@dataclass(frozen=True)
class CutoverResult:
    """
    Result of a cutover operation.

    Contains details about whether cutover succeeded and timing.
    This class is immutable because it represents a completed operation.

    Attributes:
        success: Whether cutover succeeded.
        duration_ms: How long cutover took in milliseconds.
        events_synced: Events synced during cutover pause.
        error_message: Error message if failed.
        rolled_back: Whether rollback was performed.
    """

    success: bool
    duration_ms: float
    events_synced: int = 0
    error_message: str | None = None
    rolled_back: bool = False

    @property
    def within_timeout(self) -> bool:
        """
        Check if cutover completed within typical SLA.

        The standard SLA is sub-100ms cutover pause.

        Returns:
            True if cutover was within 100ms.
        """
        return self.duration_ms < 100.0


__all__ = [
    "CutoverResult",
    "PositionMapping",
    "SyncLag",
    "TenantRouting",
]
