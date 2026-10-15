"""Routing, position mapping, sync lag, and cutover result data models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from eventsource.ports.migration.models.phases import TenantMigrationState
from eventsource.ports.positions import Position


@dataclass
class TenantRouting:
    """Mutable tenant store routing configuration and migration state."""

    tenant_id: UUID
    store_id: str
    migration_state: TenantMigrationState = TenantMigrationState.NORMAL
    active_migration_id: UUID | None = None
    target_store_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_migrating(self) -> bool:
        """Return True if a migration is currently in progress."""
        return self.migration_state.is_migrating

    @property
    def effective_store_id(self) -> str:
        """Return the store ID to use for reads (target store once MIGRATED)."""
        if self.migration_state == TenantMigrationState.MIGRATED:
            return self.target_store_id or self.store_id
        return self.store_id

    def can_transition_to(self, target_state: TenantMigrationState) -> bool:
        """Return True if transitioning to `target_state` is valid."""
        return self.migration_state.can_transition_to(target_state)


@dataclass(frozen=True)
class PositionMapping:
    """Immutable mapping from source store position to target store position."""

    migration_id: UUID
    source_position: Position
    target_position: Position
    event_id: UUID
    mapped_at: datetime


@dataclass(frozen=True)
class SyncLag:
    """Point-in-time synchronization lag between source and target stores.

    Attributes:
        events: Number of source events not yet copied to the target,
            counted exactly up to the sync threshold.
        source_position: Current source store position (reporting only;
            never compared with `target_position`).
        target_position: Current target store position (reporting only).
        timestamp: When this lag was measured.
        count_is_bounded: True when the count stopped at its bound and the
            real backlog may be larger.
    """

    events: int
    source_position: Position | None
    target_position: Position | None
    timestamp: datetime
    count_is_bounded: bool = False

    @property
    def is_converged(self) -> bool:
        """Return True if lag is zero."""
        return self.events == 0

    @property
    def lag_ms(self) -> float:
        """Estimate lag in milliseconds (assuming ~1ms per event)."""
        return float(self.events)

    def is_within_threshold(self, max_lag: int) -> bool:
        """Return True if lag is within `max_lag` and is not a bounded count."""
        if self.count_is_bounded:
            return False
        return self.events <= max_lag


@dataclass(frozen=True)
class CutoverResult:
    """Outcome and timing details of a cutover operation."""

    success: bool
    duration_ms: float
    events_synced: int = 0
    error_message: str | None = None
    rolled_back: bool = False

    @property
    def within_timeout(self) -> bool:
        """Return True if cutover completed within the 100ms SLA."""
        return self.duration_ms < 100.0


__all__ = [
    "CutoverResult",
    "PositionMapping",
    "SyncLag",
    "TenantRouting",
]
