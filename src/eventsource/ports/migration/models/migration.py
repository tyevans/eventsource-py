"""Tenant migration operation data model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import UUID

from eventsource.ports.migration.models.config import MigrationConfig
from eventsource.ports.migration.models.phases import MigrationPhase
from eventsource.ports.positions import Position


@dataclass
class Migration:
    """Tracks the state and progress of migrating a tenant's events between stores."""

    id: UUID
    tenant_id: UUID
    source_store_id: str
    target_store_id: str
    phase: MigrationPhase = MigrationPhase.PENDING
    events_total: int = 0
    events_copied: int = 0
    last_source_position: Position | None = None
    last_target_position: Position | None = None
    started_at: datetime | None = None
    bulk_copy_started_at: datetime | None = None
    bulk_copy_completed_at: datetime | None = None
    dual_write_started_at: datetime | None = None
    cutover_started_at: datetime | None = None
    completed_at: datetime | None = None
    config: MigrationConfig = field(default_factory=MigrationConfig)
    error_count: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None
    is_paused: bool = False
    paused_at: datetime | None = None
    pause_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    created_by: str | None = None

    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage (0-100)."""
        if self.events_total == 0:
            return 0.0
        return min(100.0, (self.events_copied / self.events_total) * 100)

    @property
    def is_active(self) -> bool:
        """Check if migration is active (not terminal)."""
        return not self.phase.is_terminal

    @property
    def is_terminal(self) -> bool:
        """Check if migration has reached a terminal state."""
        return self.phase.is_terminal

    @property
    def events_remaining(self) -> int:
        """Calculate events remaining to copy."""
        return max(0, self.events_total - self.events_copied)

    @property
    def duration(self) -> timedelta | None:
        """Calculate total migration duration."""
        if self.started_at is None:
            return None
        return (self.completed_at or datetime.now()) - self.started_at

    @property
    def bulk_copy_duration(self) -> timedelta | None:
        """Calculate bulk copy phase duration."""
        if self.bulk_copy_started_at is None:
            return None
        return (self.bulk_copy_completed_at or datetime.now()) - self.bulk_copy_started_at

    @property
    def current_phase_started_at(self) -> datetime | None:
        """Get the start time of the current phase."""
        phase_start_times = {
            MigrationPhase.BULK_COPY: self.bulk_copy_started_at,
            MigrationPhase.DUAL_WRITE: self.dual_write_started_at,
            MigrationPhase.CUTOVER: self.cutover_started_at,
            MigrationPhase.COMPLETED: self.completed_at,
        }
        return phase_start_times.get(self.phase, self.started_at)

    def can_transition_to(self, target_phase: MigrationPhase) -> bool:
        """Check if transition to target phase is valid."""
        return self.phase.can_transition_to(target_phase)


__all__ = ["Migration"]
