"""Migration audit log models and event types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from eventsource.ports.migration.models.phases import MigrationPhase


class AuditEventType(Enum):
    """Types of audit events for migration operations."""

    MIGRATION_STARTED = "migration_started"
    PHASE_CHANGED = "phase_changed"
    MIGRATION_PAUSED = "migration_paused"
    MIGRATION_RESUMED = "migration_resumed"
    MIGRATION_ABORTED = "migration_aborted"
    MIGRATION_COMPLETED = "migration_completed"
    MIGRATION_FAILED = "migration_failed"
    ERROR_OCCURRED = "error_occurred"
    CUTOVER_INITIATED = "cutover_initiated"
    CUTOVER_COMPLETED = "cutover_completed"
    CUTOVER_ROLLED_BACK = "cutover_rolled_back"
    VERIFICATION_STARTED = "verification_started"
    VERIFICATION_COMPLETED = "verification_completed"
    VERIFICATION_FAILED = "verification_failed"
    PROGRESS_CHECKPOINT = "progress_checkpoint"


@dataclass(frozen=True)
class MigrationAuditEntry:
    """Immutable audit log entry for migration events."""

    id: int | None
    migration_id: UUID
    event_type: AuditEventType
    old_phase: MigrationPhase | None
    new_phase: MigrationPhase | None
    details: dict[str, Any] | None
    operator: str | None
    occurred_at: datetime

    @classmethod
    def phase_change(
        cls,
        migration_id: UUID,
        old_phase: MigrationPhase,
        new_phase: MigrationPhase,
        occurred_at: datetime,
        operator: str | None = None,
        details: dict[str, Any] | None = None,
        id: int | None = None,
    ) -> MigrationAuditEntry:
        """Create an audit entry for a phase change."""
        return cls(
            id=id,
            migration_id=migration_id,
            event_type=AuditEventType.PHASE_CHANGED,
            old_phase=old_phase,
            new_phase=new_phase,
            details=details,
            operator=operator,
            occurred_at=occurred_at,
        )

    @classmethod
    def migration_started(
        cls,
        migration_id: UUID,
        occurred_at: datetime,
        operator: str | None = None,
        details: dict[str, Any] | None = None,
        id: int | None = None,
    ) -> MigrationAuditEntry:
        """Create an audit entry for migration start."""
        return cls(
            id=id,
            migration_id=migration_id,
            event_type=AuditEventType.MIGRATION_STARTED,
            old_phase=None,
            new_phase=MigrationPhase.PENDING,
            details=details,
            operator=operator,
            occurred_at=occurred_at,
        )

    @classmethod
    def error_occurred(
        cls,
        migration_id: UUID,
        occurred_at: datetime,
        error_message: str,
        error_type: str | None = None,
        operator: str | None = None,
        id: int | None = None,
    ) -> MigrationAuditEntry:
        """Create an audit entry for an error occurrence."""
        return cls(
            id=id,
            migration_id=migration_id,
            event_type=AuditEventType.ERROR_OCCURRED,
            old_phase=None,
            new_phase=None,
            details={
                "error_message": error_message,
                "error_type": error_type,
            },
            operator=operator,
            occurred_at=occurred_at,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "migration_id": str(self.migration_id),
            "event_type": self.event_type.value,
            "old_phase": self.old_phase.value if self.old_phase else None,
            "new_phase": self.new_phase.value if self.new_phase else None,
            "details": self.details,
            "operator": self.operator,
            "occurred_at": self.occurred_at.isoformat(),
        }


__all__ = [
    "AuditEventType",
    "MigrationAuditEntry",
]
