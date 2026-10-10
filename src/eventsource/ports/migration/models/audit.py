"""
Migration audit log models and event types.

This module defines audit event classifications and audit log entries
for tracking migration lifecycle operations, state changes, errors,
and checkpoints.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from eventsource.ports.migration.models.phases import MigrationPhase


class AuditEventType(Enum):
    """
    Types of audit events for migration operations.

    These event types correspond to the CHECK constraint in the
    migration_audit_log table and represent significant migration
    lifecycle events for compliance and debugging purposes.

    Attributes:
        MIGRATION_STARTED: A new migration was initiated.
        PHASE_CHANGED: Migration transitioned between phases.
        MIGRATION_PAUSED: Migration was paused by operator.
        MIGRATION_RESUMED: Migration was resumed after pause.
        MIGRATION_ABORTED: Migration was cancelled by operator.
        MIGRATION_COMPLETED: Migration finished successfully.
        MIGRATION_FAILED: Migration failed due to error.
        ERROR_OCCURRED: A recoverable error was recorded.
        CUTOVER_INITIATED: Cutover process started.
        CUTOVER_COMPLETED: Cutover finished successfully.
        CUTOVER_ROLLED_BACK: Cutover was rolled back.
        VERIFICATION_STARTED: Consistency verification began.
        VERIFICATION_COMPLETED: Consistency verification passed.
        VERIFICATION_FAILED: Consistency verification failed.
        PROGRESS_CHECKPOINT: Periodic progress snapshot.
    """

    MIGRATION_STARTED = "migration_started"
    """A new migration was initiated."""

    PHASE_CHANGED = "phase_changed"
    """Migration transitioned between phases."""

    MIGRATION_PAUSED = "migration_paused"
    """Migration was paused by operator."""

    MIGRATION_RESUMED = "migration_resumed"
    """Migration was resumed after pause."""

    MIGRATION_ABORTED = "migration_aborted"
    """Migration was cancelled by operator."""

    MIGRATION_COMPLETED = "migration_completed"
    """Migration finished successfully."""

    MIGRATION_FAILED = "migration_failed"
    """Migration failed due to error."""

    ERROR_OCCURRED = "error_occurred"
    """A recoverable error was recorded."""

    CUTOVER_INITIATED = "cutover_initiated"
    """Cutover process started."""

    CUTOVER_COMPLETED = "cutover_completed"
    """Cutover finished successfully."""

    CUTOVER_ROLLED_BACK = "cutover_rolled_back"
    """Cutover was rolled back."""

    VERIFICATION_STARTED = "verification_started"
    """Consistency verification began."""

    VERIFICATION_COMPLETED = "verification_completed"
    """Consistency verification passed."""

    VERIFICATION_FAILED = "verification_failed"
    """Consistency verification failed."""

    PROGRESS_CHECKPOINT = "progress_checkpoint"
    """Periodic progress snapshot."""


@dataclass(frozen=True)
class MigrationAuditEntry:
    """
    Audit log entry for migration events.

    Used for compliance and debugging. This class is immutable
    because audit entries should never be modified.

    Attributes:
        id: Unique audit entry identifier (None for new entries).
        migration_id: Migration this entry belongs to.
        event_type: Type of audit event.
        old_phase: Previous phase (for phase changes).
        new_phase: New phase (for phase changes).
        details: Additional event details.
        operator: Who triggered the event.
        occurred_at: When the event occurred.
    """

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
        """
        Create an audit entry for a phase change.

        Args:
            migration_id: Migration ID.
            old_phase: Previous phase.
            new_phase: New phase.
            occurred_at: When change occurred.
            operator: Who triggered the change.
            details: Additional details.
            id: Audit entry ID (optional, set by database).

        Returns:
            MigrationAuditEntry instance.
        """
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
        """
        Create an audit entry for migration start.

        Args:
            migration_id: Migration ID.
            occurred_at: When migration started.
            operator: Who triggered the migration.
            details: Additional details (config, etc.).
            id: Audit entry ID (optional, set by database).

        Returns:
            MigrationAuditEntry instance.
        """
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
        """
        Create an audit entry for an error occurrence.

        Args:
            migration_id: Migration ID.
            occurred_at: When error occurred.
            error_message: The error message.
            error_type: Classification of the error.
            operator: Who was operating when error occurred.
            id: Audit entry ID (optional, set by database).

        Returns:
            MigrationAuditEntry instance.
        """
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
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation suitable for JSON.
        """
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
