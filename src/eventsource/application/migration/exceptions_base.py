"""Base migration exceptions and lifecycle/state errors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.migration.error_classification import (
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.domain.exceptions import EventSourceError

if TYPE_CHECKING:
    from eventsource.ports.migration.models import MigrationPhase


class MigrationError(EventSourceError):
    """Base exception for all migration-related errors with severity and recoverability metadata."""

    _default_classification: ErrorClassification = ErrorClassification(
        ErrorSeverity.ERROR,
        ErrorRecoverability.FATAL,
        "MIGRATION_ERROR",
        "general",
        "Review migration logs and contact support if issue persists",
    )

    def __init__(
        self,
        message: str,
        *,
        migration_id: UUID | None = None,
        tenant_id: UUID | None = None,
        recoverable: bool = False,
        suggested_action: str | None = None,
    ) -> None:
        self.message = message
        self.migration_id = migration_id
        self.tenant_id = tenant_id
        self.recoverable = recoverable
        self.suggested_action = suggested_action
        super().__init__(message)

    def __str__(self) -> str:
        parts = [self.message]
        if self.migration_id:
            parts.append(f"migration_id={self.migration_id}")
        if self.tenant_id:
            parts.append(f"tenant_id={self.tenant_id}")
        if self.recoverable:
            parts.append("(recoverable)")
        return " ".join(parts)

    @property
    def is_recoverable(self) -> bool:
        return self.recoverable or self.classification.recoverability != ErrorRecoverability.FATAL

    @property
    def classification(self) -> ErrorClassification:
        return self._default_classification

    @property
    def severity(self) -> ErrorSeverity:
        return self.classification.severity

    @property
    def recoverability_type(self) -> ErrorRecoverability:
        return self.classification.recoverability

    @property
    def error_code(self) -> str:
        return self.classification.error_code

    @property
    def retry_config(self) -> RetryConfig | None:
        return self.classification.retry_config

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "migration_id": str(self.migration_id) if self.migration_id else None,
            "tenant_id": str(self.tenant_id) if self.tenant_id else None,
            "error_code": self.error_code,
            "classification": self.classification.to_dict(),
        }


class MigrationNotFoundError(MigrationError):
    """Raised when a requested migration does not exist."""

    _default_classification = ErrorClassification(
        ErrorSeverity.ERROR,
        ErrorRecoverability.FATAL,
        "MIGRATION_NOT_FOUND",
        "lookup",
        "Verify the migration ID is correct and the migration was created",
    )

    def __init__(self, migration_id: UUID) -> None:
        super().__init__(f"Migration not found: {migration_id}", migration_id=migration_id)


class MigrationAlreadyExistsError(MigrationError):
    """Raised when attempting to create a migration for a tenant with an active migration."""

    _default_classification = ErrorClassification(
        ErrorSeverity.WARNING,
        ErrorRecoverability.RECOVERABLE,
        "MIGRATION_ALREADY_EXISTS",
        "state",
        "Wait for existing migration to complete or abort it first",
    )

    def __init__(self, tenant_id: UUID, existing_migration_id: UUID) -> None:
        self.existing_migration_id = existing_migration_id
        super().__init__(
            f"Active migration already exists for tenant {tenant_id}: {existing_migration_id}",
            migration_id=existing_migration_id,
            tenant_id=tenant_id,
            suggested_action="Wait for existing migration to complete or abort it",
        )


class MigrationStateError(MigrationError):
    """Raised when a migration operation is invalid for the current state."""

    _default_classification = ErrorClassification(
        ErrorSeverity.ERROR,
        ErrorRecoverability.FATAL,
        "MIGRATION_STATE_ERROR",
        "state",
        "Ensure migration is in the correct phase before attempting this operation",
    )

    def __init__(
        self,
        message: str,
        migration_id: UUID,
        current_phase: MigrationPhase,
        expected_phases: list[MigrationPhase] | None = None,
        operation: str | None = None,
    ) -> None:
        self.current_phase = current_phase
        self.expected_phases = expected_phases or []
        self.operation = operation
        super().__init__(message=message, migration_id=migration_id)


class InvalidPhaseTransitionError(MigrationStateError):
    """Raised when attempting an invalid phase transition in the migration state machine."""

    _default_classification = ErrorClassification(
        ErrorSeverity.ERROR,
        ErrorRecoverability.FATAL,
        "INVALID_PHASE_TRANSITION",
        "state",
        "Review the migration state machine and ensure valid transitions",
    )

    def __init__(
        self,
        migration_id: UUID,
        current_phase: MigrationPhase,
        target_phase: MigrationPhase,
    ) -> None:
        self.target_phase = target_phase
        super().__init__(
            f"Invalid phase transition: {current_phase.value} -> {target_phase.value}",
            migration_id=migration_id,
            current_phase=current_phase,
            operation="phase_transition",
        )


__all__ = [
    "InvalidPhaseTransitionError",
    "MigrationAlreadyExistsError",
    "MigrationError",
    "MigrationNotFoundError",
    "MigrationStateError",
]
