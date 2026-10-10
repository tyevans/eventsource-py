"""
Base migration exceptions and lifecycle/state errors.
"""

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
    """
    Base exception for all migration-related errors.

    All exceptions raised by the migration system inherit from this class,
    allowing callers to catch all migration errors with a single handler.

    The error classification system (P4-004) extends MigrationError with:
    - Severity levels (CRITICAL, ERROR, WARNING, INFO)
    - Recoverability (RECOVERABLE, TRANSIENT, FATAL)
    - Suggested actions for operators
    - Retry configuration for transient errors

    Attributes:
        message: Human-readable error description.
        migration_id: The ID of the migration that caused the error, if applicable.
        tenant_id: The tenant ID involved, if applicable.
        recoverable: Whether this error can be recovered from (legacy attribute).
        suggested_action: Suggested action for recovery.
        classification: Rich error classification metadata (P4-004).
    """

    # Default classification for the base MigrationError
    _default_classification: ErrorClassification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="MIGRATION_ERROR",
        category="general",
        suggested_action="Review migration logs and contact support if issue persists",
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
        """Return formatted error string with context."""
        parts = [self.message]
        if self.migration_id:
            parts.append(f"migration_id={self.migration_id}")
        if self.tenant_id:
            parts.append(f"tenant_id={self.tenant_id}")
        if self.recoverable:
            parts.append("(recoverable)")
        return " ".join(parts)

    @property
    def classification(self) -> ErrorClassification:
        """
        Get the error classification for this exception.

        Subclasses override _default_classification to provide
        specific classification metadata for their error type.

        Returns:
            ErrorClassification with severity, recoverability, and guidance.
        """
        return self._default_classification

    @property
    def severity(self) -> ErrorSeverity:
        """
        Get the severity level of this error.

        Returns:
            ErrorSeverity enum value.
        """
        return self.classification.severity

    @property
    def recoverability_type(self) -> ErrorRecoverability:
        """
        Get the recoverability classification of this error.

        Note: This is different from the legacy 'recoverable' boolean.
        Use this for new code that needs detailed recoverability info.

        Returns:
            ErrorRecoverability enum value.
        """
        return self.classification.recoverability

    @property
    def error_code(self) -> str:
        """
        Get the unique error code for this exception.

        Error codes are useful for programmatic error handling
        and internationalization of error messages.

        Returns:
            String error code (e.g., "CUTOVER_TIMEOUT").
        """
        return self.classification.error_code

    @property
    def retry_config(self) -> RetryConfig | None:
        """
        Get the retry configuration for this error, if applicable.

        Returns:
            RetryConfig for transient errors, None otherwise.
        """
        return self.classification.retry_config

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the exception to a dictionary for serialization.

        Useful for API responses and logging.

        Returns:
            Dictionary representation of the error.
        """
        return {
            "message": self.message,
            "migration_id": str(self.migration_id) if self.migration_id else None,
            "tenant_id": str(self.tenant_id) if self.tenant_id else None,
            "error_code": self.error_code,
            "classification": self.classification.to_dict(),
        }


class MigrationNotFoundError(MigrationError):
    """
    Raised when a requested migration does not exist.

    This typically occurs when:
    - Attempting to get status of a non-existent migration
    - Attempting to resume a migration that was never started
    - Using an incorrect migration ID

    Attributes:
        migration_id: The ID that was not found.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="MIGRATION_NOT_FOUND",
        category="lookup",
        suggested_action="Verify the migration ID is correct and the migration was created",
    )

    def __init__(self, migration_id: UUID) -> None:
        super().__init__(
            message=f"Migration not found: {migration_id}",
            migration_id=migration_id,
            recoverable=False,
        )


class MigrationAlreadyExistsError(MigrationError):
    """
    Raised when attempting to create a migration that already exists.

    This prevents duplicate migrations for the same tenant and ensures
    only one migration per tenant can be active at a time.

    Attributes:
        tenant_id: The tenant ID for which a migration already exists.
        existing_migration_id: The ID of the existing migration.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.WARNING,
        recoverability=ErrorRecoverability.RECOVERABLE,
        error_code="MIGRATION_ALREADY_EXISTS",
        category="state",
        suggested_action="Wait for existing migration to complete or abort it first",
    )

    def __init__(
        self,
        tenant_id: UUID,
        existing_migration_id: UUID,
    ) -> None:
        self.existing_migration_id = existing_migration_id
        super().__init__(
            message=(
                f"Active migration already exists for tenant {tenant_id}: {existing_migration_id}"
            ),
            migration_id=existing_migration_id,
            tenant_id=tenant_id,
            recoverable=False,
            suggested_action="Wait for existing migration to complete or abort it",
        )


class MigrationStateError(MigrationError):
    """
    Raised when a migration operation is invalid for the current state.

    This enforces the migration state machine, ensuring operations only
    occur in valid states (e.g., cannot start cutover before bulk copy).

    Attributes:
        current_phase: The current phase of the migration.
        expected_phases: The phases that would have been valid.
        operation: The operation that was attempted.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="MIGRATION_STATE_ERROR",
        category="state",
        suggested_action="Ensure migration is in the correct phase before attempting this operation",
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
        super().__init__(
            message=message,
            migration_id=migration_id,
            recoverable=False,
        )


class InvalidPhaseTransitionError(MigrationStateError):
    """
    Raised when attempting an invalid phase transition.

    The migration system enforces a strict state machine. This error
    indicates an attempt to transition to a phase that is not allowed
    from the current phase.

    Attributes:
        current_phase: The current phase of the migration.
        target_phase: The phase that was attempted.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="INVALID_PHASE_TRANSITION",
        category="state",
        suggested_action="Review the migration state machine and ensure valid transitions",
    )

    def __init__(
        self,
        migration_id: UUID,
        current_phase: MigrationPhase,
        target_phase: MigrationPhase,
    ) -> None:
        self.target_phase = target_phase
        super().__init__(
            message=(f"Invalid phase transition: {current_phase.value} -> {target_phase.value}"),
            migration_id=migration_id,
            current_phase=current_phase,
            expected_phases=[],
            operation="phase_transition",
        )


__all__ = [
    "InvalidPhaseTransitionError",
    "MigrationAlreadyExistsError",
    "MigrationError",
    "MigrationNotFoundError",
    "MigrationStateError",
]
