"""
Migration exception taxonomy.

`MigrationError` and its subclasses, rooted in `EventSourceError`. Each
declares its default classification (severity, recoverability, retry
policy) using the vocabulary from `error_classification.py`.

Sibling modules own the runtime machinery that used to live here:
`circuit_breaker.py` (failure gating) and `error_handling.py`
(`ErrorHandler`, `classify_exception`). See ADR 0044.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.domain.exceptions import EventSourceError

if TYPE_CHECKING:
    from eventsource.ports.migration.models import MigrationPhase
    from eventsource.ports.positions import Position


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


class CutoverError(MigrationError):
    """
    Base exception for cutover failures.

    Cutover is the critical phase where writes are briefly paused and
    traffic is switched from source to target store. Failures here
    require careful handling to avoid data inconsistency.

    Attributes:
        rollback_performed: Whether automatic rollback was performed.
        reason: Detailed reason for the failure.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.RECOVERABLE,
        error_code="CUTOVER_ERROR",
        category="cutover",
        suggested_action="Reduce sync lag and retry cutover operation",
        retry_config=CUTOVER_RETRY_CONFIG,
    )

    def __init__(
        self,
        message: str,
        migration_id: UUID,
        rollback_performed: bool = False,
        reason: str | None = None,
    ) -> None:
        self.rollback_performed = rollback_performed
        self.reason = reason
        super().__init__(
            message=message,
            migration_id=migration_id,
            recoverable=True,
            suggested_action="Reduce sync lag and retry cutover",
        )


class CutoverTimeoutError(CutoverError):
    """
    Raised when cutover exceeds the maximum allowed pause time.

    The migration system guarantees sub-100ms cutover pause. If this
    timeout is exceeded, the operation is aborted and rolled back to
    maintain availability SLAs.

    Attributes:
        elapsed_ms: How long the cutover actually took.
        timeout_ms: The configured timeout that was exceeded.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.TRANSIENT,
        error_code="CUTOVER_TIMEOUT",
        category="cutover",
        suggested_action=(
            "Cutover exceeded timeout and was rolled back. Wait for sync lag to decrease and retry."
        ),
        retry_config=CUTOVER_RETRY_CONFIG,
    )

    def __init__(
        self,
        migration_id: UUID,
        elapsed_ms: float,
        timeout_ms: float,
    ) -> None:
        self.elapsed_ms = elapsed_ms
        self.timeout_ms = timeout_ms
        super().__init__(
            message=(f"Cutover timeout exceeded: {elapsed_ms:.2f}ms (limit: {timeout_ms:.2f}ms)"),
            migration_id=migration_id,
            rollback_performed=True,
            reason="timeout",
        )


class CutoverLagError(CutoverError):
    """
    Raised when sync lag is too high for cutover.

    The migration system requires sync lag to be below a threshold
    before cutover can proceed. This error indicates the lag is still
    too high.

    Attributes:
        current_lag: Current sync lag in events.
        max_lag: Maximum allowed lag for cutover.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.WARNING,
        recoverability=ErrorRecoverability.TRANSIENT,
        error_code="CUTOVER_LAG_TOO_HIGH",
        category="cutover",
        suggested_action=(
            "Sync lag is too high for cutover. Run MigrationCoordinator.run_resync_pass "
            "to recover a clamped lag anchor and retry, or explicitly accept a bounded "
            "loss window by passing a nonzero MigrationConfig.cutover_max_lag_events."
        ),
        retry_config=RetryConfig(
            max_attempts=10,
            base_delay_ms=5000.0,
            max_delay_ms=60000.0,
            exponential_base=1.5,
            jitter_factor=0.2,
        ),
    )

    def __init__(
        self,
        migration_id: UUID,
        current_lag: int,
        max_lag: int,
    ) -> None:
        self.current_lag = current_lag
        self.max_lag = max_lag
        super().__init__(
            message=(f"Sync lag too high for cutover: {current_lag} events (max: {max_lag})"),
            migration_id=migration_id,
            rollback_performed=False,
            reason="lag_too_high",
        )
        # Override CutoverError's generic "reduce sync lag" text: under the
        # strict-zero default, waiting for lag to drain is not always
        # possible (a clamped anchor never drains on its own), so point
        # operators at the actual remedies.
        self.suggested_action = (
            "Run MigrationCoordinator.run_resync_pass to recover a clamped lag "
            "anchor and retry, or explicitly accept a bounded loss window by "
            "passing a nonzero MigrationConfig.cutover_max_lag_events."
        )


class ConsistencyError(MigrationError):
    """
    Raised when data consistency verification fails.

    The migration system verifies data integrity by comparing event
    counts and checksums between source and target stores. This error
    indicates a mismatch that must be resolved before cutover.

    Attributes:
        source_count: Number of events in source store.
        target_count: Number of events in target store.
        stream_id: The specific stream where inconsistency was detected, if applicable.
        details: Additional details about the inconsistency.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.CRITICAL,
        recoverability=ErrorRecoverability.RECOVERABLE,
        error_code="CONSISTENCY_ERROR",
        category="consistency",
        suggested_action=(
            "Data inconsistency detected between source and target stores. "
            "Review migration logs, investigate the discrepancy, and consider "
            "manual reconciliation or restarting the migration."
        ),
    )

    def __init__(
        self,
        message: str,
        migration_id: UUID,
        source_count: int | None = None,
        target_count: int | None = None,
        stream_id: str | None = None,
        details: str | None = None,
    ) -> None:
        self.source_count = source_count
        self.target_count = target_count
        self.stream_id = stream_id
        self.details = details
        super().__init__(
            message=message,
            migration_id=migration_id,
            recoverable=False,
            suggested_action="Review migration logs and consider manual reconciliation",
        )


class BulkCopyError(MigrationError):
    """
    Raised during bulk copy failures.

    Bulk copy is the phase where historical events are copied from
    source to target. This error indicates a failure during that
    process, which is typically recoverable by resuming.

    Attributes:
        last_position: The last successfully copied source position
            (None when nothing had been copied yet).
        original_error: The underlying error message.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.TRANSIENT,
        error_code="BULK_COPY_ERROR",
        category="bulk_copy",
        suggested_action=(
            "Bulk copy failed but can be resumed from the last checkpoint. "
            "Check connectivity and disk space, then resume the migration."
        ),
        retry_config=CONNECTIVITY_RETRY_CONFIG,
    )

    def __init__(
        self,
        migration_id: UUID,
        last_position: Position | None,
        error: str,
    ) -> None:
        self.last_position = last_position
        self.original_error = error
        rendered = last_position.to_str() if last_position is not None else "start"
        super().__init__(
            message=f"Bulk copy failed at position {rendered}: {error}",
            migration_id=migration_id,
            recoverable=True,
            suggested_action="Resume migration to continue from last checkpoint",
        )


class DualWriteError(MigrationError):
    """
    Raised during dual-write failures.

    During dual-write phase, events are written to both source and
    target stores. If the target write fails, this error is raised.
    The system can recover via background sync.

    Attributes:
        target_error: The error from the target store write.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.WARNING,
        recoverability=ErrorRecoverability.TRANSIENT,
        error_code="DUAL_WRITE_ERROR",
        category="dual_write",
        suggested_action=(
            "Target store write failed during dual-write phase. "
            "The system will automatically recover via background sync. "
            "Monitor sync lag to ensure it decreases."
        ),
        retry_config=TRANSIENT_RETRY_CONFIG,
    )

    def __init__(
        self,
        migration_id: UUID,
        target_error: str,
    ) -> None:
        self.target_error = target_error
        super().__init__(
            message=f"Target store write failed: {target_error}",
            migration_id=migration_id,
            recoverable=True,
            suggested_action="Background sync will recover; monitor sync lag",
        )


class PositionMappingError(MigrationError):
    """
    Raised when position mapping between stores fails.

    During migration, event positions in the source store must be mapped
    to positions in the target store for subscription continuity. This
    error indicates that mapping could not be established or is invalid.

    Attributes:
        source_position: The source store position that failed to map.
        reason: Detailed reason for the mapping failure.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.RECOVERABLE,
        error_code="POSITION_MAPPING_ERROR",
        category="subscription",
        suggested_action=(
            "Position mapping failed for subscription migration. "
            "Check that the migration completed successfully and "
            "position mappings were recorded during bulk copy."
        ),
    )

    def __init__(
        self,
        message: str,
        migration_id: UUID,
        source_position: Position | None = None,
        reason: str | None = None,
    ) -> None:
        self.source_position = source_position
        self.reason = reason
        if source_position is not None:
            message = f"{message} (source_position={source_position.to_str()})"
        super().__init__(
            message=message,
            migration_id=migration_id,
            recoverable=False,
        )


class RoutingError(MigrationError):
    """
    Raised when tenant routing operations fail.

    The TenantStoreRouter determines which store handles operations for
    each tenant. This error indicates routing configuration or lookup
    failures that prevent proper operation dispatching.

    Attributes:
        tenant_id: The tenant ID that caused the routing error.
        reason: Detailed reason for the routing failure.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="ROUTING_ERROR",
        category="routing",
        suggested_action=(
            "Tenant routing configuration is invalid or missing. "
            "Ensure the tenant has proper routing configuration and "
            "all required stores are registered with the router."
        ),
    )

    def __init__(
        self,
        message: str,
        tenant_id: UUID,
        reason: str | None = None,
    ) -> None:
        self.reason = reason
        super().__init__(
            message=message,
            tenant_id=tenant_id,
            recoverable=False,
        )


# =============================================================================
# Circuit Breaker
# =============================================================================


class CircuitBreakerOpenError(MigrationError):
    """
    Raised when an operation is rejected due to open circuit breaker.

    This error indicates that too many recent failures have occurred and
    the system is protecting itself by rejecting new operations temporarily.

    Attributes:
        operation_name: Name of the operation that was rejected.
        time_until_retry: Seconds until the circuit will try again.
    """

    _default_classification = ErrorClassification(
        severity=ErrorSeverity.WARNING,
        recoverability=ErrorRecoverability.TRANSIENT,
        error_code="CIRCUIT_BREAKER_OPEN",
        category="circuit_breaker",
        suggested_action=(
            "Circuit breaker is open due to repeated failures. "
            "Wait for the timeout period before retrying. "
            "Investigate the underlying failures if this persists."
        ),
        retry_config=RetryConfig(
            max_attempts=3,
            base_delay_ms=30000.0,
            max_delay_ms=120000.0,
            exponential_base=2.0,
            jitter_factor=0.2,
        ),
    )

    def __init__(
        self,
        operation_name: str,
        time_until_retry: float,
        migration_id: UUID | None = None,
    ) -> None:
        self.operation_name = operation_name
        self.time_until_retry = time_until_retry
        super().__init__(
            message=(
                f"Circuit breaker open for '{operation_name}'. Retry after {time_until_retry:.1f}s"
            ),
            migration_id=migration_id,
            recoverable=True,
            suggested_action=f"Wait {time_until_retry:.0f}s before retrying",
        )
