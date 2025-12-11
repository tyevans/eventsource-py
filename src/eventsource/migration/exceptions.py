"""
Migration-specific exceptions for the eventsource migration system.

This module defines all exceptions that can be raised during tenant migration
operations, organized by the component or phase that raises them.

Exception Hierarchy:
    MigrationError (base)
    +-- MigrationNotFoundError
    +-- MigrationAlreadyExistsError
    +-- MigrationStateError
    |   +-- InvalidPhaseTransitionError
    +-- CutoverError
    |   +-- CutoverTimeoutError
    |   +-- CutoverLagError
    +-- ConsistencyError
    +-- BulkCopyError
    +-- DualWriteError
    +-- PositionMappingError
    +-- RoutingError

Error Classification System (P4-004):
    The error classification system provides:
    - ErrorSeverity: CRITICAL, ERROR, WARNING, INFO levels
    - ErrorRecoverability: RECOVERABLE, TRANSIENT, FATAL categories
    - ErrorClassification: Rich metadata for each error type
    - ErrorHandler: Automatic retry for transient errors with circuit breaker

See Also:
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
    - Task: P4-004-error-handling.md
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.migration.models import MigrationPhase

logger = logging.getLogger(__name__)

# Type variable for generic async functions
T = TypeVar("T")


class ErrorSeverity(Enum):
    """
    Severity level of migration errors.

    Used for alerting, logging, and operator notification decisions.

    Attributes:
        CRITICAL: System-level failure requiring immediate attention.
            Examples: Data corruption, unrecoverable consistency errors.
        ERROR: Significant failure that may require operator intervention.
            Examples: Cutover failure, bulk copy failure.
        WARNING: Issue that should be monitored but may self-resolve.
            Examples: High sync lag, transient connection errors.
        INFO: Informational condition, not a failure.
            Examples: Migration paused by operator, normal completion.
    """

    CRITICAL = "critical"
    """System-level failure requiring immediate attention."""

    ERROR = "error"
    """Significant failure that may require operator intervention."""

    WARNING = "warning"
    """Issue that should be monitored but may self-resolve."""

    INFO = "info"
    """Informational condition, not a failure."""

    @property
    def should_alert(self) -> bool:
        """
        Check if this severity level should trigger an alert.

        Returns:
            True for CRITICAL and ERROR levels.
        """
        return self in (ErrorSeverity.CRITICAL, ErrorSeverity.ERROR)

    @property
    def log_level(self) -> int:
        """
        Get the corresponding Python logging level.

        Returns:
            Python logging level constant.
        """
        level_map = {
            ErrorSeverity.CRITICAL: logging.CRITICAL,
            ErrorSeverity.ERROR: logging.ERROR,
            ErrorSeverity.WARNING: logging.WARNING,
            ErrorSeverity.INFO: logging.INFO,
        }
        return level_map[self]


class ErrorRecoverability(Enum):
    """
    Recoverability classification for migration errors.

    Determines how the system should respond to errors and whether
    automatic retry is appropriate.

    Attributes:
        RECOVERABLE: Error can be recovered from with operator action.
            The migration can continue after the issue is resolved.
            Examples: Sync lag too high, consistency check failed.

        TRANSIENT: Temporary error that may resolve on retry.
            The system should automatically retry with backoff.
            Examples: Network timeout, temporary database unavailability.

        FATAL: Unrecoverable error requiring migration abort.
            No automatic recovery is possible; migration must be aborted.
            Examples: Data corruption, invalid state transitions.
    """

    RECOVERABLE = "recoverable"
    """Error can be recovered from with operator action."""

    TRANSIENT = "transient"
    """Temporary error that may resolve on retry."""

    FATAL = "fatal"
    """Unrecoverable error requiring migration abort."""

    @property
    def should_retry(self) -> bool:
        """
        Check if automatic retry is appropriate for this category.

        Returns:
            True only for TRANSIENT errors.
        """
        return self == ErrorRecoverability.TRANSIENT

    @property
    def should_abort(self) -> bool:
        """
        Check if the migration should be aborted.

        Returns:
            True only for FATAL errors.
        """
        return self == ErrorRecoverability.FATAL


@dataclass(frozen=True)
class ErrorClassification:
    """
    Rich metadata for error classification.

    Provides comprehensive information about an error for automated
    handling, operator guidance, and observability.

    Attributes:
        severity: The severity level of the error.
        recoverability: How the error can be recovered from.
        error_code: Unique error code for programmatic handling.
        category: Error category for grouping related errors.
        suggested_action: Human-readable guidance for operators.
        retry_config: Configuration for automatic retry (if applicable).
        documentation_url: Link to documentation about this error.
        metrics_labels: Labels for metrics instrumentation.

    Example:
        >>> classification = ErrorClassification(
        ...     severity=ErrorSeverity.WARNING,
        ...     recoverability=ErrorRecoverability.TRANSIENT,
        ...     error_code="MIGRATION_CONN_TIMEOUT",
        ...     category="connectivity",
        ...     suggested_action="Check database connectivity",
        ...     retry_config=RetryConfig(max_attempts=5, base_delay_ms=1000),
        ... )
    """

    severity: ErrorSeverity
    """The severity level of the error."""

    recoverability: ErrorRecoverability
    """How the error can be recovered from."""

    error_code: str
    """Unique error code for programmatic handling."""

    category: str
    """Error category for grouping related errors."""

    suggested_action: str
    """Human-readable guidance for operators."""

    retry_config: RetryConfig | None = None
    """Configuration for automatic retry (if applicable)."""

    documentation_url: str | None = None
    """Link to documentation about this error."""

    metrics_labels: dict[str, str] = field(default_factory=dict)
    """Labels for metrics instrumentation."""

    def to_dict(self) -> dict[str, Any]:
        """
        Convert classification to dictionary for serialization.

        Returns:
            Dictionary representation of the classification.
        """
        result: dict[str, Any] = {
            "severity": self.severity.value,
            "recoverability": self.recoverability.value,
            "error_code": self.error_code,
            "category": self.category,
            "suggested_action": self.suggested_action,
        }
        if self.retry_config:
            result["retry_config"] = self.retry_config.to_dict()
        if self.documentation_url:
            result["documentation_url"] = self.documentation_url
        if self.metrics_labels:
            result["metrics_labels"] = self.metrics_labels
        return result


@dataclass(frozen=True)
class RetryConfig:
    """
    Configuration for automatic error retry.

    Implements exponential backoff with jitter for transient errors.

    Attributes:
        max_attempts: Maximum number of retry attempts (including initial).
        base_delay_ms: Base delay between retries in milliseconds.
        max_delay_ms: Maximum delay between retries in milliseconds.
        exponential_base: Base for exponential backoff (default 2.0).
        jitter_factor: Random jitter factor (0.0 to 1.0, default 0.1).

    Example:
        >>> config = RetryConfig(
        ...     max_attempts=5,
        ...     base_delay_ms=100,
        ...     max_delay_ms=10000,
        ... )
        >>> config.get_delay_ms(attempt=3)  # Returns delay for 3rd attempt
        800  # (100 * 2^3 = 800ms, plus jitter)
    """

    max_attempts: int = 3
    """Maximum number of retry attempts (including initial)."""

    base_delay_ms: float = 100.0
    """Base delay between retries in milliseconds."""

    max_delay_ms: float = 30000.0
    """Maximum delay between retries in milliseconds."""

    exponential_base: float = 2.0
    """Base for exponential backoff."""

    jitter_factor: float = 0.1
    """Random jitter factor (0.0 to 1.0)."""

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.base_delay_ms < 0:
            raise ValueError(f"base_delay_ms must be >= 0, got {self.base_delay_ms}")
        if self.max_delay_ms < self.base_delay_ms:
            raise ValueError(
                f"max_delay_ms ({self.max_delay_ms}) must be >= "
                f"base_delay_ms ({self.base_delay_ms})"
            )
        if self.exponential_base < 1.0:
            raise ValueError(f"exponential_base must be >= 1.0, got {self.exponential_base}")
        if not 0.0 <= self.jitter_factor <= 1.0:
            raise ValueError(f"jitter_factor must be between 0.0 and 1.0, got {self.jitter_factor}")

    def get_delay_ms(self, attempt: int) -> float:
        """
        Calculate delay for a specific retry attempt.

        Uses exponential backoff with optional jitter.

        Args:
            attempt: Current attempt number (0-indexed).

        Returns:
            Delay in milliseconds before the next retry.
        """
        import random

        # Calculate base exponential delay
        delay = self.base_delay_ms * (self.exponential_base**attempt)

        # Apply jitter
        if self.jitter_factor > 0:
            jitter = delay * self.jitter_factor * random.random()  # nosec B311 - retry jitter, not security
            delay = delay + jitter

        # Cap at maximum
        return min(delay, self.max_delay_ms)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dictionary representation of the retry config.
        """
        return {
            "max_attempts": self.max_attempts,
            "base_delay_ms": self.base_delay_ms,
            "max_delay_ms": self.max_delay_ms,
            "exponential_base": self.exponential_base,
            "jitter_factor": self.jitter_factor,
        }


# Default retry configurations for different error categories
TRANSIENT_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay_ms=100.0,
    max_delay_ms=30000.0,
    exponential_base=2.0,
    jitter_factor=0.1,
)

CONNECTIVITY_RETRY_CONFIG = RetryConfig(
    max_attempts=10,
    base_delay_ms=500.0,
    max_delay_ms=60000.0,
    exponential_base=2.0,
    jitter_factor=0.2,
)

CUTOVER_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay_ms=1000.0,
    max_delay_ms=10000.0,
    exponential_base=2.0,
    jitter_factor=0.1,
)


class MigrationError(Exception):
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
            "Sync lag is too high for cutover. "
            "Wait for lag to decrease below threshold, then retry."
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
        last_position: The last successfully copied position.
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
        last_position: int,
        error: str,
    ) -> None:
        self.last_position = last_position
        self.original_error = error
        super().__init__(
            message=f"Bulk copy failed at position {last_position}: {error}",
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
        source_position: int | None = None,
        reason: str | None = None,
    ) -> None:
        self.source_position = source_position
        self.reason = reason
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
# Error Handler (P4-004)
# =============================================================================


class CircuitState(Enum):
    """
    Circuit breaker state.

    Attributes:
        CLOSED: Circuit is closed, operations proceed normally.
        OPEN: Circuit is open, operations are rejected immediately.
        HALF_OPEN: Circuit is testing if operations can succeed again.
    """

    CLOSED = "closed"
    """Circuit is closed, operations proceed normally."""

    OPEN = "open"
    """Circuit is open, operations are rejected immediately."""

    HALF_OPEN = "half_open"
    """Circuit is testing if operations can succeed again."""


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for circuit breaker behavior.

    Attributes:
        failure_threshold: Number of failures before opening circuit.
        success_threshold: Number of successes in half-open state to close.
        timeout_seconds: Seconds before trying half-open state.
        excluded_exceptions: Exception types that don't trip the circuit.
    """

    failure_threshold: int = 5
    """Number of failures before opening circuit."""

    success_threshold: int = 2
    """Number of successes in half-open state to close circuit."""

    timeout_seconds: float = 30.0
    """Seconds before trying half-open state."""

    excluded_exceptions: tuple[type[Exception], ...] = ()
    """Exception types that don't trip the circuit."""


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


class CircuitBreaker:
    """
    Circuit breaker implementation for migration operations.

    Tracks failures and opens the circuit when a threshold is reached,
    preventing cascading failures during system instability.

    Usage:
        >>> cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=5))
        >>> async with cb.protect("my_operation"):
        ...     await risky_operation()

    Attributes:
        config: Circuit breaker configuration.
        state: Current circuit state.
        failure_count: Number of consecutive failures.
        success_count: Number of consecutive successes in half-open state.
        last_failure_time: Timestamp of last failure.
    """

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        name: str = "default",
    ) -> None:
        """
        Initialize the circuit breaker.

        Args:
            config: Circuit breaker configuration (uses defaults if None).
            name: Name for logging and identification.
        """
        self.config = config or CircuitBreakerConfig()
        self.name = name
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get the current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get the current failure count."""
        return self._failure_count

    async def _check_state(self) -> None:
        """Check and potentially update the circuit state."""
        if self._state == CircuitState.OPEN and self._last_failure_time is not None:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.config.timeout_seconds:
                logger.info(
                    "Circuit breaker '%s' transitioning to half-open after %.1fs",
                    self.name,
                    elapsed,
                )
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0

    async def _record_success(self) -> None:
        """Record a successful operation."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    logger.info(
                        "Circuit breaker '%s' closing after %d successes",
                        self.name,
                        self._success_count,
                    )
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def _record_failure(self, exc: Exception) -> None:
        """Record a failed operation."""
        # Check if this exception should trip the circuit
        if isinstance(exc, self.config.excluded_exceptions):
            return

        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(
                    "Circuit breaker '%s' opening from half-open after failure",
                    self.name,
                )
                self._state = CircuitState.OPEN
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.config.failure_threshold
            ):
                logger.warning(
                    "Circuit breaker '%s' opening after %d failures",
                    self.name,
                    self._failure_count,
                )
                self._state = CircuitState.OPEN

    def get_time_until_retry(self) -> float:
        """Get seconds until the circuit will try half-open."""
        if self._state != CircuitState.OPEN or self._last_failure_time is None:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self.config.timeout_seconds - elapsed)

    async def protect(
        self,
        operation_name: str,
        migration_id: UUID | None = None,
    ) -> CircuitBreakerContext:
        """
        Create a context manager for protecting an operation.

        Args:
            operation_name: Name of the operation for logging.
            migration_id: Optional migration ID for error context.

        Returns:
            Context manager that protects the operation.

        Raises:
            CircuitBreakerOpenError: If the circuit is open.
        """
        await self._check_state()

        if self._state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(
                operation_name=operation_name,
                time_until_retry=self.get_time_until_retry(),
                migration_id=migration_id,
            )

        return CircuitBreakerContext(
            circuit_breaker=self,
            operation_name=operation_name,
        )

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None


class CircuitBreakerContext:
    """Context manager for circuit breaker protected operations."""

    def __init__(
        self,
        circuit_breaker: CircuitBreaker,
        operation_name: str,
    ) -> None:
        self._cb = circuit_breaker
        self._operation_name = operation_name

    async def __aenter__(self) -> CircuitBreakerContext:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if exc_val is None:
            await self._cb._record_success()
        elif isinstance(exc_val, Exception):
            await self._cb._record_failure(exc_val)
        return False  # Don't suppress exceptions


class ErrorHandler:
    """
    Error handler with automatic retry and circuit breaker support.

    Provides a unified interface for handling migration errors with:
    - Automatic retry for transient errors with exponential backoff
    - Circuit breaker to prevent cascading failures
    - Structured logging with error classification
    - Alert hooks for critical/error severity

    Usage:
        >>> handler = ErrorHandler()
        >>>
        >>> # Execute with automatic retry
        >>> result = await handler.execute_with_retry(
        ...     async_operation,
        ...     operation_name="bulk_copy",
        ...     migration_id=migration_id,
        ... )
        >>>
        >>> # Use as decorator
        >>> @handler.with_retry(operation_name="sync_events")
        ... async def sync_events():
        ...     ...

    Attributes:
        circuit_breaker: Optional circuit breaker for the handler.
        alert_callback: Callback for alerting on errors.
        metrics_callback: Callback for recording error metrics.
    """

    def __init__(
        self,
        circuit_breaker: CircuitBreaker | None = None,
        alert_callback: Callable[[MigrationError], None] | None = None,
        metrics_callback: Callable[[MigrationError, bool], None] | None = None,
    ) -> None:
        """
        Initialize the error handler.

        Args:
            circuit_breaker: Optional circuit breaker for protecting operations.
            alert_callback: Callback invoked when errors with alerting severity occur.
            metrics_callback: Callback for recording error metrics (error, retried).
        """
        self.circuit_breaker = circuit_breaker
        self.alert_callback = alert_callback
        self.metrics_callback = metrics_callback

    async def execute_with_retry(
        self,
        operation: Callable[[], Coroutine[Any, Any, T]],
        operation_name: str,
        *,
        migration_id: UUID | None = None,
        retry_config: RetryConfig | None = None,
        on_retry: Callable[[int, Exception, float], None] | None = None,
    ) -> T:
        """
        Execute an operation with automatic retry for transient errors.

        Args:
            operation: Async callable to execute.
            operation_name: Name for logging and metrics.
            migration_id: Optional migration ID for error context.
            retry_config: Override retry configuration.
            on_retry: Callback invoked on each retry (attempt, exception, delay_ms).

        Returns:
            The result of the operation.

        Raises:
            MigrationError: If all retries are exhausted or error is non-transient.
            CircuitBreakerOpenError: If circuit breaker is open.
        """
        # Check circuit breaker if configured
        if self.circuit_breaker:
            ctx = await self.circuit_breaker.protect(operation_name, migration_id)
        else:
            ctx = None

        last_exception: Exception | None = None
        attempt = 0
        effective_config = retry_config or TRANSIENT_RETRY_CONFIG

        while attempt < effective_config.max_attempts:
            try:
                if ctx:
                    async with ctx:
                        result = await operation()
                else:
                    result = await operation()

                # Success - log if we had to retry
                if attempt > 0:
                    logger.info(
                        "Operation '%s' succeeded after %d retries",
                        operation_name,
                        attempt,
                    )

                return result

            except MigrationError as e:
                last_exception = e
                self._handle_error(e, operation_name)

                # Check if we should retry
                if not e.recoverability_type.should_retry:
                    logger.error(
                        "Non-retryable error in '%s': %s (code=%s)",
                        operation_name,
                        e.message,
                        e.error_code,
                    )
                    raise

                # Use error's retry config if available and no override
                config = retry_config or e.retry_config or TRANSIENT_RETRY_CONFIG

                # Check if we have retries left
                if attempt + 1 >= config.max_attempts:
                    logger.error(
                        "Exhausted %d retries for '%s': %s",
                        config.max_attempts,
                        operation_name,
                        e.message,
                    )
                    raise

                # Calculate delay and sleep
                delay_ms = config.get_delay_ms(attempt)
                delay_s = delay_ms / 1000.0

                logger.warning(
                    "Retryable error in '%s' (attempt %d/%d): %s. Retrying in %.1fs",
                    operation_name,
                    attempt + 1,
                    config.max_attempts,
                    e.message,
                    delay_s,
                )

                if on_retry:
                    on_retry(attempt, e, delay_ms)

                await asyncio.sleep(delay_s)
                attempt += 1

            except Exception as e:
                # Non-MigrationError exceptions are not automatically retried
                logger.exception(
                    "Unexpected error in '%s': %s",
                    operation_name,
                    str(e),
                )
                raise

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError(f"Unexpected state in retry loop for {operation_name}")

    def _handle_error(self, error: MigrationError, operation_name: str) -> None:
        """Handle an error by logging and potentially alerting."""
        classification = error.classification

        # Log at appropriate level
        logger.log(
            classification.severity.log_level,
            "Error in '%s': %s [code=%s, severity=%s, recoverable=%s]",
            operation_name,
            error.message,
            classification.error_code,
            classification.severity.value,
            classification.recoverability.value,
        )

        # Invoke alert callback if severity warrants it
        if classification.severity.should_alert and self.alert_callback:
            try:
                self.alert_callback(error)
            except Exception:
                logger.exception("Alert callback failed")

        # Record metrics
        if self.metrics_callback:
            try:
                self.metrics_callback(error, classification.recoverability.should_retry)
            except Exception:
                logger.exception("Metrics callback failed")

    def with_retry(
        self,
        operation_name: str,
        *,
        retry_config: RetryConfig | None = None,
    ) -> Callable[
        [Callable[..., Coroutine[Any, Any, T]]],
        Callable[..., Coroutine[Any, Any, T]],
    ]:
        """
        Decorator for automatic retry on transient errors.

        Args:
            operation_name: Name for logging and metrics.
            retry_config: Override retry configuration.

        Returns:
            Decorator function.

        Example:
            >>> handler = ErrorHandler()
            >>>
            >>> @handler.with_retry(operation_name="process_batch")
            ... async def process_batch(batch_id: str):
            ...     ...
        """

        def decorator(
            func: Callable[..., Coroutine[Any, Any, T]],
        ) -> Callable[..., Coroutine[Any, Any, T]]:
            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> T:
                return await self.execute_with_retry(
                    lambda: func(*args, **kwargs),
                    operation_name=operation_name,
                    retry_config=retry_config,
                )

            return wrapper

        return decorator


def classify_exception(exc: Exception) -> ErrorClassification:
    """
    Classify any exception and return its error classification.

    For MigrationError subclasses, returns their specific classification.
    For other exceptions, returns a generic classification.

    Args:
        exc: The exception to classify.

    Returns:
        ErrorClassification for the exception.

    Example:
        >>> try:
        ...     await risky_operation()
        ... except Exception as e:
        ...     classification = classify_exception(e)
        ...     if classification.severity.should_alert:
        ...         send_alert(e, classification)
    """
    if isinstance(exc, MigrationError):
        return exc.classification

    # Generic classification for non-migration errors
    return ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="UNKNOWN_ERROR",
        category="unknown",
        suggested_action="An unexpected error occurred. Review logs and contact support.",
    )
