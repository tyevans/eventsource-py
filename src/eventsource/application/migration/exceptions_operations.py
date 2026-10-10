"""
Operation-specific migration exceptions: consistency, bulk copy, dual write, position mapping, circuit breaker.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.application.migration.exceptions_base import MigrationError

if TYPE_CHECKING:
    from eventsource.ports.positions import Position


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


__all__ = [
    "BulkCopyError",
    "CircuitBreakerOpenError",
    "ConsistencyError",
    "DualWriteError",
    "PositionMappingError",
]
