"""Operation-specific migration exceptions: consistency, bulk copy, dual write, position mapping, circuit breaker."""

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
    """Raised when data consistency verification between source and target stores fails."""

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
    """Raised during bulk copy failures while copying historical events from source to target."""

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

    def __init__(self, migration_id: UUID, last_position: Position | None, error: str) -> None:
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
    """Raised when writing to the target store fails during the dual-write phase."""

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

    def __init__(self, migration_id: UUID, target_error: str) -> None:
        self.target_error = target_error
        super().__init__(
            message=f"Target store write failed: {target_error}",
            migration_id=migration_id,
            recoverable=True,
            suggested_action="Background sync will recover; monitor sync lag",
        )


class PositionMappingError(MigrationError):
    """Raised when position mapping between source and target stores fails."""

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
        super().__init__(message=message, migration_id=migration_id, recoverable=False)


class CircuitBreakerOpenError(MigrationError):
    """Raised when an operation is rejected due to an open circuit breaker."""

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
        retry_config=RetryConfig(3, 30000.0, 120000.0, 2.0, 0.2),
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
            message=f"Circuit breaker open for '{operation_name}'. Retry after {time_until_retry:.1f}s",
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
