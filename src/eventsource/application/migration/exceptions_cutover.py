"""
Cutover exceptions for the migration system.
"""

from __future__ import annotations

from uuid import UUID

from eventsource.application.migration.error_classification import (
    CUTOVER_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.application.migration.exceptions_base import MigrationError


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


__all__ = [
    "CutoverError",
    "CutoverLagError",
    "CutoverTimeoutError",
]
