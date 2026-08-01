"""
Error classification vocabulary for migration operations.

The value objects that describe *what kind of error* occurred and *how to
respond to it*: severity, recoverability, the combined classification
record, and retry configuration.

This module is the base layer of the migration error stack and imports
nothing from its siblings — `exceptions.py` depends on this vocabulary to
declare each error's default classification, so a dependency in the other
direction would be a cycle (ADR 0044).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

__all__ = [
    "ErrorSeverity",
    "ErrorRecoverability",
    "ErrorClassification",
    "RetryConfig",
    "TRANSIENT_RETRY_CONFIG",
    "CONNECTIVITY_RETRY_CONFIG",
    "CUTOVER_RETRY_CONFIG",
]


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
