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
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

    @property
    def should_alert(self) -> bool:
        """Check if this severity level should trigger an alert (True for CRITICAL and ERROR)."""
        return self in (ErrorSeverity.CRITICAL, ErrorSeverity.ERROR)

    @property
    def log_level(self) -> int:
        """Get the corresponding Python logging level constant."""
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
        TRANSIENT: Temporary error that may resolve on retry with backoff.
        FATAL: Unrecoverable error requiring migration abort.
    """

    RECOVERABLE = "recoverable"
    TRANSIENT = "transient"
    FATAL = "fatal"

    @property
    def should_retry(self) -> bool:
        """Check if automatic retry is appropriate (True only for TRANSIENT errors)."""
        return self == ErrorRecoverability.TRANSIENT

    @property
    def should_abort(self) -> bool:
        """Check if the migration should be aborted (True only for FATAL errors)."""
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
    """

    severity: ErrorSeverity
    recoverability: ErrorRecoverability
    error_code: str
    category: str
    suggested_action: str
    retry_config: RetryConfig | None = None
    documentation_url: str | None = None
    metrics_labels: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert classification to dictionary for serialization."""
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
    Configuration for automatic error retry with exponential backoff and jitter.

    Attributes:
        max_attempts: Maximum number of retry attempts (including initial).
        base_delay_ms: Base delay between retries in milliseconds.
        max_delay_ms: Maximum delay between retries in milliseconds.
        exponential_base: Base for exponential backoff (default 2.0).
        jitter_factor: Random jitter factor (0.0 to 1.0, default 0.1).
    """

    max_attempts: int = 3
    base_delay_ms: float = 100.0
    max_delay_ms: float = 30000.0
    exponential_base: float = 2.0
    jitter_factor: float = 0.1

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
        """Calculate delay in milliseconds for a 0-indexed retry attempt."""
        import random

        delay = self.base_delay_ms * (self.exponential_base**attempt)
        if self.jitter_factor > 0:
            jitter = delay * self.jitter_factor * random.random()  # nosec B311 - retry jitter, not security
            delay = delay + jitter
        return min(delay, self.max_delay_ms)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
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
