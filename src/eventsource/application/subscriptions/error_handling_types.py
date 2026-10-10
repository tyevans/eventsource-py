"""
Types, enums, data structures, and statistics for subscription error handling.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID

from eventsource.application.subscriptions.subscription import render_position
from eventsource.ports.positions import Position

_RATE_WINDOW_SECONDS = 60
"""Width of the rolling window behind `ErrorStats.error_rate_per_minute`."""


# =============================================================================
# Error Classification Enums & Data Structures
# =============================================================================


class ErrorCategory(Enum):
    """
    Categories of errors for classification.

    These categories help determine how to handle different types of errors.
    """

    TRANSIENT = "transient"
    """Temporary failures that may succeed on retry (network issues, timeouts)."""

    PERMANENT = "permanent"
    """Failures that will not succeed on retry (validation errors, data issues)."""

    INFRASTRUCTURE = "infrastructure"
    """Infrastructure failures (database down, message broker unavailable)."""

    APPLICATION = "application"
    """Application-level errors (business logic failures, handler bugs)."""

    UNKNOWN = "unknown"
    """Unknown error category (fallback for unclassified errors)."""


class ErrorSeverity(Enum):
    """
    Severity levels for errors.

    Determines the urgency and escalation path for errors.
    """

    LOW = "low"
    """Minor errors that don't significantly impact processing."""

    MEDIUM = "medium"
    """Errors that affect some processing but system continues."""

    HIGH = "high"
    """Significant errors that need attention."""

    CRITICAL = "critical"
    """Critical errors requiring immediate attention."""


@dataclass(frozen=True)
class ErrorClassification:
    """
    Classification result for an error.

    Combines category, severity, and retry information.
    """

    category: ErrorCategory
    severity: ErrorSeverity
    retryable: bool
    description: str = ""


# =============================================================================
# Error Context and Tracking
# =============================================================================


@dataclass
class ErrorInfo:
    """
    Detailed information about a processing error.

    Captures all relevant context about an error for debugging,
    monitoring, and DLQ handling.
    """

    event_id: UUID
    event_type: str
    position: Position | None
    error_type: str
    error_message: str
    error_stacktrace: str
    classification: ErrorClassification
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    subscription_name: str = ""
    retry_count: int = 0
    sent_to_dlq: bool = False
    dlq_id: int | str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "position": render_position(self.position),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "error_stacktrace": self.error_stacktrace,
            "category": self.classification.category.value,
            "severity": self.classification.severity.value,
            "retryable": self.classification.retryable,
            "timestamp": self.timestamp.isoformat(),
            "subscription_name": self.subscription_name,
            "retry_count": self.retry_count,
            "sent_to_dlq": self.sent_to_dlq,
            "dlq_id": self.dlq_id,
        }


@dataclass
class ErrorStats:
    """
    Aggregate error statistics for a subscription.

    Tracks error patterns and rates for health monitoring.
    """

    total_errors: int = 0
    transient_errors: int = 0
    permanent_errors: int = 0
    retried_errors: int = 0
    dlq_count: int = 0
    errors_by_type: dict[str, int] = field(default_factory=dict)
    errors_by_category: dict[str, int] = field(default_factory=dict)
    first_error_at: datetime | None = None
    last_error_at: datetime | None = None

    _rate_buckets: list[int] = field(
        default_factory=lambda: [0] * _RATE_WINDOW_SECONDS, init=False, repr=False
    )
    _rate_bucket_stamps: list[int] = field(
        default_factory=lambda: [-_RATE_WINDOW_SECONDS] * _RATE_WINDOW_SECONDS,
        init=False,
        repr=False,
    )

    @property
    def error_rate_per_minute(self) -> float:
        """Errors recorded in the last 60 seconds."""
        now = int(time.monotonic())
        return float(
            sum(
                count
                for count, stamp in zip(self._rate_buckets, self._rate_bucket_stamps, strict=True)
                if now - stamp < _RATE_WINDOW_SECONDS
            )
        )

    def _record_rate_sample(self) -> None:
        """Count one error into the current second's bucket."""
        now = int(time.monotonic())
        index = now % _RATE_WINDOW_SECONDS
        if self._rate_bucket_stamps[index] != now:
            self._rate_bucket_stamps[index] = now
            self._rate_buckets[index] = 0
        self._rate_buckets[index] += 1

    def record_error(self, error_info: ErrorInfo) -> None:
        """Record an error in statistics."""
        self.total_errors += 1

        category = error_info.classification.category.value
        self.errors_by_category[category] = self.errors_by_category.get(category, 0) + 1

        if error_info.classification.category == ErrorCategory.TRANSIENT:
            self.transient_errors += 1
        else:
            self.permanent_errors += 1

        self.errors_by_type[error_info.error_type] = (
            self.errors_by_type.get(error_info.error_type, 0) + 1
        )

        if error_info.sent_to_dlq:
            self.dlq_count += 1

        now = datetime.now(UTC)
        if self.first_error_at is None:
            self.first_error_at = now
        self.last_error_at = now

        self._record_rate_sample()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_errors": self.total_errors,
            "transient_errors": self.transient_errors,
            "permanent_errors": self.permanent_errors,
            "retried_errors": self.retried_errors,
            "dlq_count": self.dlq_count,
            "errors_by_type": dict(self.errors_by_type),
            "errors_by_category": dict(self.errors_by_category),
            "first_error_at": (self.first_error_at.isoformat() if self.first_error_at else None),
            "last_error_at": (self.last_error_at.isoformat() if self.last_error_at else None),
            "error_rate_per_minute": self.error_rate_per_minute,
        }


# =============================================================================
# Error Handling Strategy & Config
# =============================================================================


class ErrorHandlingStrategy(Enum):
    """
    Strategy for handling event processing errors.

    Determines what happens when an event fails to process.
    """

    STOP = "stop"
    """Stop processing immediately on first error."""

    CONTINUE = "continue"
    """Log error and continue with next event."""

    RETRY_THEN_CONTINUE = "retry_then_continue"
    """Retry with backoff, then continue if all retries fail."""

    RETRY_THEN_DLQ = "retry_then_dlq"
    """Retry with backoff, then send to DLQ if all retries fail."""

    DLQ_ONLY = "dlq_only"
    """Send to DLQ immediately without retry."""


@dataclass(frozen=True)
class ErrorHandlingConfig:
    """
    Configuration for error handling behavior.

    Controls how errors are classified, handled, and tracked.
    """

    strategy: ErrorHandlingStrategy = ErrorHandlingStrategy.RETRY_THEN_CONTINUE
    """Default strategy for handling errors."""

    max_recent_errors: int = 100
    """Maximum number of recent errors to keep in memory."""

    max_errors_before_stop: int | None = None
    """If set, stop subscription after this many errors."""

    error_rate_threshold: float | None = None
    """If set, trigger alert when error rate (per minute) exceeds this."""

    dlq_enabled: bool = True
    """Whether to send failed events to DLQ."""

    notify_on_severity: ErrorSeverity = ErrorSeverity.HIGH
    """Minimum severity level to trigger callbacks."""


ErrorCallback = Callable[[ErrorInfo], Awaitable[None]]
SyncErrorCallback = Callable[[ErrorInfo], None]

__all__ = [
    "ErrorCallback",
    "ErrorCategory",
    "ErrorClassification",
    "ErrorHandlingConfig",
    "ErrorHandlingStrategy",
    "ErrorInfo",
    "ErrorSeverity",
    "ErrorStats",
    "SyncErrorCallback",
    "_RATE_WINDOW_SECONDS",
]
