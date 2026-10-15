"""Types, enums, data structures, and statistics for subscription error handling."""

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


class ErrorCategory(Enum):
    """Categories of errors for classification."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    INFRASTRUCTURE = "infrastructure"
    APPLICATION = "application"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    """Severity levels determining urgency and escalation path for errors."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(frozen=True)
class ErrorClassification:
    """Classification result combining category, severity, and retry information."""

    category: ErrorCategory
    severity: ErrorSeverity
    retryable: bool
    description: str = ""


@dataclass
class ErrorInfo:
    """Detailed information about a processing error for debugging, monitoring, and DLQ."""

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
    """Aggregate error statistics for a subscription."""

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


class ErrorHandlingStrategy(Enum):
    """Strategy for handling event processing errors."""

    STOP = "stop"
    CONTINUE = "continue"
    RETRY_THEN_CONTINUE = "retry_then_continue"
    RETRY_THEN_DLQ = "retry_then_dlq"
    DLQ_ONLY = "dlq_only"


@dataclass(frozen=True)
class ErrorHandlingConfig:
    """Configuration for error handling behavior."""

    strategy: ErrorHandlingStrategy = ErrorHandlingStrategy.RETRY_THEN_CONTINUE
    max_recent_errors: int = 100
    max_errors_before_stop: int | None = None
    error_rate_threshold: float | None = None
    dlq_enabled: bool = True
    notify_on_severity: ErrorSeverity = ErrorSeverity.HIGH


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
