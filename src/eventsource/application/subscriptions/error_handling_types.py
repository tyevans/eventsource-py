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


class ErrorCategory(Enum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    INFRASTRUCTURE = "infrastructure"
    APPLICATION = "application"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(frozen=True)
class ErrorClassification:
    category: ErrorCategory
    severity: ErrorSeverity
    retryable: bool
    description: str = ""


@dataclass
class ErrorInfo:
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

    # fmt: off
    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id), "event_type": self.event_type,
            "position": render_position(self.position), "error_type": self.error_type,
            "error_message": self.error_message, "error_stacktrace": self.error_stacktrace,
            "category": self.classification.category.value, "severity": self.classification.severity.value,
            "retryable": self.classification.retryable, "timestamp": self.timestamp.isoformat(),
            "subscription_name": self.subscription_name, "retry_count": self.retry_count,
            "sent_to_dlq": self.sent_to_dlq, "dlq_id": self.dlq_id,
        }
    # fmt: on


@dataclass
class ErrorStats:
    total_errors: int = 0
    transient_errors: int = 0
    permanent_errors: int = 0
    retried_errors: int = 0
    dlq_count: int = 0
    errors_by_type: dict[str, int] = field(default_factory=dict)
    errors_by_category: dict[str, int] = field(default_factory=dict)
    first_error_at: datetime | None = None
    last_error_at: datetime | None = None
    _rate_buckets: list[int] = field(default_factory=lambda: [0] * _RATE_WINDOW_SECONDS, init=False, repr=False)  # fmt: skip
    _rate_bucket_stamps: list[int] = field(default_factory=lambda: [-_RATE_WINDOW_SECONDS] * _RATE_WINDOW_SECONDS, init=False, repr=False)  # fmt: skip

    @property
    def error_rate_per_minute(self) -> float:
        now = int(time.monotonic())
        stamps = zip(self._rate_buckets, self._rate_bucket_stamps, strict=True)
        return float(sum(c for c, s in stamps if now - s < _RATE_WINDOW_SECONDS))

    def _record_rate_sample(self) -> None:
        now = int(time.monotonic())
        index = now % _RATE_WINDOW_SECONDS
        if self._rate_bucket_stamps[index] != now:
            self._rate_bucket_stamps[index] = now
            self._rate_buckets[index] = 0
        self._rate_buckets[index] += 1

    def record_error(self, error_info: ErrorInfo) -> None:
        self.total_errors += 1
        cat, err_t = error_info.classification.category.value, error_info.error_type
        self.errors_by_category[cat] = self.errors_by_category.get(cat, 0) + 1
        self.errors_by_type[err_t] = self.errors_by_type.get(err_t, 0) + 1
        if error_info.classification.category == ErrorCategory.TRANSIENT:
            self.transient_errors += 1
        else:
            self.permanent_errors += 1
        if error_info.sent_to_dlq:
            self.dlq_count += 1
        now = datetime.now(UTC)
        self.first_error_at, self.last_error_at = self.first_error_at or now, now
        self._record_rate_sample()

    # fmt: off
    def to_dict(self) -> dict[str, Any]:
        return {
            "total_errors": self.total_errors, "transient_errors": self.transient_errors,
            "permanent_errors": self.permanent_errors, "retried_errors": self.retried_errors,
            "dlq_count": self.dlq_count, "errors_by_type": dict(self.errors_by_type),
            "errors_by_category": dict(self.errors_by_category),
            "first_error_at": self.first_error_at.isoformat() if self.first_error_at else None,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
            "error_rate_per_minute": self.error_rate_per_minute,
        }
    # fmt: on


class ErrorHandlingStrategy(Enum):
    STOP = "stop"
    CONTINUE = "continue"
    RETRY_THEN_CONTINUE = "retry_then_continue"
    RETRY_THEN_DLQ = "retry_then_dlq"
    DLQ_ONLY = "dlq_only"


@dataclass(frozen=True)
class ErrorHandlingConfig:
    strategy: ErrorHandlingStrategy = ErrorHandlingStrategy.RETRY_THEN_CONTINUE
    max_recent_errors: int = 100
    max_errors_before_stop: int | None = None
    error_rate_threshold: float | None = None
    dlq_enabled: bool = True
    notify_on_severity: ErrorSeverity = ErrorSeverity.HIGH


ErrorCallback = Callable[[ErrorInfo], Awaitable[None]]
SyncErrorCallback = Callable[[ErrorInfo], None]

# fmt: off
__all__ = [
    "ErrorCallback", "ErrorCategory", "ErrorClassification", "ErrorHandlingConfig",
    "ErrorHandlingStrategy", "ErrorInfo", "ErrorSeverity", "ErrorStats",
    "SyncErrorCallback", "_RATE_WINDOW_SECONDS",
]
# fmt: on
