"""
Error handling integration for subscription management.

This module provides a unified error handling strategy for subscriptions:
- Error classification (transient vs permanent, retryable vs fatal)
- Error callbacks/hooks for monitoring and alerting
- Dead letter queue integration
- Error tracking and statistics
- Health check integration

The error handling system integrates with:
- Backpressure (PHASE2-001)
- Graceful shutdown (PHASE2-002)
- Retry mechanisms (PHASE2-003)
"""

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent
    from eventsource.repositories.dlq import DLQRepository
    from eventsource.stores.interface import StoredEvent

logger = logging.getLogger(__name__)


# =============================================================================
# Error Classification
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


class ErrorClassifier:
    """
    Classifies errors to determine handling strategy.

    The classifier examines exceptions and categorizes them based on:
    - Exception type
    - Exception message patterns
    - Custom classification rules

    This enables consistent error handling across all subscription components.

    Example:
        >>> classifier = ErrorClassifier()
        >>> classification = classifier.classify(ConnectionError("timeout"))
        >>> classification.category
        ErrorCategory.TRANSIENT
        >>> classification.retryable
        True
    """

    # Default classification rules for common exception types
    _DEFAULT_CLASSIFICATIONS: dict[type[Exception], ErrorClassification] = {
        # Transient network/connection errors
        ConnectionError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Connection error - likely temporary",
        ),
        TimeoutError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Timeout error - operation took too long",
        ),
        asyncio.TimeoutError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Async timeout error",
        ),
        OSError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="OS-level error - may include network issues",
        ),
        # Permanent application errors
        ValueError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.MEDIUM,
            retryable=False,
            description="Invalid value - data/validation issue",
        ),
        TypeError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.HIGH,
            retryable=False,
            description="Type error - likely code bug",
        ),
        AttributeError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.HIGH,
            retryable=False,
            description="Attribute error - likely code bug",
        ),
        KeyError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.MEDIUM,
            retryable=False,
            description="Key error - missing expected data",
        ),
        # Critical errors
        MemoryError: ErrorClassification(
            category=ErrorCategory.INFRASTRUCTURE,
            severity=ErrorSeverity.CRITICAL,
            retryable=False,
            description="Memory exhausted",
        ),
        SystemError: ErrorClassification(
            category=ErrorCategory.INFRASTRUCTURE,
            severity=ErrorSeverity.CRITICAL,
            retryable=False,
            description="System error",
        ),
    }

    def __init__(self) -> None:
        """Initialize the error classifier."""
        self._custom_rules: dict[type[Exception], ErrorClassification] = {}
        self._pattern_rules: list[tuple[str, ErrorClassification]] = []

    def register_classification(
        self,
        exception_type: type[Exception],
        classification: ErrorClassification,
    ) -> None:
        """
        Register a custom classification for an exception type.

        Args:
            exception_type: The exception class to classify
            classification: The classification to apply
        """
        self._custom_rules[exception_type] = classification

    def register_pattern_rule(
        self,
        pattern: str,
        classification: ErrorClassification,
    ) -> None:
        """
        Register a classification rule based on error message pattern.

        Args:
            pattern: Substring to match in error message (case-insensitive)
            classification: The classification to apply if pattern matches
        """
        self._pattern_rules.append((pattern.lower(), classification))

    def classify(self, error: Exception) -> ErrorClassification:
        """
        Classify an error based on type and message.

        Classification priority:
        1. Custom rules (registered via register_classification)
        2. Pattern rules (registered via register_pattern_rule)
        3. Default classifications
        4. Unknown classification (fallback)

        Args:
            error: The exception to classify

        Returns:
            ErrorClassification with category, severity, and retry info
        """
        error_type = type(error)
        error_message = str(error).lower()

        # 1. Check custom rules first
        if error_type in self._custom_rules:
            return self._custom_rules[error_type]

        # 2. Check for pattern matches in error message
        for pattern, classification in self._pattern_rules:
            if pattern in error_message:
                return classification

        # 3. Check default classifications (including parent classes)
        for exc_type in error_type.__mro__:
            if exc_type in self._DEFAULT_CLASSIFICATIONS:
                return self._DEFAULT_CLASSIFICATIONS[exc_type]

        # 4. Fallback to unknown
        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            severity=ErrorSeverity.MEDIUM,
            retryable=False,
            description=f"Unclassified error: {error_type.__name__}",
        )

    def is_retryable(self, error: Exception) -> bool:
        """
        Check if an error should be retried.

        Args:
            error: The exception to check

        Returns:
            True if the error is retryable
        """
        return self.classify(error).retryable

    def is_transient(self, error: Exception) -> bool:
        """
        Check if an error is transient.

        Args:
            error: The exception to check

        Returns:
            True if the error is transient
        """
        return self.classify(error).category == ErrorCategory.TRANSIENT


# Default global classifier instance
_default_classifier = ErrorClassifier()


def get_default_classifier() -> ErrorClassifier:
    """Get the default error classifier instance."""
    return _default_classifier


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
    position: int
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
            "position": self.position,
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
    error_rate_per_minute: float = 0.0

    def record_error(self, error_info: ErrorInfo) -> None:
        """
        Record an error in statistics.

        Args:
            error_info: The error information to record
        """
        self.total_errors += 1

        # Track by category
        category = error_info.classification.category.value
        self.errors_by_category[category] = self.errors_by_category.get(category, 0) + 1

        if error_info.classification.category == ErrorCategory.TRANSIENT:
            self.transient_errors += 1
        else:
            self.permanent_errors += 1

        # Track by error type
        self.errors_by_type[error_info.error_type] = (
            self.errors_by_type.get(error_info.error_type, 0) + 1
        )

        # Track DLQ
        if error_info.sent_to_dlq:
            self.dlq_count += 1

        # Track timing
        now = datetime.now(UTC)
        if self.first_error_at is None:
            self.first_error_at = now
        self.last_error_at = now

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
# Error Callbacks and Handlers
# =============================================================================


# Type alias for error callbacks
ErrorCallback = Callable[[ErrorInfo], Awaitable[None]]
"""Async callback invoked when an error occurs."""

SyncErrorCallback = Callable[[ErrorInfo], None]
"""Sync callback invoked when an error occurs."""


class ErrorHandlerRegistry:
    """
    Registry for error callbacks and handlers.

    Allows registering multiple callbacks that will be invoked when
    errors occur. Supports filtering by error category and severity.

    Example:
        >>> registry = ErrorHandlerRegistry()
        >>> async def alert_on_critical(error: ErrorInfo):
        ...     if error.classification.severity == ErrorSeverity.CRITICAL:
        ...         await send_alert(error)
        >>> registry.register(alert_on_critical)
    """

    def __init__(self) -> None:
        """Initialize the error handler registry."""
        self._callbacks: list[ErrorCallback] = []
        self._sync_callbacks: list[SyncErrorCallback] = []
        self._category_callbacks: dict[ErrorCategory, list[ErrorCallback]] = {}
        self._severity_callbacks: dict[ErrorSeverity, list[ErrorCallback]] = {}

    def register(self, callback: ErrorCallback) -> None:
        """
        Register a callback for all errors.

        Args:
            callback: Async function to call on error
        """
        self._callbacks.append(callback)

    def register_sync(self, callback: SyncErrorCallback) -> None:
        """
        Register a synchronous callback for all errors.

        Args:
            callback: Sync function to call on error
        """
        self._sync_callbacks.append(callback)

    def register_for_category(
        self,
        category: ErrorCategory,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific category.

        Args:
            category: The error category to filter on
            callback: Async function to call for matching errors
        """
        if category not in self._category_callbacks:
            self._category_callbacks[category] = []
        self._category_callbacks[category].append(callback)

    def register_for_severity(
        self,
        severity: ErrorSeverity,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific severity.

        Args:
            severity: The error severity to filter on
            callback: Async function to call for matching errors
        """
        if severity not in self._severity_callbacks:
            self._severity_callbacks[severity] = []
        self._severity_callbacks[severity].append(callback)

    async def notify(self, error_info: ErrorInfo) -> None:
        """
        Notify all registered callbacks about an error.

        Invokes callbacks in order:
        1. General callbacks
        2. Category-specific callbacks
        3. Severity-specific callbacks

        Errors in callbacks are logged but don't prevent other callbacks.

        Args:
            error_info: The error information to broadcast
        """
        # Invoke sync callbacks first
        for sync_cb in self._sync_callbacks:
            try:
                sync_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in sync error callback: {e}",
                    extra={
                        "callback": sync_cb.__name__,
                        "error_info": error_info.to_dict(),
                    },
                )

        # Invoke general async callbacks
        for async_cb in self._callbacks:
            try:
                await async_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in error callback: {e}",
                    extra={
                        "callback": async_cb.__name__,
                        "error_info": error_info.to_dict(),
                    },
                )

        # Invoke category-specific callbacks
        category = error_info.classification.category
        if category in self._category_callbacks:
            for cat_cb in self._category_callbacks[category]:
                try:
                    await cat_cb(error_info)
                except Exception as e:
                    logger.error(
                        f"Error in category callback: {e}",
                        extra={
                            "callback": cat_cb.__name__,
                            "category": category.value,
                        },
                    )

        # Invoke severity-specific callbacks
        severity = error_info.classification.severity
        if severity in self._severity_callbacks:
            for sev_cb in self._severity_callbacks[severity]:
                try:
                    await sev_cb(error_info)
                except Exception as e:
                    logger.error(
                        f"Error in severity callback: {e}",
                        extra={
                            "callback": sev_cb.__name__,
                            "severity": severity.value,
                        },
                    )

    def clear(self) -> None:
        """Remove all registered callbacks."""
        self._callbacks.clear()
        self._sync_callbacks.clear()
        self._category_callbacks.clear()
        self._severity_callbacks.clear()


# =============================================================================
# Error Handling Strategy
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


# =============================================================================
# Subscription Error Handler
# =============================================================================


class SubscriptionErrorHandler:
    """
    Unified error handler for subscription event processing.

    Integrates error classification, tracking, callbacks, and DLQ handling
    into a cohesive error handling strategy.

    This class is the main entry point for error handling in subscriptions.

    Example:
        >>> handler = SubscriptionErrorHandler(
        ...     subscription_name="OrderProjection",
        ...     dlq_repo=dlq_repo,
        ... )
        >>> handler.on_error(async_callback)
        >>> try:
        ...     await process_event(event)
        ... except Exception as e:
        ...     await handler.handle_error(e, stored_event)
    """

    def __init__(
        self,
        subscription_name: str,
        config: ErrorHandlingConfig | None = None,
        dlq_repo: "DLQRepository | None" = None,
        classifier: ErrorClassifier | None = None,
    ) -> None:
        """
        Initialize the subscription error handler.

        Args:
            subscription_name: Name of the subscription this handler is for
            config: Error handling configuration
            dlq_repo: Optional DLQ repository for dead letter handling
            classifier: Optional custom error classifier
        """
        self.subscription_name = subscription_name
        self.config = config or ErrorHandlingConfig()
        self._dlq_repo = dlq_repo
        self._classifier = classifier or get_default_classifier()
        self._callback_registry = ErrorHandlerRegistry()
        self._stats = ErrorStats()
        self._recent_errors: list[ErrorInfo] = []
        self._lock = asyncio.Lock()

    def on_error(self, callback: ErrorCallback) -> None:
        """
        Register a callback for error notifications.

        Args:
            callback: Async function to call on error
        """
        self._callback_registry.register(callback)

    def on_error_sync(self, callback: SyncErrorCallback) -> None:
        """
        Register a synchronous callback for error notifications.

        Args:
            callback: Sync function to call on error
        """
        self._callback_registry.register_sync(callback)

    def on_category(
        self,
        category: ErrorCategory,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific category.

        Args:
            category: The error category to filter on
            callback: Async function to call for matching errors
        """
        self._callback_registry.register_for_category(category, callback)

    def on_severity(
        self,
        severity: ErrorSeverity,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific severity.

        Args:
            severity: The error severity to filter on
            callback: Async function to call for matching errors
        """
        self._callback_registry.register_for_severity(severity, callback)

    async def handle_error(
        self,
        error: Exception,
        stored_event: "StoredEvent | None" = None,
        event: "DomainEvent | None" = None,
        retry_count: int = 0,
    ) -> ErrorInfo:
        """
        Handle a processing error.

        This method:
        1. Classifies the error
        2. Creates ErrorInfo with full context
        3. Logs the error
        4. Records statistics
        5. Optionally sends to DLQ
        6. Notifies callbacks

        Args:
            error: The exception that occurred
            stored_event: The stored event being processed (if available)
            event: The domain event being processed (if available)
            retry_count: Number of retry attempts made

        Returns:
            ErrorInfo with classification and tracking data
        """
        # Classify the error
        classification = self._classifier.classify(error)

        # Build error info
        error_info = ErrorInfo(
            event_id=self._get_event_id(stored_event, event),
            event_type=self._get_event_type(stored_event, event),
            position=self._get_position(stored_event),
            error_type=type(error).__name__,
            error_message=str(error),
            error_stacktrace=traceback.format_exc(),
            classification=classification,
            subscription_name=self.subscription_name,
            retry_count=retry_count,
        )

        # Log the error
        self._log_error(error_info)

        async with self._lock:
            # Record in statistics
            self._stats.record_error(error_info)

            # Track recent errors
            self._recent_errors.append(error_info)
            if len(self._recent_errors) > self.config.max_recent_errors:
                self._recent_errors = self._recent_errors[-self.config.max_recent_errors :]

        # Send to DLQ if configured and appropriate
        if self._should_send_to_dlq(error_info):
            await self._send_to_dlq(error_info, stored_event, event)

        # Notify callbacks
        if error_info.classification.severity.value >= self.config.notify_on_severity.value:
            await self._callback_registry.notify(error_info)

        return error_info

    def _get_event_id(
        self,
        stored_event: "StoredEvent | None",
        event: "DomainEvent | None",
    ) -> UUID:
        """Extract event ID from stored event or domain event."""
        if stored_event:
            return stored_event.event_id
        if event:
            return event.event_id
        from uuid import uuid4

        return uuid4()

    def _get_event_type(
        self,
        stored_event: "StoredEvent | None",
        event: "DomainEvent | None",
    ) -> str:
        """Extract event type from stored event or domain event."""
        if stored_event:
            return stored_event.event_type
        if event:
            return event.event_type
        return "Unknown"

    def _get_position(self, stored_event: "StoredEvent | None") -> int:
        """Extract global position from stored event."""
        if stored_event:
            return stored_event.global_position
        return -1

    def _log_error(self, error_info: ErrorInfo) -> None:
        """Log the error with appropriate level."""
        severity = error_info.classification.severity
        extra = {
            "subscription": self.subscription_name,
            "event_id": str(error_info.event_id),
            "event_type": error_info.event_type,
            "position": error_info.position,
            "error_type": error_info.error_type,
            "category": error_info.classification.category.value,
            "severity": severity.value,
            "retryable": error_info.classification.retryable,
            "retry_count": error_info.retry_count,
        }

        if severity == ErrorSeverity.CRITICAL:
            logger.critical(
                f"Critical event processing error: {error_info.error_message}",
                extra=extra,
            )
        elif severity == ErrorSeverity.HIGH:
            logger.error(
                f"Event processing error: {error_info.error_message}",
                extra=extra,
            )
        elif severity == ErrorSeverity.MEDIUM:
            logger.warning(
                f"Event processing warning: {error_info.error_message}",
                extra=extra,
            )
        else:
            logger.info(
                f"Event processing issue: {error_info.error_message}",
                extra=extra,
            )

    def _should_send_to_dlq(self, error_info: ErrorInfo) -> bool:
        """Determine if error should be sent to DLQ."""
        if not self.config.dlq_enabled:
            return False

        if self._dlq_repo is None:
            return False

        # Only send non-retryable errors or errors after all retries
        strategy = self.config.strategy

        if strategy == ErrorHandlingStrategy.DLQ_ONLY:
            return True

        if strategy in (
            ErrorHandlingStrategy.RETRY_THEN_DLQ,
            ErrorHandlingStrategy.RETRY_THEN_CONTINUE,
        ):
            # DLQ on permanent errors or after retries exhausted
            return not error_info.classification.retryable

        return False

    async def _send_to_dlq(
        self,
        error_info: ErrorInfo,
        stored_event: "StoredEvent | None",
        event: "DomainEvent | None",
    ) -> None:
        """Send failed event to dead letter queue."""
        if self._dlq_repo is None:
            return

        try:
            # Build event data from available sources
            event_data: dict[str, Any] = {}
            if event:
                event_data = event.model_dump(mode="json")
            elif stored_event and stored_event.event:
                event_data = stored_event.event.model_dump(mode="json")

            await self._dlq_repo.add_failed_event(
                event_id=error_info.event_id,
                projection_name=self.subscription_name,
                event_type=error_info.event_type,
                event_data=event_data,
                error=Exception(error_info.error_message),
                retry_count=error_info.retry_count,
            )

            error_info.sent_to_dlq = True

            logger.info(
                f"Event sent to DLQ for {self.subscription_name}",
                extra={
                    "subscription": self.subscription_name,
                    "event_id": str(error_info.event_id),
                    "error_type": error_info.error_type,
                },
            )

        except Exception as dlq_error:
            logger.error(
                f"Failed to send event to DLQ: {dlq_error}",
                extra={
                    "subscription": self.subscription_name,
                    "event_id": str(error_info.event_id),
                    "original_error": error_info.error_message,
                },
            )

    def should_continue(self) -> bool:
        """
        Check if processing should continue based on error state.

        Returns:
            False if max_errors_before_stop threshold is reached
        """
        if self.config.max_errors_before_stop is None:
            return True
        return self._stats.total_errors < self.config.max_errors_before_stop

    def should_retry(self, error: Exception) -> bool:
        """
        Check if an error should be retried.

        Args:
            error: The exception to check

        Returns:
            True if the error should be retried based on classification
        """
        return self._classifier.is_retryable(error)

    @property
    def stats(self) -> ErrorStats:
        """Get error statistics."""
        return self._stats

    @property
    def recent_errors(self) -> list[ErrorInfo]:
        """Get list of recent errors."""
        return list(self._recent_errors)

    @property
    def dlq_count(self) -> int:
        """Get count of events sent to DLQ."""
        return self._stats.dlq_count

    @property
    def total_errors(self) -> int:
        """Get total error count."""
        return self._stats.total_errors

    def get_health_status(self) -> dict[str, Any]:
        """
        Get health status based on error state.

        Returns:
            Dictionary with health indicators
        """
        is_healthy = True
        warnings: list[str] = []
        errors: list[str] = []

        # Check error rate threshold
        if (
            self.config.error_rate_threshold is not None
            and self._stats.error_rate_per_minute > self.config.error_rate_threshold
        ):
            is_healthy = False
            errors.append(
                f"Error rate {self._stats.error_rate_per_minute:.2f}/min "
                f"exceeds threshold {self.config.error_rate_threshold}/min"
            )

        # Check max errors
        if (
            self.config.max_errors_before_stop is not None
            and self._stats.total_errors >= self.config.max_errors_before_stop * 0.8
        ):
            warnings.append(
                f"Approaching error limit: {self._stats.total_errors}/"
                f"{self.config.max_errors_before_stop}"
            )

        # Check DLQ backlog
        if self._stats.dlq_count > 0:
            warnings.append(f"DLQ has {self._stats.dlq_count} unprocessed events")

        return {
            "healthy": is_healthy,
            "warnings": warnings,
            "errors": errors,
            "stats": self._stats.to_dict(),
            "recent_error_count": len(self._recent_errors),
        }

    async def clear_stats(self) -> None:
        """Clear all error statistics and recent errors."""
        async with self._lock:
            self._stats = ErrorStats()
            self._recent_errors.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Classification
    "ErrorCategory",
    "ErrorSeverity",
    "ErrorClassification",
    "ErrorClassifier",
    "get_default_classifier",
    # Tracking
    "ErrorInfo",
    "ErrorStats",
    # Callbacks
    "ErrorCallback",
    "SyncErrorCallback",
    "ErrorHandlerRegistry",
    # Strategy
    "ErrorHandlingStrategy",
    "ErrorHandlingConfig",
    # Handler
    "SubscriptionErrorHandler",
]
