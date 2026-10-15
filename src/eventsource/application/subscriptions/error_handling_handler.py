"""Subscription error handler coordinating classification, DLQ, callbacks, and health tracking."""

from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING, Any

from eventsource.application.subscriptions.error_handling_classifier import (
    ErrorClassifier,
    get_default_classifier,
)
from eventsource.application.subscriptions.error_handling_dlq import SubscriptionErrorDLQMixin
from eventsource.application.subscriptions.error_handling_registry import ErrorHandlerRegistry
from eventsource.application.subscriptions.error_handling_types import (
    ErrorCallback,
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorInfo,
    ErrorSeverity,
    ErrorStats,
    SyncErrorCallback,
)

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent
    from eventsource.ports.dlq import DLQRepository
    from eventsource.ports.envelopes import EventEnvelope


class SubscriptionErrorHandler(SubscriptionErrorDLQMixin):
    """Unified error handler for subscription event processing."""

    def __init__(
        self,
        subscription_name: str,
        config: ErrorHandlingConfig | None = None,
        dlq_repo: DLQRepository | None = None,
        classifier: ErrorClassifier | None = None,
    ) -> None:
        self.subscription_name = subscription_name
        self.config = config or ErrorHandlingConfig()
        self._dlq_repo = dlq_repo
        self._classifier = classifier or get_default_classifier()
        self._callback_registry = ErrorHandlerRegistry()
        self._stats = ErrorStats()
        self._recent_errors: list[ErrorInfo] = []
        self._lock = asyncio.Lock()

    def on_error(self, callback: ErrorCallback) -> None:
        self._callback_registry.register(callback)

    def on_error_sync(self, callback: SyncErrorCallback) -> None:
        self._callback_registry.register_sync(callback)

    def on_category(self, category: ErrorCategory, callback: ErrorCallback) -> None:
        self._callback_registry.register_for_category(category, callback)

    def on_severity(self, severity: ErrorSeverity, callback: ErrorCallback) -> None:
        self._callback_registry.register_for_severity(severity, callback)

    async def handle_error(
        self,
        error: Exception,
        envelope: EventEnvelope | None = None,
        event: DomainEvent | None = None,
        retry_count: int = 0,
    ) -> ErrorInfo:
        """Handle a processing error (classify, log, record stats, DLQ, notify callbacks)."""
        error_info = ErrorInfo(
            event_id=self._get_event_id(envelope, event),
            event_type=self._get_event_type(envelope, event),
            position=self._get_position(envelope),
            error_type=type(error).__name__,
            error_message=str(error),
            error_stacktrace=traceback.format_exc(),
            classification=self._classifier.classify(error),
            subscription_name=self.subscription_name,
            retry_count=retry_count,
        )
        self._log_error(error_info)

        async with self._lock:
            self._stats.record_error(error_info)
            self._recent_errors.append(error_info)
            if len(self._recent_errors) > self.config.max_recent_errors:
                self._recent_errors = self._recent_errors[-self.config.max_recent_errors :]

        if self._should_send_to_dlq(error_info):
            await self._send_to_dlq(error_info, envelope, event)

        if error_info.classification.severity.value >= self.config.notify_on_severity.value:
            await self._callback_registry.notify(error_info)

        return error_info

    def should_continue(self) -> bool:
        if self.config.max_errors_before_stop is None:
            return True
        return self._stats.total_errors < self.config.max_errors_before_stop

    def should_retry(self, error: Exception) -> bool:
        return self._classifier.is_retryable(error)

    @property
    def stats(self) -> ErrorStats:
        return self._stats

    @property
    def recent_errors(self) -> list[ErrorInfo]:
        return list(self._recent_errors)

    @property
    def dlq_count(self) -> int:
        return self._stats.dlq_count

    @property
    def total_errors(self) -> int:
        return self._stats.total_errors

    def get_health_status(self) -> dict[str, Any]:
        """Get health status based on error state."""
        is_healthy = True
        warnings: list[str] = []
        errors: list[str] = []
        thresh = self.config.error_rate_threshold
        rate = self._stats.error_rate_per_minute
        if thresh is not None and rate > thresh:
            is_healthy = False
            errors.append(f"Error rate {rate:.2f}/min exceeds threshold {thresh}/min")

        max_err = self.config.max_errors_before_stop
        if max_err is not None and self._stats.total_errors >= max_err * 0.8:
            warnings.append(f"Approaching error limit: {self._stats.total_errors}/{max_err}")

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
        async with self._lock:
            self._stats = ErrorStats()
            self._recent_errors.clear()


__all__ = ["ErrorHandlerRegistry", "SubscriptionErrorHandler"]
