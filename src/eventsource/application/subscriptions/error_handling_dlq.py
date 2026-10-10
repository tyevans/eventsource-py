"""
DLQ and logging mixin for SubscriptionErrorHandler.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.subscriptions.error_handling_types import (
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    ErrorSeverity,
)
from eventsource.application.subscriptions.subscription import render_position
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent
    from eventsource.ports.dlq import DLQRepository
    from eventsource.ports.envelopes import EventEnvelope

logger = logging.getLogger(__name__)


class SubscriptionErrorDLQMixin:
    """
    Mixin providing logging, position extraction, and DLQ handling for SubscriptionErrorHandler.
    """

    subscription_name: str
    config: ErrorHandlingConfig
    _dlq_repo: DLQRepository | None

    def _get_event_id(
        self,
        envelope: EventEnvelope | None,
        event: DomainEvent | None,
    ) -> UUID:
        """Extract event ID from envelope or domain event."""
        if envelope:
            return envelope.event.event_id
        if event:
            return event.event_id
        from uuid import uuid4

        return uuid4()

    def _get_event_type(
        self,
        envelope: EventEnvelope | None,
        event: DomainEvent | None,
    ) -> str:
        """Extract event type from envelope or domain event."""
        if envelope:
            return envelope.event.event_type
        if event:
            return event.event_type
        return "Unknown"

    def _get_position(self, envelope: EventEnvelope | None) -> Position | None:
        """Extract the global-feed position from an envelope, if any."""
        if envelope:
            return envelope.position
        return None

    def _log_error(self, error_info: ErrorInfo) -> None:
        """Log the error with appropriate level."""
        severity = error_info.classification.severity
        extra = {
            "subscription": self.subscription_name,
            "event_id": str(error_info.event_id),
            "event_type": error_info.event_type,
            "position": render_position(error_info.position),
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
        envelope: EventEnvelope | None,
        event: DomainEvent | None,
    ) -> None:
        """Send failed event to dead letter queue."""
        if self._dlq_repo is None:
            return

        try:
            # Build event data from available sources
            event_data: dict[str, Any] = {}
            if event:
                event_data = event.model_dump(mode="json")
            elif envelope:
                event_data = envelope.event.model_dump(mode="json")

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


__all__ = ["SubscriptionErrorDLQMixin"]
