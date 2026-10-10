"""Resilient payment analytics projection and error handlers."""

import logging
import random
from typing import Any

from eventsource.application.subscriptions import (
    CircuitBreaker,
    CircuitBreakerConfig,
    ErrorInfo,
)
from eventsource.domain import DomainEvent
from eventsource.ports.handlers import EventSubscriber

from .domain import PaymentFailed, PaymentReceived, PaymentRefunded

logger = logging.getLogger(__name__)


class PaymentAnalyticsProjection(EventSubscriber):
    """
    Production-ready projection with built-in resilience.

    This projection demonstrates:
    - Simulated transient failures that trigger retry logic
    - Simulated permanent failures that go to DLQ
    - Revenue tracking with error handling
    """

    def __init__(
        self,
        failure_rate: float = 0.3,  # 30% transient failure rate
        permanent_failure_rate: float = 0.05,  # 5% permanent failure rate
    ):
        """
        Initialize the projection.

        Args:
            failure_rate: Probability of transient failure (for demo)
            permanent_failure_rate: Probability of permanent failure (for demo)
        """
        self.failure_rate = failure_rate
        self.permanent_failure_rate = permanent_failure_rate

        # Read model
        self.total_revenue: float = 0.0
        self.total_refunds: float = 0.0
        self.payment_count: int = 0
        self.failed_payment_count: int = 0
        self.events_processed: int = 0

        # Circuit breaker for external service calls
        self._circuit_breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,  # Open after 3 failures
                recovery_timeout=10.0,  # Try recovery after 10 seconds
            )
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [PaymentReceived, PaymentFailed, PaymentRefunded]

    async def handle(self, event: DomainEvent) -> None:
        """
        Process a single event with simulated failures.

        In production, failures would come from:
        - Database connection errors
        - External API failures
        - Network timeouts
        """
        self.events_processed += 1

        # Simulate permanent failure (e.g., data validation error)
        if random.random() < self.permanent_failure_rate:
            raise ValueError(
                f"Data validation error for event {event.event_id} - this error cannot be retried"
            )

        # Simulate transient failure (e.g., network timeout)
        if random.random() < self.failure_rate:
            raise ConnectionError(f"Transient network error processing event {event.event_id}")

        # Process the event using circuit breaker
        await self._process_with_circuit_breaker(event)

    async def _process_with_circuit_breaker(self, event: DomainEvent) -> None:
        """Process event through circuit breaker."""

        async def process() -> None:
            await self._update_read_model(event)

        await self._circuit_breaker.execute(process, "update_read_model")

    async def _update_read_model(self, event: DomainEvent) -> None:
        """Update the in-memory read model."""
        if isinstance(event, PaymentReceived):
            self.total_revenue += event.amount
            self.payment_count += 1
            logger.info(
                f"Payment received: ${event.amount:.2f}, total revenue=${self.total_revenue:.2f}"
            )

        elif isinstance(event, PaymentFailed):
            self.failed_payment_count += 1
            logger.info(
                f"Payment failed: {event.reason}, total failures={self.failed_payment_count}"
            )

        elif isinstance(event, PaymentRefunded):
            self.total_refunds += event.amount
            logger.info(
                f"Payment refunded: ${event.amount:.2f}, total refunds=${self.total_refunds:.2f}"
            )

    def get_stats(self) -> dict[str, Any]:
        """Get analytics statistics."""
        return {
            "total_revenue": self.total_revenue,
            "total_refunds": self.total_refunds,
            "net_revenue": self.total_revenue - self.total_refunds,
            "payment_count": self.payment_count,
            "failed_payment_count": self.failed_payment_count,
            "events_processed": self.events_processed,
            "circuit_breaker_state": self._circuit_breaker.state.value,
        }


# =============================================================================
# Error Callback Functions
# =============================================================================
# These would typically send alerts to monitoring systems


async def on_any_error(error_info: ErrorInfo) -> None:
    """Called for any error - log to monitoring system."""
    logger.warning(f"Error notification: {error_info.error_type} - {error_info.error_message}")


async def on_transient_error(error_info: ErrorInfo) -> None:
    """Called for transient errors - these will be retried."""
    logger.info(
        f"Transient error (will retry): {error_info.error_message}, "
        f"attempt {error_info.retry_count + 1}"
    )


async def on_critical_error(error_info: ErrorInfo) -> None:
    """Called for critical errors - send alert!"""
    logger.error(
        f"CRITICAL ERROR - ALERT REQUIRED: {error_info.error_type} - {error_info.error_message}"
    )
    # In production: await send_pagerduty_alert(error_info)
