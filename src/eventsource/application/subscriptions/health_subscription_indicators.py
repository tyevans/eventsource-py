"""Indicators mixin for subscription health checking."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.health_check import (
    HealthCheckConfig,
    HealthIndicator,
    HealthStatus,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.application.subscriptions.subscription import Subscription


class SubscriptionHealthIndicatorsMixin:
    """Mixin providing indicator evaluations for SubscriptionHealthChecker."""

    subscription: Subscription
    config: HealthCheckConfig
    _error_handler: SubscriptionErrorHandler | None
    _handler_circuit_breaker_provider: Callable[[], CircuitBreaker | None] | None
    _infra_circuit_breaker_provider: Callable[[], CircuitBreaker | None] | None

    def _check_state(self) -> HealthIndicator:
        """Check subscription state indicator."""
        from eventsource.application.subscriptions.subscription import SubscriptionState

        state = self.subscription.state
        details: dict[str, object] = {"state": state.value}
        if state == SubscriptionState.LIVE:
            return HealthIndicator(
                "state", HealthStatus.HEALTHY, "Subscription is live and processing events", details
            )
        if state == SubscriptionState.CATCHING_UP:
            return HealthIndicator(
                "state",
                HealthStatus.HEALTHY,
                "Subscription is catching up on historical events",
                details,
            )
        if state == SubscriptionState.PAUSED:
            return HealthIndicator(
                "state", HealthStatus.DEGRADED, "Subscription is paused", details
            )
        if state == SubscriptionState.ERROR:
            return HealthIndicator(
                "state",
                HealthStatus.UNHEALTHY,
                f"Subscription in error state: {self.subscription.last_error}",
                {"state": state.value, "error": str(self.subscription.last_error)},
            )
        if state == SubscriptionState.STOPPED:
            return HealthIndicator(
                "state", HealthStatus.DEGRADED, "Subscription is stopped", details
            )
        return HealthIndicator(
            "state", HealthStatus.UNKNOWN, f"Unknown state: {state.value}", details
        )

    def _check_errors(self) -> HealthIndicator:
        """Check error statistics indicator."""
        events_failed = self.subscription.events_failed
        error_rate = self._error_handler.stats.error_rate_per_minute if self._error_handler else 0.0
        base_details: dict[str, object] = {
            "total_errors": events_failed,
            "error_rate_per_minute": error_rate,
        }
        if events_failed >= self.config.max_errors_critical:
            return HealthIndicator(
                "errors",
                HealthStatus.CRITICAL,
                f"Critical error count: {events_failed}",
                {**base_details, "threshold": self.config.max_errors_critical},
            )
        if (
            events_failed >= self.config.max_errors_warning
            or error_rate > self.config.max_error_rate_per_minute
        ):
            return HealthIndicator(
                "errors",
                HealthStatus.DEGRADED,
                f"Elevated error count: {events_failed}",
                {**base_details, "warning_threshold": self.config.max_errors_warning},
            )
        return HealthIndicator(
            "errors", HealthStatus.HEALTHY, "Error rate within acceptable limits", base_details
        )

    def _check_lag(self) -> HealthIndicator:
        """Check subscription lag indicator."""
        lag = self.subscription.lag
        if lag >= self.config.max_lag_events_critical:
            return HealthIndicator(
                "lag",
                HealthStatus.CRITICAL,
                f"Critical lag: {lag} events behind",
                {"lag_events": lag, "threshold": self.config.max_lag_events_critical},
            )
        if lag >= self.config.max_lag_events_warning:
            return HealthIndicator(
                "lag",
                HealthStatus.DEGRADED,
                f"High lag: {lag} events behind",
                {"lag_events": lag, "warning_threshold": self.config.max_lag_events_warning},
            )
        return HealthIndicator(
            "lag", HealthStatus.HEALTHY, f"Lag within limits: {lag} events", {"lag_events": lag}
        )

    def _check_circuit_breaker(
        self,
        indicator_name: str,
        provider: Callable[[], CircuitBreaker | None],
    ) -> HealthIndicator:
        """Check one circuit breaker's state indicator."""
        circuit_breaker = provider()
        if circuit_breaker is None:
            return HealthIndicator(
                indicator_name, HealthStatus.UNKNOWN, "Circuit breaker not available"
            )

        from eventsource.application.subscriptions.retry import CircuitState

        state = circuit_breaker.state
        details = {"state": state.value, "failure_count": circuit_breaker.failure_count}
        if state == CircuitState.OPEN:
            status = (
                HealthStatus.UNHEALTHY
                if self.config.circuit_open_is_unhealthy
                else HealthStatus.DEGRADED
            )
            return HealthIndicator(
                indicator_name, status, "Circuit breaker is OPEN - requests blocked", details
            )
        if state == CircuitState.HALF_OPEN:
            return HealthIndicator(
                indicator_name,
                HealthStatus.DEGRADED,
                "Circuit breaker is HALF_OPEN - testing recovery",
                details,
            )
        return HealthIndicator(
            indicator_name,
            HealthStatus.HEALTHY,
            "Circuit breaker is CLOSED - normal operation",
            details,
        )

    def _check_dlq(self) -> HealthIndicator:
        """Check DLQ backlog indicator."""
        if self._error_handler is None:
            return HealthIndicator("dlq", HealthStatus.UNKNOWN, "Error handler not available")

        dlq_count = self._error_handler.dlq_count
        if dlq_count >= self.config.max_dlq_events_critical:
            return HealthIndicator(
                "dlq",
                HealthStatus.CRITICAL,
                f"Critical DLQ backlog: {dlq_count} events",
                {"dlq_count": dlq_count, "threshold": self.config.max_dlq_events_critical},
            )
        if dlq_count >= self.config.max_dlq_events_warning:
            return HealthIndicator(
                "dlq",
                HealthStatus.DEGRADED,
                f"DLQ backlog: {dlq_count} events",
                {"dlq_count": dlq_count, "warning_threshold": self.config.max_dlq_events_warning},
            )
        if dlq_count > 0:
            return HealthIndicator(
                "dlq", HealthStatus.HEALTHY, f"DLQ has {dlq_count} events", {"dlq_count": dlq_count}
            )
        return HealthIndicator("dlq", HealthStatus.HEALTHY, "DLQ is empty", {"dlq_count": 0})

    def _determine_overall_status(self, indicators: list[HealthIndicator]) -> HealthStatus:
        """Determine overall health status from indicators."""
        status_priority = {
            HealthStatus.CRITICAL: 4,
            HealthStatus.UNHEALTHY: 3,
            HealthStatus.DEGRADED: 2,
            HealthStatus.UNKNOWN: 1,
            HealthStatus.HEALTHY: 0,
        }
        return max(
            (ind.status for ind in indicators),
            key=lambda s: status_priority.get(s, 0),
            default=HealthStatus.HEALTHY,
        )


__all__ = ["SubscriptionHealthIndicatorsMixin"]
