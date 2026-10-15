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
            return HealthIndicator("state", HealthStatus.HEALTHY, "Subscription is live and processing events", details)  # fmt: skip
        if state == SubscriptionState.CATCHING_UP:
            return HealthIndicator("state", HealthStatus.HEALTHY, "Subscription is catching up on historical events", details)  # fmt: skip
        if state == SubscriptionState.PAUSED:
            return HealthIndicator("state", HealthStatus.DEGRADED, "Subscription is paused", details)  # fmt: skip
        if state == SubscriptionState.ERROR:
            return HealthIndicator(
                "state",
                HealthStatus.UNHEALTHY,
                f"Subscription in error state: {self.subscription.last_error}",
                {"state": state.value, "error": str(self.subscription.last_error)},
            )
        if state == SubscriptionState.STOPPED:
            return HealthIndicator("state", HealthStatus.DEGRADED, "Subscription is stopped", details)  # fmt: skip
        return HealthIndicator("state", HealthStatus.UNKNOWN, f"Unknown state: {state.value}", details)  # fmt: skip

    def _check_errors(self) -> HealthIndicator:
        """Check error statistics indicator."""
        events_failed = self.subscription.events_failed
        error_rate = self._error_handler.stats.error_rate_per_minute if self._error_handler else 0.0
        base: dict[str, object] = {
            "total_errors": events_failed,
            "error_rate_per_minute": error_rate,
        }
        if events_failed >= self.config.max_errors_critical:
            return HealthIndicator(
                "errors",
                HealthStatus.CRITICAL,
                f"Critical error count: {events_failed}",
                {**base, "threshold": self.config.max_errors_critical},
            )
        if (
            events_failed >= self.config.max_errors_warning
            or error_rate > self.config.max_error_rate_per_minute
        ):
            return HealthIndicator(
                "errors",
                HealthStatus.DEGRADED,
                f"Elevated error count: {events_failed}",
                {**base, "warning_threshold": self.config.max_errors_warning},
            )
        return HealthIndicator("errors", HealthStatus.HEALTHY, "Error rate within acceptable limits", base)  # fmt: skip

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
        return HealthIndicator("lag", HealthStatus.HEALTHY, f"Lag within limits: {lag} events", {"lag_events": lag})  # fmt: skip

    def _check_circuit_breaker(
        self, indicator_name: str, provider: Callable[[], CircuitBreaker | None]
    ) -> HealthIndicator:
        """Check one circuit breaker's state indicator."""
        if (cb := provider()) is None:
            return HealthIndicator(indicator_name, HealthStatus.UNKNOWN, "Circuit breaker not available")  # fmt: skip
        from eventsource.application.subscriptions.retry import CircuitState

        state = cb.state
        details = {"state": state.value, "failure_count": cb.failure_count}
        if state == CircuitState.OPEN:
            status = HealthStatus.UNHEALTHY if self.config.circuit_open_is_unhealthy else HealthStatus.DEGRADED  # fmt: skip
            return HealthIndicator(indicator_name, status, "Circuit breaker is OPEN - requests blocked", details)  # fmt: skip
        if state == CircuitState.HALF_OPEN:
            return HealthIndicator(indicator_name, HealthStatus.DEGRADED, "Circuit breaker is HALF_OPEN - testing recovery", details)  # fmt: skip
        return HealthIndicator(indicator_name, HealthStatus.HEALTHY, "Circuit breaker is CLOSED - normal operation", details)  # fmt: skip

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
        msg = f"DLQ has {dlq_count} events" if dlq_count > 0 else "DLQ is empty"
        return HealthIndicator("dlq", HealthStatus.HEALTHY, msg, {"dlq_count": dlq_count})

    def _determine_overall_status(self, indicators: list[HealthIndicator]) -> HealthStatus:
        """Determine overall health status from indicators."""
        priority = {
            HealthStatus.CRITICAL: 4,
            HealthStatus.UNHEALTHY: 3,
            HealthStatus.DEGRADED: 2,
            HealthStatus.UNKNOWN: 1,
            HealthStatus.HEALTHY: 0,
        }
        return max(
            (ind.status for ind in indicators),
            key=lambda s: priority.get(s, 0),
            default=HealthStatus.HEALTHY,
        )


__all__ = ["SubscriptionHealthIndicatorsMixin"]
