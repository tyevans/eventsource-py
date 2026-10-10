"""
Indicators mixin for subscription health checking.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.health_check import (
    HealthCheckConfig,
    HealthIndicator,
    HealthStatus,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.error_handling import (
        SubscriptionErrorHandler,
    )
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.application.subscriptions.subscription import Subscription


class SubscriptionHealthIndicatorsMixin:
    """
    Mixin providing indicator evaluations for SubscriptionHealthChecker.
    """

    subscription: Subscription
    config: HealthCheckConfig
    _error_handler: SubscriptionErrorHandler | None
    _handler_circuit_breaker_provider: Callable[[], CircuitBreaker | None] | None
    _infra_circuit_breaker_provider: Callable[[], CircuitBreaker | None] | None

    def _check_state(self) -> HealthIndicator:
        """Check subscription state indicator."""
        from eventsource.application.subscriptions.subscription import SubscriptionState

        state = self.subscription.state

        if state == SubscriptionState.LIVE:
            return HealthIndicator(
                name="state",
                status=HealthStatus.HEALTHY,
                message="Subscription is live and processing events",
                details={"state": state.value},
            )
        elif state == SubscriptionState.CATCHING_UP:
            return HealthIndicator(
                name="state",
                status=HealthStatus.HEALTHY,
                message="Subscription is catching up on historical events",
                details={"state": state.value},
            )
        elif state == SubscriptionState.PAUSED:
            return HealthIndicator(
                name="state",
                status=HealthStatus.DEGRADED,
                message="Subscription is paused",
                details={"state": state.value},
            )
        elif state == SubscriptionState.ERROR:
            return HealthIndicator(
                name="state",
                status=HealthStatus.UNHEALTHY,
                message=f"Subscription in error state: {self.subscription.last_error}",
                details={
                    "state": state.value,
                    "error": str(self.subscription.last_error),
                },
            )
        elif state == SubscriptionState.STOPPED:
            return HealthIndicator(
                name="state",
                status=HealthStatus.DEGRADED,
                message="Subscription is stopped",
                details={"state": state.value},
            )
        else:
            return HealthIndicator(
                name="state",
                status=HealthStatus.UNKNOWN,
                message=f"Unknown state: {state.value}",
                details={"state": state.value},
            )

    def _check_errors(self) -> HealthIndicator:
        """Check error statistics indicator."""
        events_failed = self.subscription.events_failed

        error_rate = 0.0
        if self._error_handler:
            error_rate = self._error_handler.stats.error_rate_per_minute

        if events_failed >= self.config.max_errors_critical:
            return HealthIndicator(
                name="errors",
                status=HealthStatus.CRITICAL,
                message=f"Critical error count: {events_failed}",
                details={
                    "total_errors": events_failed,
                    "error_rate_per_minute": error_rate,
                    "threshold": self.config.max_errors_critical,
                },
            )
        elif (
            events_failed >= self.config.max_errors_warning
            or error_rate > self.config.max_error_rate_per_minute
        ):
            return HealthIndicator(
                name="errors",
                status=HealthStatus.DEGRADED,
                message=f"Elevated error count: {events_failed}",
                details={
                    "total_errors": events_failed,
                    "error_rate_per_minute": error_rate,
                    "warning_threshold": self.config.max_errors_warning,
                },
            )
        else:
            return HealthIndicator(
                name="errors",
                status=HealthStatus.HEALTHY,
                message="Error rate within acceptable limits",
                details={
                    "total_errors": events_failed,
                    "error_rate_per_minute": error_rate,
                },
            )

    def _check_lag(self) -> HealthIndicator:
        """Check subscription lag indicator."""
        lag = self.subscription.lag

        if lag >= self.config.max_lag_events_critical:
            return HealthIndicator(
                name="lag",
                status=HealthStatus.CRITICAL,
                message=f"Critical lag: {lag} events behind",
                details={
                    "lag_events": lag,
                    "threshold": self.config.max_lag_events_critical,
                },
            )
        elif lag >= self.config.max_lag_events_warning:
            return HealthIndicator(
                name="lag",
                status=HealthStatus.DEGRADED,
                message=f"High lag: {lag} events behind",
                details={
                    "lag_events": lag,
                    "warning_threshold": self.config.max_lag_events_warning,
                },
            )
        else:
            return HealthIndicator(
                name="lag",
                status=HealthStatus.HEALTHY,
                message=f"Lag within limits: {lag} events",
                details={"lag_events": lag},
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
                name=indicator_name,
                status=HealthStatus.UNKNOWN,
                message="Circuit breaker not available",
            )

        from eventsource.application.subscriptions.retry import CircuitState

        state = circuit_breaker.state

        if state == CircuitState.OPEN:
            status = (
                HealthStatus.UNHEALTHY
                if self.config.circuit_open_is_unhealthy
                else HealthStatus.DEGRADED
            )
            return HealthIndicator(
                name=indicator_name,
                status=status,
                message="Circuit breaker is OPEN - requests blocked",
                details={
                    "state": state.value,
                    "failure_count": circuit_breaker.failure_count,
                },
            )
        elif state == CircuitState.HALF_OPEN:
            return HealthIndicator(
                name=indicator_name,
                status=HealthStatus.DEGRADED,
                message="Circuit breaker is HALF_OPEN - testing recovery",
                details={
                    "state": state.value,
                    "failure_count": circuit_breaker.failure_count,
                },
            )
        else:  # CLOSED
            return HealthIndicator(
                name=indicator_name,
                status=HealthStatus.HEALTHY,
                message="Circuit breaker is CLOSED - normal operation",
                details={
                    "state": state.value,
                    "failure_count": circuit_breaker.failure_count,
                },
            )

    def _check_dlq(self) -> HealthIndicator:
        """Check DLQ backlog indicator."""
        if self._error_handler is None:
            return HealthIndicator(
                name="dlq",
                status=HealthStatus.UNKNOWN,
                message="Error handler not available",
            )

        dlq_count = self._error_handler.dlq_count

        if dlq_count >= self.config.max_dlq_events_critical:
            return HealthIndicator(
                name="dlq",
                status=HealthStatus.CRITICAL,
                message=f"Critical DLQ backlog: {dlq_count} events",
                details={
                    "dlq_count": dlq_count,
                    "threshold": self.config.max_dlq_events_critical,
                },
            )
        elif dlq_count >= self.config.max_dlq_events_warning:
            return HealthIndicator(
                name="dlq",
                status=HealthStatus.DEGRADED,
                message=f"DLQ backlog: {dlq_count} events",
                details={
                    "dlq_count": dlq_count,
                    "warning_threshold": self.config.max_dlq_events_warning,
                },
            )
        elif dlq_count > 0:
            return HealthIndicator(
                name="dlq",
                status=HealthStatus.HEALTHY,
                message=f"DLQ has {dlq_count} events",
                details={"dlq_count": dlq_count},
            )
        else:
            return HealthIndicator(
                name="dlq",
                status=HealthStatus.HEALTHY,
                message="DLQ is empty",
                details={"dlq_count": 0},
            )

    def _determine_overall_status(
        self,
        indicators: list[HealthIndicator],
    ) -> HealthStatus:
        """Determine overall health status from indicators."""
        status_priority = {
            HealthStatus.CRITICAL: 4,
            HealthStatus.UNHEALTHY: 3,
            HealthStatus.DEGRADED: 2,
            HealthStatus.UNKNOWN: 1,
            HealthStatus.HEALTHY: 0,
        }

        worst_status = HealthStatus.HEALTHY
        worst_priority = 0

        for indicator in indicators:
            priority = status_priority.get(indicator.status, 0)
            if priority > worst_priority:
                worst_priority = priority
                worst_status = indicator.status

        return worst_status


__all__ = ["SubscriptionHealthIndicatorsMixin"]
