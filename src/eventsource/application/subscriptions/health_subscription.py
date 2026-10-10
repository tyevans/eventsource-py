"""
Subscription health checker for individual subscriptions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.health_check import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthIndicator,
)
from eventsource.application.subscriptions.health_subscription_indicators import (
    SubscriptionHealthIndicatorsMixin,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.error_handling import (
        SubscriptionErrorHandler,
    )
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.application.subscriptions.subscription import Subscription


class SubscriptionHealthChecker(SubscriptionHealthIndicatorsMixin):
    """
    Health checker for individual subscriptions.

    Evaluates subscription health based on multiple indicators:
    - State (running, stopped, error)
    - Error statistics
    - Circuit breaker state
    - Lag metrics
    - DLQ backlog

    Example:
        >>> checker = SubscriptionHealthChecker(subscription, config)
        >>> result = checker.check()
        >>> if result.overall_status == HealthStatus.UNHEALTHY:
        ...     await alert_ops_team(result)
    """

    def __init__(
        self,
        subscription: Subscription,
        config: HealthCheckConfig | None = None,
        error_handler: SubscriptionErrorHandler | None = None,
        handler_circuit_breaker_provider: Callable[[], CircuitBreaker | None] | None = None,
        infra_circuit_breaker_provider: Callable[[], CircuitBreaker | None] | None = None,
    ) -> None:
        """Initialize the health checker."""
        self.subscription = subscription
        self.config = config or HealthCheckConfig()
        self._error_handler = error_handler
        self._handler_circuit_breaker_provider = handler_circuit_breaker_provider
        self._infra_circuit_breaker_provider = infra_circuit_breaker_provider

    def check(self) -> HealthCheckResult:
        """
        Perform comprehensive health check.

        Evaluates all health indicators and determines overall status.

        Returns:
            HealthCheckResult with overall status and individual indicators
        """
        indicators: list[HealthIndicator] = []

        # Check subscription state
        indicators.append(self._check_state())

        # Check error statistics
        indicators.append(self._check_errors())

        # Check lag
        indicators.append(self._check_lag())

        # Check circuit breakers
        if self._handler_circuit_breaker_provider is not None:
            indicators.append(
                self._check_circuit_breaker(
                    "handler_circuit_breaker", self._handler_circuit_breaker_provider
                )
            )
        if self._infra_circuit_breaker_provider is not None:
            indicators.append(
                self._check_circuit_breaker(
                    "infra_circuit_breaker", self._infra_circuit_breaker_provider
                )
            )

        # Check DLQ
        if self._error_handler:
            indicators.append(self._check_dlq())

        # Determine overall status
        overall_status = self._determine_overall_status(indicators)

        return HealthCheckResult(
            overall_status=overall_status,
            indicators=indicators,
            subscription_name=self.subscription.name,
            uptime_seconds=self.subscription.uptime_seconds,
        )


__all__ = ["SubscriptionHealthChecker"]
