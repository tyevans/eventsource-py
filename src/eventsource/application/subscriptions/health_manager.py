"""
Manager health checker aggregating per-subscription health.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.application.subscriptions.health_check import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthIndicator,
    HealthStatus,
)
from eventsource.application.subscriptions.health_subscription import (
    SubscriptionHealthChecker,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.subscription import Subscription


class ManagerHealthChecker:
    """
    Health checker for the subscription manager.

    Aggregates health from all managed subscriptions and provides
    an overall manager health status.

    Example:
        >>> checker = ManagerHealthChecker(subscriptions, config)
        >>> result = checker.check()
        >>> print(f"Manager health: {result['status']}")
    """

    def __init__(
        self,
        subscriptions: list[Subscription],
        config: HealthCheckConfig | None = None,
        subscription_checkers: dict[str, SubscriptionHealthChecker] | None = None,
    ) -> None:
        """Initialize the manager health checker."""
        self.subscriptions = subscriptions
        self.config = config or HealthCheckConfig()
        self._subscription_checkers = subscription_checkers or {}

    def check(self) -> dict[str, Any]:
        """
        Perform comprehensive health check for all subscriptions.

        Returns:
            Dictionary with overall status and per-subscription health
        """
        subscription_results: dict[str, HealthCheckResult] = {}
        overall_indicators: list[HealthIndicator] = []

        for subscription in self.subscriptions:
            name = subscription.name

            if name in self._subscription_checkers:
                checker = self._subscription_checkers[name]
            else:
                checker = SubscriptionHealthChecker(
                    subscription=subscription,
                    config=self.config,
                )

            result = checker.check()
            subscription_results[name] = result

            overall_indicators.append(
                HealthIndicator(
                    name=f"subscription.{name}",
                    status=result.overall_status,
                    message=f"Subscription {name} is {result.overall_status.value}",
                    details={"subscription_name": name},
                )
            )

        overall_status = self._determine_overall_status(overall_indicators)

        return {
            "status": overall_status.value,
            "timestamp": datetime.now(UTC).isoformat(),
            "subscription_count": len(self.subscriptions),
            "subscriptions": {
                name: result.to_dict() for name, result in subscription_results.items()
            },
            "indicators": [i.to_dict() for i in overall_indicators],
        }

    def _determine_overall_status(
        self,
        indicators: list[HealthIndicator],
    ) -> HealthStatus:
        """Determine overall status from subscription statuses."""
        if not indicators:
            return HealthStatus.UNKNOWN

        status_counts: dict[HealthStatus, int] = {}
        for indicator in indicators:
            status_counts[indicator.status] = status_counts.get(indicator.status, 0) + 1

        if status_counts.get(HealthStatus.CRITICAL, 0) > 0:
            return HealthStatus.CRITICAL

        total = len(indicators)
        unhealthy = status_counts.get(HealthStatus.UNHEALTHY, 0)
        if unhealthy > total // 2:
            return HealthStatus.UNHEALTHY

        if unhealthy > 0:
            return HealthStatus.DEGRADED

        if status_counts.get(HealthStatus.DEGRADED, 0) > 0:
            return HealthStatus.DEGRADED

        if status_counts.get(HealthStatus.UNKNOWN, 0) == total:
            return HealthStatus.UNKNOWN

        return HealthStatus.HEALTHY


__all__ = ["ManagerHealthChecker"]
