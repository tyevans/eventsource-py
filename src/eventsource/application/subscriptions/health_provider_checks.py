"""Health check aggregation operations for subscription health monitoring."""

from typing import TYPE_CHECKING, Any

from eventsource.application.subscriptions.health import (
    HealthCheckResult,
    HealthStatus,
    ManagerHealthChecker,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
    from eventsource.application.subscriptions.health import (
        HealthCheckConfig,
        SubscriptionHealthChecker,
    )
    from eventsource.application.subscriptions.registry import SubscriptionRegistry


class HealthCheckAggregationMixin:
    """Mixin providing health check aggregation operations."""

    _registry: "SubscriptionRegistry"
    _config: "HealthCheckConfig"
    _health_checkers: "dict[str, SubscriptionHealthChecker]"
    _error_handlers: "dict[str, SubscriptionErrorHandler]"

    def check_health(self, subscription_name: str) -> HealthCheckResult:
        """
        Check health of a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            HealthCheckResult with status and indicators

        Raises:
            KeyError: If subscription not found
        """
        if subscription_name not in self._health_checkers:
            raise KeyError(f"Subscription '{subscription_name}' not found")

        return self._health_checkers[subscription_name].check()

    def check_all_health(self, is_running: bool) -> dict[str, Any]:
        """
        Check health of all subscriptions.

        Args:
            is_running: Whether the manager is running

        Returns:
            Dictionary with overall and per-subscription health
        """
        subscriptions = self._registry.get_all()

        checker = ManagerHealthChecker(
            subscriptions=subscriptions,
            config=self._config,
            subscription_checkers=self._health_checkers,
        )

        health = checker.check()

        # Add error stats
        stats: dict[str, Any] = {
            "total_errors": 0,
            "total_dlq_count": 0,
            "subscriptions": {},
        }

        for name, handler in self._error_handlers.items():
            sub_stats = handler.stats.to_dict()
            stats["subscriptions"][name] = sub_stats
            stats["total_errors"] += handler.total_errors
            stats["total_dlq_count"] += handler.dlq_count

        health["error_stats"] = stats

        return health

    def get_comprehensive_health(self, is_running: bool) -> dict[str, Any]:
        """
        Get comprehensive health report including all resilience metrics.

        Args:
            is_running: Whether the manager is running

        Returns:
            Complete health report dictionary
        """
        health = self.check_all_health(is_running)

        # Add recent errors per subscription
        recent_errors: dict[str, list[dict[str, Any]]] = {}
        for name, handler in self._error_handlers.items():
            recent_errors[name] = [e.to_dict() for e in handler.recent_errors[-10:]]

        health["recent_errors"] = recent_errors

        # Add subscription-level DLQ counts
        dlq_status: dict[str, int] = {}
        for name, subscription in self._registry.items():
            dlq_status[name] = subscription.dlq_count

        health["dlq_status"] = dlq_status

        return health

    def is_healthy(self, is_running: bool) -> bool:
        """
        Quick health check.

        Args:
            is_running: Whether the manager is running

        Returns:
            True if all subscriptions are healthy
        """
        health = self.check_all_health(is_running)
        return health.get("status") in (
            HealthStatus.HEALTHY.value,
            "healthy",
        )


__all__ = ["HealthCheckAggregationMixin"]
