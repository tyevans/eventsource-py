"""
Health check provider for subscription health monitoring.

The HealthCheckProvider follows the Single Responsibility Principle by
handling only health-related operations:
- Health checks for individual subscriptions
- Manager-wide health aggregation
- Kubernetes-style readiness and liveness probes

Example:
    >>> provider = HealthCheckProvider(
    ...     registry=registry,
    ...     error_handlers=error_handlers,
    ...     config=HealthCheckConfig(),
    ... )
    >>> health = await provider.health_check()
    >>> readiness = await provider.readiness_check()
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.subscriptions.error_handling import SubscriptionErrorHandler
from eventsource.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthStatus,
    LivenessStatus,
    ManagerHealth,
    ManagerHealthChecker,
    ReadinessStatus,
    SubscriptionHealth,
    SubscriptionHealthChecker,
)
from eventsource.subscriptions.subscription import Subscription, SubscriptionState

if TYPE_CHECKING:
    from eventsource.subscriptions.registry import SubscriptionRegistry

logger = logging.getLogger(__name__)


class HealthCheckProvider:
    """
    Provides health check functionality for subscriptions.

    Handles:
    - Individual subscription health checks
    - Manager-wide health aggregation
    - Kubernetes-style readiness and liveness probes
    - Health checker lifecycle management

    Example:
        >>> provider = HealthCheckProvider(
        ...     registry=registry,
        ...     error_handlers=error_handlers,
        ... )
        >>> health = await provider.health_check()
    """

    def __init__(
        self,
        registry: "SubscriptionRegistry",
        error_handlers: dict[str, SubscriptionErrorHandler],
        config: HealthCheckConfig | None = None,
        lock: asyncio.Lock | None = None,
    ) -> None:
        """
        Initialize the health check provider.

        Args:
            registry: Subscription registry to monitor
            error_handlers: Error handlers for subscriptions
            config: Health check configuration
            lock: Optional shared lock for thread safety
        """
        self._registry = registry
        self._error_handlers = error_handlers
        self._config = config or HealthCheckConfig()
        self._lock = lock or asyncio.Lock()
        self._health_checkers: dict[str, SubscriptionHealthChecker] = {}
        self._started_at: datetime | None = None

    def register_subscription(
        self,
        subscription: Subscription,
        error_handler: SubscriptionErrorHandler,
    ) -> SubscriptionHealthChecker:
        """
        Register a subscription for health monitoring.

        Args:
            subscription: The subscription to monitor
            error_handler: The subscription's error handler

        Returns:
            The created SubscriptionHealthChecker
        """
        checker = SubscriptionHealthChecker(
            subscription=subscription,
            config=self._config,
            error_handler=error_handler,
        )
        self._health_checkers[subscription.name] = checker
        return checker

    def unregister_subscription(self, name: str) -> None:
        """
        Unregister a subscription from health monitoring.

        Args:
            name: Subscription name to unregister
        """
        self._health_checkers.pop(name, None)

    def set_started(self, started_at: datetime | None = None) -> None:
        """
        Mark the manager as started.

        Args:
            started_at: The start time (defaults to now)
        """
        self._started_at = started_at or datetime.now(UTC)

    def clear_started(self) -> None:
        """Clear the started timestamp."""
        self._started_at = None

    @property
    def uptime_seconds(self) -> float:
        """
        Get manager uptime in seconds.

        Returns:
            Seconds since manager was started, or 0 if not started.
        """
        if self._started_at is None:
            return 0.0
        return (datetime.now(UTC) - self._started_at).total_seconds()

    def get_health_checker(
        self,
        subscription_name: str,
    ) -> SubscriptionHealthChecker | None:
        """
        Get the health checker for a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            SubscriptionHealthChecker or None if not found
        """
        return self._health_checkers.get(subscription_name)

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

    async def health_check(self, is_running: bool) -> ManagerHealth:
        """
        Get comprehensive health status of the subscription manager.

        This is the primary health check API that provides:
        - Overall manager health status (healthy/degraded/unhealthy/critical)
        - Running state
        - Subscription counts by health category
        - Aggregate metrics (events processed, failed, lag)
        - Per-subscription health details

        Args:
            is_running: Whether the manager is running

        Returns:
            ManagerHealth with complete health information
        """
        subscription_health_list: list[SubscriptionHealth] = []
        healthy = degraded = unhealthy = 0
        total_lag = 0
        total_processed = 0
        total_failed = 0

        for name, subscription in self._registry.items():
            checker = self._health_checkers.get(name)
            if checker:
                result = checker.check()
                status = result.overall_status.value
            else:
                if subscription.state == SubscriptionState.LIVE:
                    if subscription.lag < self._config.max_lag_events_warning:
                        status = "healthy"
                    else:
                        status = "degraded"
                elif subscription.state in (
                    SubscriptionState.CATCHING_UP,
                    SubscriptionState.PAUSED,
                ):
                    status = "degraded"
                elif subscription.state == SubscriptionState.ERROR:
                    status = "unhealthy"
                else:
                    status = "unknown"

            if status == "healthy":
                healthy += 1
            elif status == "degraded":
                degraded += 1
            else:
                unhealthy += 1

            error_rate = 0.0
            handler = self._error_handlers.get(name)
            if handler:
                error_rate = handler.stats.error_rate_per_minute

            sub_health = SubscriptionHealth(
                name=name,
                status=status,
                state=subscription.state.value,
                events_processed=subscription.events_processed,
                events_failed=subscription.events_failed,
                lag_events=subscription.lag,
                error_rate=error_rate,
                last_error=str(subscription.last_error) if subscription.last_error else None,
                uptime_seconds=subscription.uptime_seconds,
            )
            subscription_health_list.append(sub_health)

            total_lag += subscription.lag
            total_processed += subscription.events_processed
            total_failed += subscription.events_failed

        if unhealthy > 0:
            overall = "unhealthy"
        elif degraded > 0:
            overall = "degraded"
        elif healthy > 0:
            overall = "healthy"
        else:
            overall = "unknown"

        return ManagerHealth(
            status=overall,
            running=is_running,
            subscription_count=len(self._registry),
            healthy_count=healthy,
            degraded_count=degraded,
            unhealthy_count=unhealthy,
            total_events_processed=total_processed,
            total_events_failed=total_failed,
            total_lag_events=total_lag,
            uptime_seconds=self.uptime_seconds,
            subscriptions=subscription_health_list,
        )

    async def readiness_check(
        self,
        is_running: bool,
        is_shutting_down: bool,
    ) -> ReadinessStatus:
        """
        Check if the manager is ready to accept work (Kubernetes-style probe).

        A manager is ready when:
        - It is running
        - No subscriptions are in ERROR state
        - Not currently shutting down

        Args:
            is_running: Whether the manager is running
            is_shutting_down: Whether shutdown has been requested

        Returns:
            ReadinessStatus indicating readiness state
        """
        details: dict[str, Any] = {
            "running": is_running,
            "subscription_count": len(self._registry),
            "shutting_down": is_shutting_down,
        }

        if is_shutting_down:
            return ReadinessStatus(
                ready=False,
                reason="Manager is shutting down",
                details=details,
            )

        if not is_running:
            return ReadinessStatus(
                ready=False,
                reason="Manager is not running",
                details=details,
            )

        error_subscriptions = []
        starting_subscriptions = []

        for name, subscription in self._registry.items():
            if subscription.state == SubscriptionState.ERROR:
                error_subscriptions.append(name)
            elif subscription.state == SubscriptionState.STARTING:
                starting_subscriptions.append(name)

        if error_subscriptions:
            details["error_subscriptions"] = error_subscriptions
            return ReadinessStatus(
                ready=False,
                reason=f"Subscriptions in error state: {', '.join(error_subscriptions)}",
                details=details,
            )

        if starting_subscriptions:
            details["starting_subscriptions"] = starting_subscriptions
            return ReadinessStatus(
                ready=False,
                reason=f"Subscriptions still starting: {', '.join(starting_subscriptions)}",
                details=details,
            )

        return ReadinessStatus(
            ready=True,
            reason="Manager is ready",
            details=details,
        )

    async def liveness_check(self, is_running: bool) -> LivenessStatus:
        """
        Check if the manager is alive and responsive (Kubernetes-style probe).

        A manager is live when:
        - It has not crashed
        - Its internal async lock is not deadlocked
        - It can respond to health checks

        Args:
            is_running: Whether the manager is running

        Returns:
            LivenessStatus indicating liveness state
        """
        details: dict[str, Any] = {
            "running": is_running,
            "uptime_seconds": self.uptime_seconds,
        }

        try:
            lock_acquired = await asyncio.wait_for(
                self._try_acquire_lock(),
                timeout=5.0,
            )
            details["lock_responsive"] = lock_acquired
        except TimeoutError:
            return LivenessStatus(
                alive=False,
                reason="Internal lock not responsive (possible deadlock)",
                details=details,
            )

        try:
            details["subscription_count"] = len(self._registry)
        except Exception as e:
            return LivenessStatus(
                alive=False,
                reason=f"Error accessing subscriptions: {e}",
                details=details,
            )

        return LivenessStatus(
            alive=True,
            reason="Manager is alive and responsive",
            details=details,
        )

    async def _try_acquire_lock(self) -> bool:
        """
        Try to acquire the internal lock.

        Returns:
            True if lock was acquired (and released), False otherwise
        """
        try:
            async with asyncio.timeout(1.0):
                async with self._lock:
                    return True
        except TimeoutError:
            return False

    def get_subscription_health(
        self,
        subscription_name: str,
    ) -> SubscriptionHealth | None:
        """
        Get health status for a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            SubscriptionHealth for the subscription, or None if not found
        """
        subscription = self._registry.get(subscription_name)
        if subscription is None:
            return None

        checker = self._health_checkers.get(subscription_name)
        if checker:
            result = checker.check()
            status = result.overall_status.value
        else:
            if subscription.state == SubscriptionState.LIVE:
                if subscription.lag < self._config.max_lag_events_warning:
                    status = "healthy"
                else:
                    status = "degraded"
            elif subscription.state in (
                SubscriptionState.CATCHING_UP,
                SubscriptionState.PAUSED,
            ):
                status = "degraded"
            elif subscription.state == SubscriptionState.ERROR:
                status = "unhealthy"
            else:
                status = "unknown"

        error_rate = 0.0
        handler = self._error_handlers.get(subscription_name)
        if handler:
            error_rate = handler.stats.error_rate_per_minute

        return SubscriptionHealth(
            name=subscription_name,
            status=status,
            state=subscription.state.value,
            events_processed=subscription.events_processed,
            events_failed=subscription.events_failed,
            lag_events=subscription.lag,
            error_rate=error_rate,
            last_error=str(subscription.last_error) if subscription.last_error else None,
            uptime_seconds=subscription.uptime_seconds,
        )

    @property
    def total_errors(self) -> int:
        """Get total error count across all subscriptions."""
        return sum(handler.total_errors for handler in self._error_handlers.values())

    @property
    def total_dlq_count(self) -> int:
        """Get total DLQ count across all subscriptions."""
        return sum(handler.dlq_count for handler in self._error_handlers.values())

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


__all__ = ["HealthCheckProvider"]
