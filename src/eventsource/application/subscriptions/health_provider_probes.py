"""Readiness, liveness, and status probes for subscription health monitoring."""

import asyncio
from typing import TYPE_CHECKING, Any

from eventsource.application.subscriptions.health import (
    LivenessStatus,
    ManagerHealth,
    ReadinessStatus,
    SubscriptionHealth,
)
from eventsource.application.subscriptions.subscription import SubscriptionState

if TYPE_CHECKING:
    from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
    from eventsource.application.subscriptions.health import (
        HealthCheckConfig,
        SubscriptionHealthChecker,
    )
    from eventsource.application.subscriptions.registry import SubscriptionRegistry


class HealthCheckProbesMixin:
    """Mixin providing Kubernetes-style readiness/liveness and status probes."""

    _registry: "SubscriptionRegistry"
    _health_checkers: "dict[str, SubscriptionHealthChecker]"
    _config: "HealthCheckConfig"
    _error_handlers: "dict[str, SubscriptionErrorHandler]"
    _lock: asyncio.Lock

    @property
    def uptime_seconds(self) -> float:
        """Get manager uptime in seconds."""
        raise NotImplementedError

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
                uptime_seconds=self.uptime_seconds,
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
            uptime_seconds=self.uptime_seconds,
        )


__all__ = ["HealthCheckProbesMixin"]
