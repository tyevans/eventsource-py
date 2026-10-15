"""Readiness, liveness, and status probes for subscription health monitoring."""

import asyncio
from typing import TYPE_CHECKING, Any

from eventsource.application.subscriptions.health import (
    LivenessStatus,
    ManagerHealth,
    ReadinessStatus,
    SubscriptionHealth,
)
from eventsource.application.subscriptions.subscription import Subscription, SubscriptionState

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
        """Try to acquire the internal lock within 1.0s; return True if acquired."""
        try:
            async with asyncio.timeout(1.0), self._lock:
                return True
        except TimeoutError:
            return False

    def _build_subscription_health(
        self, name: str, subscription: Subscription
    ) -> SubscriptionHealth:
        """Evaluate status and construct SubscriptionHealth for a single subscription."""
        checker = self._health_checkers.get(name)
        if checker:
            status = checker.check().overall_status.value
        elif subscription.state == SubscriptionState.LIVE:
            status = "healthy" if subscription.lag < self._config.max_lag_events_warning else "degraded"  # fmt: skip
        elif subscription.state in (SubscriptionState.CATCHING_UP, SubscriptionState.PAUSED):
            status = "degraded"
        elif subscription.state == SubscriptionState.ERROR:
            status = "unhealthy"
        else:
            status = "unknown"

        handler = self._error_handlers.get(name)
        return SubscriptionHealth(
            name=name,
            status=status,
            state=subscription.state.value,
            events_processed=subscription.events_processed,
            events_failed=subscription.events_failed,
            lag_events=subscription.lag,
            error_rate=handler.stats.error_rate_per_minute if handler else 0.0,
            last_error=str(subscription.last_error) if subscription.last_error else None,
            uptime_seconds=self.uptime_seconds,
        )

    async def health_check(self, is_running: bool) -> ManagerHealth:
        """Get comprehensive health status of the subscription manager."""
        subscription_health_list: list[SubscriptionHealth] = []
        healthy = degraded = unhealthy = total_lag = total_processed = total_failed = 0

        for name, subscription in self._registry.items():
            sub_health = self._build_subscription_health(name, subscription)
            subscription_health_list.append(sub_health)
            if sub_health.status == "healthy":
                healthy += 1
            elif sub_health.status == "degraded":
                degraded += 1
            else:
                unhealthy += 1
            total_lag += subscription.lag
            total_processed += subscription.events_processed
            total_failed += subscription.events_failed

        overall = "unhealthy" if unhealthy > 0 else ("degraded" if degraded > 0 else ("healthy" if healthy > 0 else "unknown"))  # fmt: skip
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

    async def readiness_check(self, is_running: bool, is_shutting_down: bool) -> ReadinessStatus:
        """Check if the manager is ready to accept work (Kubernetes-style probe)."""
        details: dict[str, Any] = {
            "running": is_running,
            "subscription_count": len(self._registry),
            "shutting_down": is_shutting_down,
        }
        if is_shutting_down:
            return ReadinessStatus(ready=False, reason="Manager is shutting down", details=details)
        if not is_running:
            return ReadinessStatus(ready=False, reason="Manager is not running", details=details)

        error_subs = [n for n, s in self._registry.items() if s.state == SubscriptionState.ERROR]
        starting_subs = [n for n, s in self._registry.items() if s.state == SubscriptionState.STARTING]  # fmt: skip
        if error_subs:
            details["error_subscriptions"] = error_subs
            return ReadinessStatus(
                ready=False,
                reason=f"Subscriptions in error state: {', '.join(error_subs)}",
                details=details,
            )
        if starting_subs:
            details["starting_subscriptions"] = starting_subs
            return ReadinessStatus(
                ready=False,
                reason=f"Subscriptions still starting: {', '.join(starting_subs)}",
                details=details,
            )
        return ReadinessStatus(ready=True, reason="Manager is ready", details=details)

    async def liveness_check(self, is_running: bool) -> LivenessStatus:
        """Check if the manager is alive and responsive (Kubernetes-style probe)."""
        details: dict[str, Any] = {"running": is_running, "uptime_seconds": self.uptime_seconds}
        try:
            details["lock_responsive"] = await asyncio.wait_for(
                self._try_acquire_lock(), timeout=5.0
            )
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
                alive=False, reason=f"Error accessing subscriptions: {e}", details=details
            )
        return LivenessStatus(alive=True, reason="Manager is alive and responsive", details=details)

    def get_subscription_health(self, subscription_name: str) -> SubscriptionHealth | None:
        """Get health status for a specific subscription, or None if not found."""
        sub = self._registry.get(subscription_name)
        return self._build_subscription_health(subscription_name, sub) if sub is not None else None


__all__ = ["HealthCheckProbesMixin"]
