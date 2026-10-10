"""
Dispatch, error handling, and health inspection mixin for SubscriptionManager.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

from __future__ import annotations

import logging
from typing import Any

from eventsource.application.subscriptions.error_handling import (
    ErrorCallback,
    ErrorCategory,
    ErrorSeverity,
    SubscriptionErrorHandler,
)
from eventsource.application.subscriptions.health import (
    HealthCheckResult,
    LivenessStatus,
    ManagerHealth,
    ReadinessStatus,
    SubscriptionHealth,
)
from eventsource.application.subscriptions.health_provider import HealthCheckProvider
from eventsource.application.subscriptions.registry import SubscriptionRegistry

logger = logging.getLogger(__name__)


class ManagerDispatchMixin:
    """
    Mixin providing error routing, error statistics, and health check dispatch.
    """

    _registry: SubscriptionRegistry
    _error_handlers: dict[str, SubscriptionErrorHandler]
    _global_error_callbacks: list[ErrorCallback]
    _health_provider: HealthCheckProvider
    _running: bool

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown has been requested."""
        raise NotImplementedError

    def on_error(self, callback: ErrorCallback) -> None:
        """Register a callback for all error notifications."""
        self._global_error_callbacks.append(callback)
        for handler in self._error_handlers.values():
            handler.on_error(callback)

    def on_error_category(
        self,
        category: ErrorCategory,
        callback: ErrorCallback,
    ) -> None:
        """Register a callback for errors of a specific category."""
        for handler in self._error_handlers.values():
            handler.on_category(category, callback)

    def on_error_severity(
        self,
        severity: ErrorSeverity,
        callback: ErrorCallback,
    ) -> None:
        """Register a callback for errors of a specific severity."""
        for handler in self._error_handlers.values():
            handler.on_severity(severity, callback)

    def get_error_handler(self, subscription_name: str) -> SubscriptionErrorHandler | None:
        """Get the error handler for a specific subscription."""
        return self._error_handlers.get(subscription_name)

    def get_error_stats(self) -> dict[str, Any]:
        """Get error statistics for all subscriptions."""
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
        return stats

    def get_health(self) -> dict[str, Any]:
        """Get health status of all subscriptions."""
        subscription_statuses = []
        has_errors = False
        all_live = True

        for subscription in self._registry.get_all():
            status = subscription.get_status()
            subscription_statuses.append(status.to_dict())

            if status.state == "error":
                has_errors = True
                all_live = False
            elif status.state != "live":
                all_live = False

        if has_errors:
            overall_status = "unhealthy"
        elif all_live and self._running:
            overall_status = "healthy"
        elif self._running:
            overall_status = "starting"
        else:
            overall_status = "stopped"

        return {
            "status": overall_status,
            "running": self._running,
            "subscription_count": len(self._registry),
            "subscriptions": subscription_statuses,
        }

    def get_health_checker(self, subscription_name: str) -> Any:
        """Get the health checker for a specific subscription."""
        return self._health_provider.get_health_checker(subscription_name)

    def check_health(self, subscription_name: str) -> HealthCheckResult:
        """Check health of a specific subscription."""
        return self._health_provider.check_health(subscription_name)

    def check_all_health(self) -> dict[str, Any]:
        """Check health of all subscriptions."""
        return self._health_provider.check_all_health(self._running)

    def get_comprehensive_health(self) -> dict[str, Any]:
        """Get comprehensive health report including all resilience metrics."""
        return self._health_provider.get_comprehensive_health(self._running)

    @property
    def total_errors(self) -> int:
        """Get total error count across all subscriptions."""
        return self._health_provider.total_errors

    @property
    def total_dlq_count(self) -> int:
        """Get total DLQ count across all subscriptions."""
        return self._health_provider.total_dlq_count

    @property
    def is_healthy(self) -> bool:
        """Quick health check."""
        return self._health_provider.is_healthy(self._running)

    @property
    def uptime_seconds(self) -> float:
        """Get manager uptime in seconds."""
        return self._health_provider.uptime_seconds

    async def health_check(self) -> ManagerHealth:
        """Get comprehensive health status of the subscription manager."""
        return await self._health_provider.health_check(self._running)

    async def readiness_check(self) -> ReadinessStatus:
        """Check if the manager is ready to accept work."""
        return await self._health_provider.readiness_check(
            self._running,
            self.is_shutting_down,
        )

    async def liveness_check(self) -> LivenessStatus:
        """Check if the manager is alive and responsive."""
        return await self._health_provider.liveness_check(self._running)

    def get_subscription_health(self, subscription_name: str) -> SubscriptionHealth | None:
        """Get health status for a specific subscription."""
        return self._health_provider.get_subscription_health(subscription_name)
