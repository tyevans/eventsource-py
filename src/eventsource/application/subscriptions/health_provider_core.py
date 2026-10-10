"""Core health check provider class for subscription health monitoring."""

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
from eventsource.application.subscriptions.health import (
    HealthCheckConfig,
    SubscriptionHealthChecker,
)
from eventsource.application.subscriptions.health_provider_checks import (
    HealthCheckAggregationMixin,
)
from eventsource.application.subscriptions.health_provider_probes import (
    HealthCheckProbesMixin,
)
from eventsource.application.subscriptions.subscription import Subscription

if TYPE_CHECKING:
    from eventsource.application.subscriptions.registry import SubscriptionRegistry
    from eventsource.application.subscriptions.retry import CircuitBreaker

logger = logging.getLogger(__name__)


class HealthCheckProvider(HealthCheckProbesMixin, HealthCheckAggregationMixin):
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
        handler_circuit_breaker_lookup: "Callable[[str], CircuitBreaker | None] | None" = None,
        infra_circuit_breaker_lookup: "Callable[[str], CircuitBreaker | None] | None" = None,
    ) -> None:
        """Initialize the health check provider."""
        self._registry = registry
        self._error_handlers = error_handlers
        self._config = config or HealthCheckConfig()
        self._lock = lock or asyncio.Lock()
        self._handler_circuit_breaker_lookup = handler_circuit_breaker_lookup
        self._infra_circuit_breaker_lookup = infra_circuit_breaker_lookup
        self._health_checkers: dict[str, SubscriptionHealthChecker] = {}
        self._started_at: datetime | None = None

    def register_subscription(
        self,
        subscription: Subscription,
        error_handler: SubscriptionErrorHandler,
    ) -> SubscriptionHealthChecker:
        """Register a subscription for health monitoring."""
        name = subscription.name

        handler_circuit_breaker_provider = None
        if self._handler_circuit_breaker_lookup is not None:
            handler_lookup = self._handler_circuit_breaker_lookup

            def handler_circuit_breaker_provider() -> "CircuitBreaker | None":
                return handler_lookup(name)

        infra_circuit_breaker_provider = None
        if self._infra_circuit_breaker_lookup is not None:
            infra_lookup = self._infra_circuit_breaker_lookup

            def infra_circuit_breaker_provider() -> "CircuitBreaker | None":
                return infra_lookup(name)

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            config=self._config,
            error_handler=error_handler,
            handler_circuit_breaker_provider=handler_circuit_breaker_provider,
            infra_circuit_breaker_provider=infra_circuit_breaker_provider,
        )
        self._health_checkers[subscription.name] = checker
        return checker

    def unregister_subscription(self, name: str) -> None:
        """Unregister a subscription from health monitoring."""
        self._health_checkers.pop(name, None)

    def set_started(self, started_at: datetime | None = None) -> None:
        """Mark the manager as started."""
        self._started_at = started_at or datetime.now(UTC)

    def clear_started(self) -> None:
        """Clear the started timestamp."""
        self._started_at = None

    @property
    def uptime_seconds(self) -> float:
        """Get manager uptime in seconds."""
        if self._started_at is None:
            return 0.0
        return (datetime.now(UTC) - self._started_at).total_seconds()

    def get_health_checker(
        self,
        subscription_name: str,
    ) -> SubscriptionHealthChecker | None:
        """Get the health checker for a specific subscription."""
        return self._health_checkers.get(subscription_name)

    @property
    def total_errors(self) -> int:
        """Get total error count across all subscriptions."""
        return sum(handler.total_errors for handler in self._error_handlers.values())

    @property
    def total_dlq_count(self) -> int:
        """Get total DLQ count across all subscriptions."""
        return sum(handler.dlq_count for handler in self._error_handlers.values())


__all__ = ["HealthCheckProvider"]
