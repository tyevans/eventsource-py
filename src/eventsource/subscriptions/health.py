"""
Health check module for subscription management.

Provides comprehensive health checks that incorporate:
- Subscription state and statistics
- Error rates and patterns
- Backpressure status
- Circuit breaker state
- Retry statistics
- DLQ backlog
- Lag metrics

This module integrates with all Phase 2 resilience features to provide
a unified view of subscription health.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.subscriptions.error_handling import SubscriptionErrorHandler
    from eventsource.subscriptions.flow_control import FlowController
    from eventsource.subscriptions.retry import CircuitBreaker, RetryableOperation
    from eventsource.subscriptions.subscription import Subscription

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """
    Health status levels.

    Indicates the overall health state of a subscription or the manager.
    """

    HEALTHY = "healthy"
    """All systems operating normally."""

    DEGRADED = "degraded"
    """Some issues present but still operational."""

    UNHEALTHY = "unhealthy"
    """Significant issues requiring attention."""

    CRITICAL = "critical"
    """Critical issues requiring immediate intervention."""

    UNKNOWN = "unknown"
    """Health status cannot be determined."""


@dataclass
class HealthIndicator:
    """
    Individual health indicator result.

    Represents the health status of a single component or metric.
    """

    name: str
    status: HealthStatus
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class HealthCheckResult:
    """
    Comprehensive health check result.

    Aggregates multiple health indicators into an overall status.
    """

    overall_status: HealthStatus
    indicators: list[HealthIndicator] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    subscription_name: str = ""
    uptime_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.overall_status.value,
            "subscription_name": self.subscription_name,
            "timestamp": self.timestamp.isoformat(),
            "uptime_seconds": self.uptime_seconds,
            "indicators": [i.to_dict() for i in self.indicators],
        }


@dataclass
class HealthCheckConfig:
    """
    Configuration for health check thresholds.

    Defines thresholds for various metrics to determine health status.
    """

    # Error thresholds
    max_error_rate_per_minute: float = 10.0
    """Error rate above this is unhealthy."""

    max_errors_warning: int = 10
    """Warn if total errors exceed this."""

    max_errors_critical: int = 100
    """Critical if total errors exceed this."""

    # Lag thresholds
    max_lag_events_warning: int = 1000
    """Warn if lag exceeds this many events."""

    max_lag_events_critical: int = 10000
    """Critical if lag exceeds this many events."""

    # Backpressure thresholds
    backpressure_warning_duration_seconds: float = 60.0
    """Warn if backpressured for this long."""

    backpressure_critical_duration_seconds: float = 300.0
    """Critical if backpressured for this long."""

    # Circuit breaker
    circuit_open_is_unhealthy: bool = True
    """Treat open circuit breaker as unhealthy."""

    # DLQ thresholds
    max_dlq_events_warning: int = 10
    """Warn if DLQ has this many events."""

    max_dlq_events_critical: int = 100
    """Critical if DLQ has this many events."""


class SubscriptionHealthChecker:
    """
    Health checker for individual subscriptions.

    Evaluates subscription health based on multiple indicators:
    - State (running, stopped, error)
    - Error statistics
    - Backpressure status
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
        subscription: "Subscription",
        config: HealthCheckConfig | None = None,
        error_handler: "SubscriptionErrorHandler | None" = None,
        flow_controller: "FlowController | None" = None,
        circuit_breaker: "CircuitBreaker | None" = None,
        retry_operation: "RetryableOperation | None" = None,
    ) -> None:
        """
        Initialize the health checker.

        Args:
            subscription: The subscription to check
            config: Health check thresholds
            error_handler: Error handler for error statistics
            flow_controller: Flow controller for backpressure status
            circuit_breaker: Circuit breaker for circuit state
            retry_operation: Retry operation for retry statistics
        """
        self.subscription = subscription
        self.config = config or HealthCheckConfig()
        self._error_handler = error_handler
        self._flow_controller = flow_controller
        self._circuit_breaker = circuit_breaker
        self._retry_operation = retry_operation

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

        # Check backpressure
        if self._flow_controller:
            indicators.append(self._check_backpressure())

        # Check circuit breaker
        if self._circuit_breaker:
            indicators.append(self._check_circuit_breaker())

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

    def _check_state(self) -> HealthIndicator:
        """Check subscription state indicator."""
        from eventsource.subscriptions.subscription import SubscriptionState

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

        # Use error handler stats if available
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

    def _check_backpressure(self) -> HealthIndicator:
        """Check backpressure status indicator."""
        if self._flow_controller is None:
            return HealthIndicator(
                name="backpressure",
                status=HealthStatus.UNKNOWN,
                message="Flow controller not available",
            )

        stats = self._flow_controller.stats
        is_backpressured = self._flow_controller.is_backpressured

        if is_backpressured:
            return HealthIndicator(
                name="backpressure",
                status=HealthStatus.DEGRADED,
                message="Subscription experiencing backpressure",
                details={
                    "is_backpressured": True,
                    "events_in_flight": stats.events_in_flight,
                    "peak_in_flight": stats.peak_in_flight,
                    "pause_count": stats.pause_count,
                },
            )
        else:
            return HealthIndicator(
                name="backpressure",
                status=HealthStatus.HEALTHY,
                message="No backpressure",
                details={
                    "is_backpressured": False,
                    "events_in_flight": stats.events_in_flight,
                    "peak_in_flight": stats.peak_in_flight,
                },
            )

    def _check_circuit_breaker(self) -> HealthIndicator:
        """Check circuit breaker state indicator."""
        if self._circuit_breaker is None:
            return HealthIndicator(
                name="circuit_breaker",
                status=HealthStatus.UNKNOWN,
                message="Circuit breaker not available",
            )

        from eventsource.subscriptions.retry import CircuitState

        state = self._circuit_breaker.state

        if state == CircuitState.OPEN:
            status = (
                HealthStatus.UNHEALTHY
                if self.config.circuit_open_is_unhealthy
                else HealthStatus.DEGRADED
            )
            return HealthIndicator(
                name="circuit_breaker",
                status=status,
                message="Circuit breaker is OPEN - requests blocked",
                details={
                    "state": state.value,
                    "failure_count": self._circuit_breaker.failure_count,
                },
            )
        elif state == CircuitState.HALF_OPEN:
            return HealthIndicator(
                name="circuit_breaker",
                status=HealthStatus.DEGRADED,
                message="Circuit breaker is HALF_OPEN - testing recovery",
                details={
                    "state": state.value,
                    "failure_count": self._circuit_breaker.failure_count,
                },
            )
        else:  # CLOSED
            return HealthIndicator(
                name="circuit_breaker",
                status=HealthStatus.HEALTHY,
                message="Circuit breaker is CLOSED - normal operation",
                details={
                    "state": state.value,
                    "failure_count": self._circuit_breaker.failure_count,
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
        """
        Determine overall health status from indicators.

        Uses the worst status among all indicators.
        """
        # Priority order: CRITICAL > UNHEALTHY > DEGRADED > UNKNOWN > HEALTHY
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


class ManagerHealthChecker:
    """
    Health checker for the subscription manager.

    Aggregates health from all managed subscriptions and provides
    an overall manager health status.

    Example:
        >>> checker = ManagerHealthChecker(subscriptions, config)
        >>> result = checker.check()
        >>> print(f"Manager health: {result.overall_status.value}")
    """

    def __init__(
        self,
        subscriptions: list["Subscription"],
        config: HealthCheckConfig | None = None,
        subscription_checkers: dict[str, SubscriptionHealthChecker] | None = None,
    ) -> None:
        """
        Initialize the manager health checker.

        Args:
            subscriptions: List of managed subscriptions
            config: Health check thresholds
            subscription_checkers: Optional pre-configured checkers per subscription
        """
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

            # Use pre-configured checker or create basic one
            if name in self._subscription_checkers:
                checker = self._subscription_checkers[name]
            else:
                checker = SubscriptionHealthChecker(
                    subscription=subscription,
                    config=self.config,
                )

            result = checker.check()
            subscription_results[name] = result

            # Add subscription status as an indicator for overall health
            overall_indicators.append(
                HealthIndicator(
                    name=f"subscription.{name}",
                    status=result.overall_status,
                    message=f"Subscription {name} is {result.overall_status.value}",
                    details={"subscription_name": name},
                )
            )

        # Determine overall manager status
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

        # Count statuses
        status_counts: dict[HealthStatus, int] = {}
        for indicator in indicators:
            status_counts[indicator.status] = status_counts.get(indicator.status, 0) + 1

        # If any critical, overall is critical
        if status_counts.get(HealthStatus.CRITICAL, 0) > 0:
            return HealthStatus.CRITICAL

        # If majority unhealthy, overall is unhealthy
        total = len(indicators)
        unhealthy = status_counts.get(HealthStatus.UNHEALTHY, 0)
        if unhealthy > total // 2:
            return HealthStatus.UNHEALTHY

        # If any unhealthy, overall is degraded
        if unhealthy > 0:
            return HealthStatus.DEGRADED

        # If any degraded, overall is degraded
        if status_counts.get(HealthStatus.DEGRADED, 0) > 0:
            return HealthStatus.DEGRADED

        # If all unknown, overall is unknown
        if status_counts.get(HealthStatus.UNKNOWN, 0) == total:
            return HealthStatus.UNKNOWN

        return HealthStatus.HEALTHY


# =============================================================================
# Manager Health API
# =============================================================================


@dataclass
class ManagerHealth:
    """
    Health status of the subscription manager.

    Provides comprehensive health information including:
    - Overall status (healthy/degraded/unhealthy)
    - Running state
    - Subscription counts by health status
    - Total lag across all subscriptions
    - Uptime information
    - Per-subscription status details

    This is designed to be JSON-serializable for health endpoints.

    Example:
        >>> health = await manager.health_check()
        >>> if health.status == "unhealthy":
        ...     await alert_ops_team(health.to_dict())
    """

    status: str
    """Overall health status: "healthy", "degraded", "unhealthy", or "critical"."""

    running: bool
    """Whether the manager is currently running."""

    subscription_count: int
    """Total number of registered subscriptions."""

    healthy_count: int
    """Number of subscriptions in healthy state."""

    degraded_count: int
    """Number of subscriptions in degraded state."""

    unhealthy_count: int
    """Number of subscriptions in unhealthy or critical state."""

    total_events_processed: int
    """Total events processed across all subscriptions."""

    total_events_failed: int
    """Total events failed across all subscriptions."""

    total_lag_events: int
    """Total lag (events behind) across all subscriptions."""

    uptime_seconds: float
    """Time since manager started in seconds."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    """Timestamp of the health check."""

    subscriptions: list["SubscriptionHealth"] = field(default_factory=list)
    """Per-subscription health details."""

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary suitable for JSON encoding and health endpoints.
        """
        return {
            "status": self.status,
            "running": self.running,
            "subscription_count": self.subscription_count,
            "healthy_count": self.healthy_count,
            "degraded_count": self.degraded_count,
            "unhealthy_count": self.unhealthy_count,
            "total_events_processed": self.total_events_processed,
            "total_events_failed": self.total_events_failed,
            "total_lag_events": self.total_lag_events,
            "uptime_seconds": self.uptime_seconds,
            "timestamp": self.timestamp.isoformat(),
            "subscriptions": [s.to_dict() for s in self.subscriptions],
        }


@dataclass
class SubscriptionHealth:
    """
    Health status of a single subscription.

    Provides detailed health information for individual subscriptions.

    Attributes:
        name: Subscription name
        status: Health status (healthy/degraded/unhealthy/critical)
        state: Current subscription state
        events_processed: Total events successfully processed
        events_failed: Total events that failed processing
        lag_events: Number of events behind
        error_rate: Error rate per minute
        last_error: Most recent error message if any
        uptime_seconds: Time since subscription started
    """

    name: str
    status: str
    state: str
    events_processed: int
    events_failed: int
    lag_events: int
    error_rate: float
    last_error: str | None
    uptime_seconds: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "status": self.status,
            "state": self.state,
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "lag_events": self.lag_events,
            "error_rate": self.error_rate,
            "last_error": self.last_error,
            "uptime_seconds": self.uptime_seconds,
        }


@dataclass
class ReadinessStatus:
    """
    Readiness probe status (Kubernetes-style).

    Indicates whether the manager is ready to accept work.

    A manager is ready when:
    - It is running
    - It has at least one subscription
    - No subscriptions are in error state

    Example:
        >>> readiness = await manager.readiness_check()
        >>> if readiness.ready:
        ...     # Accept incoming work
        ...     pass
    """

    ready: bool
    """Whether the manager is ready to accept work."""

    reason: str
    """Human-readable explanation of readiness state."""

    details: dict[str, Any] = field(default_factory=dict)
    """Additional details about readiness."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    """Timestamp of the readiness check."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "ready": self.ready,
            "reason": self.reason,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class LivenessStatus:
    """
    Liveness probe status (Kubernetes-style).

    Indicates whether the manager is still alive and responding.

    A manager is live when:
    - It is not shutting down
    - Its internal systems are responsive

    Example:
        >>> liveness = await manager.liveness_check()
        >>> if not liveness.alive:
        ...     # Manager needs restart
        ...     pass
    """

    alive: bool
    """Whether the manager is alive and responsive."""

    reason: str
    """Human-readable explanation of liveness state."""

    details: dict[str, Any] = field(default_factory=dict)
    """Additional details about liveness."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    """Timestamp of the liveness check."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "alive": self.alive,
            "reason": self.reason,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Status
    "HealthStatus",
    # Indicators
    "HealthIndicator",
    "HealthCheckResult",
    # Config
    "HealthCheckConfig",
    # Checkers
    "SubscriptionHealthChecker",
    "ManagerHealthChecker",
    # Manager Health API
    "ManagerHealth",
    "SubscriptionHealth",
    "ReadinessStatus",
    "LivenessStatus",
]
