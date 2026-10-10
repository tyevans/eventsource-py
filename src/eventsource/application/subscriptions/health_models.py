"""
Health models and probe statuses for subscription manager.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


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

    subscriptions: list[SubscriptionHealth] = field(default_factory=list)
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


__all__ = [
    "LivenessStatus",
    "ManagerHealth",
    "ReadinessStatus",
    "SubscriptionHealth",
]
