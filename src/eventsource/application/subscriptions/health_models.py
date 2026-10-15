"""
Health models and probe statuses for subscription manager.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class SubscriptionHealth:
    """Health status of a single subscription."""

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
    """Comprehensive health status of the subscription manager (JSON-serializable)."""

    status: str
    running: bool
    subscription_count: int
    healthy_count: int
    degraded_count: int
    unhealthy_count: int
    total_events_processed: int
    total_events_failed: int
    total_lag_events: int
    uptime_seconds: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    subscriptions: list[SubscriptionHealth] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
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
    """Readiness probe status (Kubernetes-style) indicating readiness to accept work."""

    ready: bool
    reason: str
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

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
    """Liveness probe status (Kubernetes-style) indicating the manager is alive and responsive."""

    alive: bool
    reason: str
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

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
