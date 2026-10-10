"""
Health check core types, indicators, results, and configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


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

    # Circuit breaker
    circuit_open_is_unhealthy: bool = True
    """Treat open circuit breaker as unhealthy."""

    # DLQ thresholds
    max_dlq_events_warning: int = 10
    """Warn if DLQ has this many events."""

    max_dlq_events_critical: int = 100
    """Critical if DLQ has this many events."""


__all__ = [
    "HealthCheckConfig",
    "HealthCheckResult",
    "HealthIndicator",
    "HealthStatus",
]
