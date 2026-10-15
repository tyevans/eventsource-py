"""Types, state enums, no-op instruments, and meter helpers for subscription metrics."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

try:
    from opentelemetry import metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    metrics = None  # type: ignore[assignment]


class StateValue(IntEnum):
    """Numeric values for subscription states as gauge values."""

    UNKNOWN = 0
    STARTING = 1
    CATCHING_UP = 2
    LIVE = 3
    PAUSED = 4
    STOPPED = 5
    ERROR = 6


STATE_MAPPING: dict[str, int] = {
    "starting": StateValue.STARTING,
    "catching_up": StateValue.CATCHING_UP,
    "live": StateValue.LIVE,
    "paused": StateValue.PAUSED,
    "stopped": StateValue.STOPPED,
    "error": StateValue.ERROR,
}

_meter: Any = None


def _get_meter() -> Any:
    """Get or create the OpenTelemetry meter instance, or None if unavailable."""
    global _meter
    if _meter is None and OTEL_METRICS_AVAILABLE and metrics is not None:
        _meter = metrics.get_meter("eventsource.application.subscriptions", version="1.0.0")
    return _meter


def reset_meter() -> None:
    """Reset the global meter instance for testing."""
    global _meter
    _meter = None


class NoOpCounter:
    """No-op counter when OpenTelemetry is not available."""

    def add(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        """No-op add operation."""


class NoOpHistogram:
    """No-op histogram when OpenTelemetry is not available."""

    def record(self, value: float, attributes: dict[str, Any] | None = None) -> None:
        """No-op record operation."""


class NoOpGauge:
    """No-op gauge when OpenTelemetry is not available."""

    def __init__(self) -> None:
        """Initialize no-op gauge."""


@dataclass
class MetricSnapshot:
    """Snapshot of current metric values for a subscription."""

    events_processed: int = 0
    events_failed: int = 0
    total_processing_time_ms: float = 0.0
    current_lag: int = 0
    current_state: int = StateValue.UNKNOWN
    current_state_name: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "total_processing_time_ms": self.total_processing_time_ms,
            "current_lag": self.current_lag,
            "current_state": self.current_state,
            "current_state_name": self.current_state_name,
        }


class _Timer:
    """Internal timer for measuring processing duration."""

    def __init__(self) -> None:
        """Initialize timer."""
        self._start: float = 0.0
        self._end: float = 0.0
        self._stopped: bool = False

    def start(self) -> None:
        """Start the timer."""
        self._start = time.perf_counter()
        self._stopped = False

    def stop(self) -> None:
        """Stop the timer."""
        if not self._stopped:
            self._end = time.perf_counter()
            self._stopped = True

    @property
    def duration_ms(self) -> float:
        """Get duration in milliseconds, or 0 if not started."""
        if self._start == 0:
            return 0.0
        end = self._end if self._stopped else time.perf_counter()
        return (end - self._start) * 1000


__all__ = [
    "OTEL_METRICS_AVAILABLE",
    "STATE_MAPPING",
    "MetricSnapshot",
    "NoOpCounter",
    "NoOpGauge",
    "NoOpHistogram",
    "StateValue",
    "_Timer",
    "_get_meter",
    "_meter",
    "metrics",
    "reset_meter",
]
