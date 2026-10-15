"""
Metric instruments, snapshots, timers, and OTel detection types for migration.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

metrics: Any = None
try:
    from opentelemetry import metrics as _otel_metrics

    metrics = _otel_metrics
    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False


# Module-level meter instance
_meter: Any = None


def _get_meter() -> Any:
    """Return the OpenTelemetry meter for the migration namespace, or None if unavailable."""
    global _meter
    if _meter is None and OTEL_METRICS_AVAILABLE and metrics is not None:
        _meter = metrics.get_meter("eventsource.migration", version="1.0.0")
    return _meter


def reset_meter() -> None:
    """Reset the global meter instance for testing."""
    global _meter
    _meter = None


class NoOpCounter:
    """No-op counter when OpenTelemetry is not available."""

    def add(
        self,
        amount: int | float,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """No-op add operation."""


class NoOpHistogram:
    """No-op histogram when OpenTelemetry is not available."""

    def record(
        self,
        value: float,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """No-op record operation."""


class NoOpGauge:
    """No-op gauge when OpenTelemetry is not available."""

    def __init__(self) -> None:
        """Initialize no-op gauge."""


@dataclass(frozen=True)
class MigrationMetricSnapshot:
    """Snapshot of current metric values for a migration."""

    events_copied: int = 0
    events_copied_rate: float = 0.0
    sync_lag_events: int = 0
    failed_target_writes: int = 0
    verification_failures: int = 0
    phase_durations: dict[str, float] = field(default_factory=dict)
    cutover_durations: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "events_copied": self.events_copied,
            "events_copied_rate": self.events_copied_rate,
            "sync_lag_events": self.sync_lag_events,
            "failed_target_writes": self.failed_target_writes,
            "verification_failures": self.verification_failures,
            "phase_durations": dict(self.phase_durations),
            "cutover_durations": list(self.cutover_durations),
        }


class _PhaseTimer:
    """Internal timer for measuring phase duration."""

    def __init__(self) -> None:
        self._start: float = 0.0
        self._end: float = 0.0
        self._stopped: bool = False

    def start(self) -> None:
        self._start = time.perf_counter()
        self._stopped = False

    def stop(self) -> None:
        if not self._stopped:
            self._end = time.perf_counter()
            self._stopped = True

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds, or 0 if not started."""
        if self._start == 0:
            return 0.0
        end = self._end if self._stopped else time.perf_counter()
        return end - self._start


class _CutoverTimer:
    """Internal timer for measuring cutover duration."""

    def __init__(self) -> None:
        self._start: float = 0.0
        self._end: float = 0.0
        self._stopped: bool = False
        self.success: bool = True

    def start(self) -> None:
        self._start = time.perf_counter()
        self._stopped = False

    def stop(self) -> None:
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
    "MigrationMetricSnapshot",
    "NoOpCounter",
    "NoOpGauge",
    "NoOpHistogram",
    "_CutoverTimer",
    "_PhaseTimer",
    "_get_meter",
    "reset_meter",
]
