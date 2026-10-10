"""
Metric instruments, snapshots, timers, and OTel detection types for migration.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

# Optional OpenTelemetry import - single source of truth
try:
    from opentelemetry import metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    metrics = None  # type: ignore[assignment]


# Module-level meter instance
_meter: Any = None


def _get_meter() -> Any:
    """
    Get or create the meter instance.

    Returns the OpenTelemetry meter for the migration namespace,
    or None if OpenTelemetry is not available.

    Returns:
        OpenTelemetry Meter or None
    """
    global _meter
    if _meter is None and OTEL_METRICS_AVAILABLE and metrics is not None:
        _meter = metrics.get_meter("eventsource.migration", version="1.0.0")
    return _meter


def reset_meter() -> None:
    """
    Reset the global meter instance.

    Useful for testing to ensure fresh meter state between tests.
    """
    global _meter
    _meter = None


class NoOpCounter:
    """
    No-op counter when OpenTelemetry is not available.

    Provides the same interface as an OpenTelemetry Counter
    but does nothing, allowing code to work without OTel.
    """

    def add(
        self,
        amount: int | float,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """No-op add operation."""
        pass


class NoOpHistogram:
    """
    No-op histogram when OpenTelemetry is not available.

    Provides the same interface as an OpenTelemetry Histogram
    but does nothing, allowing code to work without OTel.
    """

    def record(
        self,
        value: float,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """No-op record operation."""
        pass


class NoOpGauge:
    """
    No-op gauge when OpenTelemetry is not available.

    For observable gauges, we store the callback but don't invoke it.
    """

    def __init__(self) -> None:
        """Initialize no-op gauge."""
        pass


@dataclass(frozen=True)
class MigrationMetricSnapshot:
    """
    Snapshot of current metric values for a migration.

    Useful for testing and debugging to see what values
    would be reported to OpenTelemetry.

    Attributes:
        events_copied: Total events copied during bulk copy
        events_copied_rate: Current rate of events being copied (events/sec)
        sync_lag_events: Current sync lag in events
        failed_target_writes: Count of failed writes to target
        verification_failures: Count of consistency verification failures
        phase_durations: Dictionary of phase name to total duration
        cutover_durations: List of cutover duration recordings
    """

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
    """
    Internal timer for measuring phase duration.

    Used by the time_phase context manager.
    """

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
    def duration_seconds(self) -> float:
        """
        Get duration in seconds.

        Returns:
            Duration in seconds, or 0 if not started
        """
        if self._start == 0:
            return 0.0
        end = self._end if self._stopped else time.perf_counter()
        return end - self._start


class _CutoverTimer:
    """
    Internal timer for measuring cutover duration.

    Used by the time_cutover context manager.
    """

    def __init__(self) -> None:
        """Initialize timer."""
        self._start: float = 0.0
        self._end: float = 0.0
        self._stopped: bool = False
        self.success: bool = True

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
        """
        Get duration in milliseconds.

        Returns:
            Duration in milliseconds, or 0 if not started
        """
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
