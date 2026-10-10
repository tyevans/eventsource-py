"""
Shutdown metrics instruments and snapshot models.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Optional OpenTelemetry import - graceful degradation when not available
try:
    from opentelemetry import metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    metrics = None  # type: ignore[assignment]


# Module-level meter and instruments - lazy initialization
_meter: Any = None
_shutdown_initiated_counter: Any = None
_shutdown_completed_counter: Any = None
_shutdown_duration_histogram: Any = None
_drain_duration_histogram: Any = None
_events_drained_counter: Any = None
_in_flight_gauge_value: int = 0


def _get_meter() -> Any:
    """
    Get or create the meter instance for shutdown metrics.

    Returns the OpenTelemetry meter for the subscriptions.shutdown namespace,
    or None if OpenTelemetry is not available.

    Returns:
        OpenTelemetry Meter or None
    """
    global _meter
    if _meter is None and OTEL_METRICS_AVAILABLE and metrics is not None:
        _meter = metrics.get_meter(
            "eventsource.application.subscriptions.shutdown", version="1.0.0"
        )
    return _meter


def _init_shutdown_metrics() -> None:
    """
    Initialize shutdown metrics instruments if not already done.

    This creates all the OpenTelemetry metric instruments for shutdown tracking.
    Safe to call multiple times - instruments are only created once.
    """
    global _shutdown_initiated_counter, _shutdown_completed_counter
    global _shutdown_duration_histogram, _drain_duration_histogram
    global _events_drained_counter

    meter = _get_meter()
    if meter is None:
        return

    if _shutdown_initiated_counter is None:
        _shutdown_initiated_counter = meter.create_counter(
            name="eventsource.shutdown.initiated_total",
            unit="1",
            description="Total number of shutdown operations initiated",
        )

    if _shutdown_completed_counter is None:
        _shutdown_completed_counter = meter.create_counter(
            name="eventsource.shutdown.completed_total",
            unit="1",
            description="Total number of shutdown operations completed",
        )

    if _shutdown_duration_histogram is None:
        _shutdown_duration_histogram = meter.create_histogram(
            name="eventsource.shutdown.duration_seconds",
            unit="s",
            description="Duration of shutdown operations in seconds",
        )

    if _drain_duration_histogram is None:
        _drain_duration_histogram = meter.create_histogram(
            name="eventsource.shutdown.drain_duration_seconds",
            unit="s",
            description="Duration of drain phase in seconds",
        )

    if _events_drained_counter is None:
        _events_drained_counter = meter.create_counter(
            name="eventsource.shutdown.events_drained_total",
            unit="events",
            description="Total number of events drained during shutdown",
        )


def record_shutdown_initiated() -> None:
    """
    Record that a shutdown operation has been initiated.

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()
    if _shutdown_initiated_counter is not None:
        _shutdown_initiated_counter.add(1)


def record_shutdown_completed(outcome: str, duration_seconds: float) -> None:
    """
    Record a completed shutdown operation.

    Args:
        outcome: The outcome of shutdown - "clean", "forced", or "timeout"
        duration_seconds: Total duration of the shutdown in seconds

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()

    if _shutdown_completed_counter is not None:
        _shutdown_completed_counter.add(1, {"outcome": outcome})

    if _shutdown_duration_histogram is not None:
        _shutdown_duration_histogram.record(duration_seconds, {"outcome": outcome})


def record_drain_duration(duration_seconds: float) -> None:
    """
    Record the duration of the drain phase.

    Args:
        duration_seconds: Duration of drain phase in seconds

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()
    if _drain_duration_histogram is not None:
        _drain_duration_histogram.record(duration_seconds)


def record_events_drained(count: int) -> None:
    """
    Record the number of events drained during shutdown.

    Args:
        count: Number of events that were drained

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()
    if _events_drained_counter is not None and count > 0:
        _events_drained_counter.add(count)


def record_in_flight_at_shutdown(count: int) -> None:
    """
    Record the number of in-flight events when shutdown started.

    This updates the gauge value that can be observed.

    Args:
        count: Number of events in flight at shutdown start

    Safe to call even when OpenTelemetry is not configured.
    """
    global _in_flight_gauge_value
    _in_flight_gauge_value = count


def get_in_flight_at_shutdown() -> int:
    """
    Get the recorded number of in-flight events at shutdown.

    Returns:
        Number of in-flight events recorded at shutdown start
    """
    return _in_flight_gauge_value


def reset_shutdown_metrics() -> None:
    """
    Reset the shutdown metrics state.

    Useful for testing to ensure clean state between tests.
    Resets the meter and all instrument references.
    """
    global _meter, _shutdown_initiated_counter, _shutdown_completed_counter
    global _shutdown_duration_histogram, _drain_duration_histogram
    global _events_drained_counter, _in_flight_gauge_value

    _meter = None
    _shutdown_initiated_counter = None
    _shutdown_completed_counter = None
    _shutdown_duration_histogram = None
    _drain_duration_histogram = None
    _events_drained_counter = None
    _in_flight_gauge_value = 0


@dataclass(frozen=True)
class ShutdownMetricsSnapshot:
    """
    Snapshot of shutdown metrics for a single shutdown operation.

    Captures key metrics about the shutdown process that can be
    inspected after shutdown completes.

    Attributes:
        shutdown_duration_seconds: Total duration of shutdown
        drain_duration_seconds: Duration of drain phase (0 if no drain)
        events_drained: Number of events drained during shutdown
        checkpoints_saved: Number of checkpoints saved during shutdown
        in_flight_at_start: Number of in-flight events at shutdown start
        outcome: Shutdown outcome - "clean", "forced", or "timeout"
    """

    shutdown_duration_seconds: float
    drain_duration_seconds: float
    events_drained: int
    checkpoints_saved: int
    in_flight_at_start: int
    outcome: str

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation of metrics snapshot
        """
        return {
            "shutdown_duration_seconds": self.shutdown_duration_seconds,
            "drain_duration_seconds": self.drain_duration_seconds,
            "events_drained": self.events_drained,
            "checkpoints_saved": self.checkpoints_saved,
            "in_flight_at_start": self.in_flight_at_start,
            "outcome": self.outcome,
        }


__all__ = [
    "ShutdownMetricsSnapshot",
    "_get_meter",
    "_init_shutdown_metrics",
    "get_in_flight_at_shutdown",
    "record_drain_duration",
    "record_events_drained",
    "record_in_flight_at_shutdown",
    "record_shutdown_completed",
    "record_shutdown_initiated",
    "reset_shutdown_metrics",
]
