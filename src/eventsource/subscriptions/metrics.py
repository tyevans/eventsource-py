"""
OpenTelemetry metrics for subscription management.

This module provides metrics instrumentation for the subscription system,
tracking events processed, failures, processing duration, and lag.

The metrics gracefully degrade when OpenTelemetry is not installed -
all operations become no-ops without raising errors.

Example:
    >>> from eventsource.subscriptions.metrics import SubscriptionMetrics
    >>>
    >>> metrics = SubscriptionMetrics("OrderProjection")
    >>> metrics.record_event_processed("OrderCreated", 15.5)
    >>> metrics.record_event_failed("OrderCreated", "ValidationError")
    >>> metrics.record_lag(100)
    >>> metrics.record_state("live")

Metrics Exposed:
    - subscription.events.processed (Counter): Total events processed
    - subscription.events.failed (Counter): Total events failed
    - subscription.processing.duration (Histogram): Processing time in milliseconds
    - subscription.lag (Gauge): Current event lag
    - subscription.state (Gauge): Current subscription state (numeric)

All metrics include the 'subscription' attribute for filtering by subscription name.
"""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

# Optional OpenTelemetry import - single source of truth
try:
    from opentelemetry import metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    metrics = None  # type: ignore[assignment]


# State values for numeric gauge
class StateValue(IntEnum):
    """Numeric values for subscription states as gauge values."""

    UNKNOWN = 0
    STARTING = 1
    CATCHING_UP = 2
    LIVE = 3
    PAUSED = 4
    STOPPED = 5
    ERROR = 6


# Mapping from string state to numeric value
STATE_MAPPING: dict[str, int] = {
    "starting": StateValue.STARTING,
    "catching_up": StateValue.CATCHING_UP,
    "live": StateValue.LIVE,
    "paused": StateValue.PAUSED,
    "stopped": StateValue.STOPPED,
    "error": StateValue.ERROR,
}


# Module-level meter instance
_meter: Any = None


def _get_meter() -> Any:
    """
    Get or create the meter instance.

    Returns the OpenTelemetry meter for the subscriptions namespace,
    or None if OpenTelemetry is not available.

    Returns:
        OpenTelemetry Meter or None
    """
    global _meter
    if _meter is None and OTEL_METRICS_AVAILABLE and metrics is not None:
        _meter = metrics.get_meter("eventsource.subscriptions", version="1.0.0")
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


@dataclass
class MetricSnapshot:
    """
    Snapshot of current metric values for a subscription.

    Useful for testing and debugging to see what values
    would be reported to OpenTelemetry.

    Attributes:
        events_processed: Total events processed
        events_failed: Total events failed
        total_processing_time_ms: Sum of all processing times
        current_lag: Current event lag
        current_state: Current state value
        current_state_name: Current state name
    """

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


@dataclass
class SubscriptionMetrics:
    """
    Container for subscription metrics instruments.

    Provides methods to record events processed, failures, processing
    duration, lag, and state changes. All methods are safe to call
    even when OpenTelemetry is not installed - they become no-ops.

    Attributes:
        subscription_name: Name of the subscription for metric labels
        enable_metrics: Whether metrics are enabled (default True)

    Example:
        >>> metrics = SubscriptionMetrics("OrderProjection")
        >>>
        >>> # Record a successful event
        >>> start = time.perf_counter()
        >>> process_event(event)
        >>> duration_ms = (time.perf_counter() - start) * 1000
        >>> metrics.record_event_processed("OrderCreated", duration_ms)
        >>>
        >>> # Record a failure
        >>> metrics.record_event_failed("OrderCreated", "ValidationError")
        >>>
        >>> # Use timing context manager
        >>> with metrics.time_processing() as timer:
        ...     process_event(event)
        >>> metrics.record_event_processed("OrderCreated", timer.duration_ms)
    """

    subscription_name: str
    enable_metrics: bool = True

    # Internal state
    _meter: Any = field(default=None, init=False, repr=False)
    _events_processed_counter: Any = field(default=None, init=False, repr=False)
    _events_failed_counter: Any = field(default=None, init=False, repr=False)
    _processing_duration_histogram: Any = field(default=None, init=False, repr=False)
    _lag_value: int = field(default=0, init=False, repr=False)
    _state_value: int = field(default=StateValue.UNKNOWN, init=False, repr=False)
    _state_name: str = field(default="unknown", init=False, repr=False)

    # Internal counters for snapshot
    _processed_count: int = field(default=0, init=False, repr=False)
    _failed_count: int = field(default=0, init=False, repr=False)
    _total_duration_ms: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize metric instruments."""
        if self.enable_metrics and OTEL_METRICS_AVAILABLE:
            self._setup_metrics()
        else:
            self._setup_noop()

    def _setup_metrics(self) -> None:
        """Set up OpenTelemetry metric instruments."""
        self._meter = _get_meter()

        if self._meter is None:
            self._setup_noop()
            return

        # Counter: events processed
        self._events_processed_counter = self._meter.create_counter(
            name="subscription.events.processed",
            unit="events",
            description="Total number of events processed by subscription",
        )

        # Counter: events failed
        self._events_failed_counter = self._meter.create_counter(
            name="subscription.events.failed",
            unit="events",
            description="Total number of events that failed processing",
        )

        # Histogram: processing duration
        self._processing_duration_histogram = self._meter.create_histogram(
            name="subscription.processing.duration",
            unit="ms",
            description="Event processing duration in milliseconds",
        )

        # Observable Gauge: lag
        # We register a callback that will be invoked during metric collection
        self._meter.create_observable_gauge(
            name="subscription.lag",
            callbacks=[self._observe_lag],
            unit="events",
            description="Current event lag (events behind)",
        )

        # Observable Gauge: state
        self._meter.create_observable_gauge(
            name="subscription.state",
            callbacks=[self._observe_state],
            unit="1",
            description="Current subscription state (numeric)",
        )

    def _setup_noop(self) -> None:
        """Set up no-op instruments when OTel not available."""
        self._events_processed_counter = NoOpCounter()
        self._events_failed_counter = NoOpCounter()
        self._processing_duration_histogram = NoOpHistogram()

    def _observe_lag(self, options: Any) -> Any:
        """
        Callback for observable lag gauge.

        Called by OpenTelemetry during metric collection.

        Args:
            options: OpenTelemetry callback options

        Yields:
            Observation with lag value and attributes
        """
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(
                value=self._lag_value,
                attributes={"subscription": self.subscription_name},
            )

    def _observe_state(self, options: Any) -> Any:
        """
        Callback for observable state gauge.

        Called by OpenTelemetry during metric collection.

        Args:
            options: OpenTelemetry callback options

        Yields:
            Observation with state value and attributes
        """
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(
                value=self._state_value,
                attributes={
                    "subscription": self.subscription_name,
                    "state_name": self._state_name,
                },
            )

    def record_event_processed(
        self,
        event_type: str,
        duration_ms: float,
        status: str = "success",
    ) -> None:
        """
        Record a successfully processed event.

        Args:
            event_type: Type of the event (e.g., "OrderCreated")
            duration_ms: Processing time in milliseconds
            status: Status of processing (default "success")
        """
        attrs = {
            "subscription": self.subscription_name,
            "event.type": event_type,
            "status": status,
        }
        self._events_processed_counter.add(1, attrs)
        self._processing_duration_histogram.record(duration_ms, attrs)

        # Update internal counters for snapshot
        self._processed_count += 1
        self._total_duration_ms += duration_ms

    def record_event_failed(
        self,
        event_type: str,
        error_type: str,
        duration_ms: float | None = None,
    ) -> None:
        """
        Record a failed event.

        Args:
            event_type: Type of the event (e.g., "OrderCreated")
            error_type: Type of error (e.g., "ValidationError")
            duration_ms: Optional processing time before failure
        """
        attrs = {
            "subscription": self.subscription_name,
            "event.type": event_type,
            "error.type": error_type,
        }
        self._events_failed_counter.add(1, attrs)

        # Record duration if provided (time until failure)
        if duration_ms is not None:
            failure_attrs = {
                "subscription": self.subscription_name,
                "event.type": event_type,
                "status": "failed",
            }
            self._processing_duration_histogram.record(duration_ms, failure_attrs)
            self._total_duration_ms += duration_ms

        # Update internal counter for snapshot
        self._failed_count += 1

    def record_lag(self, lag_events: int) -> None:
        """
        Update the current lag value.

        This updates the internal state that will be reported
        by the observable gauge during metric collection.

        Args:
            lag_events: Number of events the subscription is behind
        """
        self._lag_value = max(0, lag_events)

    def record_state(self, state: str) -> None:
        """
        Update the current subscription state.

        This updates the internal state that will be reported
        by the observable gauge during metric collection.

        Args:
            state: Current state name (e.g., "live", "catching_up")
        """
        self._state_name = state.lower()
        self._state_value = STATE_MAPPING.get(self._state_name, StateValue.UNKNOWN)

    @contextmanager
    def time_processing(self) -> Generator[_Timer, None, None]:
        """
        Context manager for timing event processing.

        Yields a timer object with a duration_ms property
        that can be used to record processing time.

        Example:
            >>> with metrics.time_processing() as timer:
            ...     process_event(event)
            >>> metrics.record_event_processed("OrderCreated", timer.duration_ms)

        Yields:
            Timer object with duration_ms property
        """
        timer = _Timer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()

    def get_snapshot(self) -> MetricSnapshot:
        """
        Get a snapshot of current metric values.

        Useful for testing and debugging to see accumulated values.

        Returns:
            MetricSnapshot with current values
        """
        return MetricSnapshot(
            events_processed=self._processed_count,
            events_failed=self._failed_count,
            total_processing_time_ms=self._total_duration_ms,
            current_lag=self._lag_value,
            current_state=self._state_value,
            current_state_name=self._state_name,
        )

    @property
    def metrics_enabled(self) -> bool:
        """
        Check if metrics are currently enabled.

        Returns:
            True if metrics are enabled and OTel is available
        """
        return self.enable_metrics and OTEL_METRICS_AVAILABLE

    @property
    def current_lag(self) -> int:
        """Get current lag value."""
        return self._lag_value

    @property
    def current_state(self) -> str:
        """Get current state name."""
        return self._state_name


class _Timer:
    """
    Internal timer for measuring processing duration.

    Used by the time_processing context manager.
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


# Global metrics registry for tracking all subscription metrics instances
_metrics_registry: dict[str, SubscriptionMetrics] = {}


def get_metrics(subscription_name: str, enable_metrics: bool = True) -> SubscriptionMetrics:
    """
    Get or create metrics instance for a subscription.

    Creates a new SubscriptionMetrics instance if one doesn't exist
    for the given subscription name, or returns the existing one.

    Args:
        subscription_name: Name of the subscription
        enable_metrics: Whether to enable metrics (default True)

    Returns:
        SubscriptionMetrics instance for the subscription
    """
    if subscription_name not in _metrics_registry:
        _metrics_registry[subscription_name] = SubscriptionMetrics(
            subscription_name=subscription_name,
            enable_metrics=enable_metrics,
        )
    return _metrics_registry[subscription_name]


def clear_metrics_registry() -> None:
    """
    Clear the metrics registry.

    Useful for testing to reset state between tests.
    """
    global _metrics_registry
    _metrics_registry = {}
    reset_meter()


__all__ = [
    # Constants
    "OTEL_METRICS_AVAILABLE",
    "STATE_MAPPING",
    "StateValue",
    # Classes
    "SubscriptionMetrics",
    "MetricSnapshot",
    "NoOpCounter",
    "NoOpHistogram",
    "NoOpGauge",
    # Functions
    "get_metrics",
    "clear_metrics_registry",
    "reset_meter",
]
