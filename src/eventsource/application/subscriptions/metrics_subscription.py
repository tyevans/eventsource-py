"""Subscription metrics container for recording events, lag, and state."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from eventsource.application.subscriptions.metrics_types import (
    OTEL_METRICS_AVAILABLE,
    STATE_MAPPING,
    MetricSnapshot,
    NoOpCounter,
    NoOpHistogram,
    StateValue,
    _get_meter,
    _Timer,
)


@dataclass
class SubscriptionMetrics:
    """Container for subscription metrics instruments.

    Provides methods to record events processed, failures, processing
    duration, lag, and state changes. Safe to call when OpenTelemetry
    is not installed (becomes no-ops).
    """

    subscription_name: str
    enable_metrics: bool = True

    _meter: Any = field(default=None, init=False, repr=False)
    _events_processed_counter: Any = field(default=None, init=False, repr=False)
    _events_failed_counter: Any = field(default=None, init=False, repr=False)
    _processing_duration_histogram: Any = field(default=None, init=False, repr=False)
    _lag_value: int = field(default=0, init=False, repr=False)
    _state_value: int = field(default=StateValue.UNKNOWN, init=False, repr=False)
    _state_name: str = field(default="unknown", init=False, repr=False)

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

        self._events_processed_counter = self._meter.create_counter(
            name="subscription.events.processed",
            unit="events",
            description="Total number of events processed by subscription",
        )
        self._events_failed_counter = self._meter.create_counter(
            name="subscription.events.failed",
            unit="events",
            description="Total number of events that failed processing",
        )
        self._processing_duration_histogram = self._meter.create_histogram(
            name="subscription.processing.duration",
            unit="ms",
            description="Event processing duration in milliseconds",
        )
        self._meter.create_observable_gauge(
            name="subscription.lag",
            callbacks=[self._observe_lag],
            unit="events",
            description="Current event lag (events behind)",
        )
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
        """Callback for observable lag gauge."""
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(
                value=self._lag_value,
                attributes={"subscription": self.subscription_name},
            )

    def _observe_state(self, options: Any) -> Any:
        """Callback for observable state gauge."""
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
        """Record a successfully processed event."""
        attrs = {
            "subscription": self.subscription_name,
            "event.type": event_type,
            "status": status,
        }
        self._events_processed_counter.add(1, attrs)
        self._processing_duration_histogram.record(duration_ms, attrs)
        self._processed_count += 1
        self._total_duration_ms += duration_ms

    def record_event_failed(
        self,
        event_type: str,
        error_type: str,
        duration_ms: float | None = None,
    ) -> None:
        """Record a failed event."""
        attrs = {
            "subscription": self.subscription_name,
            "event.type": event_type,
            "error.type": error_type,
        }
        self._events_failed_counter.add(1, attrs)

        if duration_ms is not None:
            failure_attrs = {
                "subscription": self.subscription_name,
                "event.type": event_type,
                "status": "failed",
            }
            self._processing_duration_histogram.record(duration_ms, failure_attrs)
            self._total_duration_ms += duration_ms

        self._failed_count += 1

    def record_lag(self, lag_events: int) -> None:
        """Update the current lag value."""
        self._lag_value = max(0, lag_events)

    def record_state(self, state: str) -> None:
        """Update the current subscription state."""
        self._state_name = state.lower()
        self._state_value = STATE_MAPPING.get(self._state_name, StateValue.UNKNOWN)

    @contextmanager
    def time_processing(self) -> Generator[_Timer]:
        """Context manager for timing event processing."""
        timer = _Timer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()

    def get_snapshot(self) -> MetricSnapshot:
        """Get a snapshot of current metric values."""
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
        """Check if metrics are currently enabled."""
        return self.enable_metrics and OTEL_METRICS_AVAILABLE

    @property
    def current_lag(self) -> int:
        """Get current lag value."""
        return self._lag_value

    @property
    def current_state(self) -> str:
        """Get current state name."""
        return self._state_name


__all__ = ["SubscriptionMetrics"]
