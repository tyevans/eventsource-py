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
    """Container for subscription metrics instruments (safe no-op when OTel unavailable)."""

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
        if self.enable_metrics and OTEL_METRICS_AVAILABLE:
            self._setup_metrics()
        else:
            self._setup_noop()

    def _setup_metrics(self) -> None:
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
        for name, cb, unit, desc in (
            ("subscription.lag", self._observe_lag, "events", "Current event lag (events behind)"),
            (
                "subscription.state",
                self._observe_state,
                "1",
                "Current subscription state (numeric)",
            ),
        ):
            self._meter.create_observable_gauge(
                name=name, callbacks=[cb], unit=unit, description=desc
            )

    def _setup_noop(self) -> None:
        self._events_processed_counter = NoOpCounter()
        self._events_failed_counter = NoOpCounter()
        self._processing_duration_histogram = NoOpHistogram()

    def _observe_lag(self, options: Any) -> Any:
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(self._lag_value, {"subscription": self.subscription_name})

    def _observe_state(self, options: Any) -> Any:
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            attrs = {"subscription": self.subscription_name, "state_name": self._state_name}
            yield Observation(self._state_value, attrs)

    def record_event_processed(
        self, event_type: str, duration_ms: float, status: str = "success"
    ) -> None:
        """Record a successfully processed event."""
        attrs = {"subscription": self.subscription_name, "event.type": event_type, "status": status}
        self._events_processed_counter.add(1, attrs)
        self._processing_duration_histogram.record(duration_ms, attrs)
        self._processed_count += 1
        self._total_duration_ms += duration_ms

    def record_event_failed(
        self, event_type: str, error_type: str, duration_ms: float | None = None
    ) -> None:
        """Record a failed event."""
        base = {"subscription": self.subscription_name, "event.type": event_type}
        self._events_failed_counter.add(1, {**base, "error.type": error_type})
        if duration_ms is not None:
            self._processing_duration_histogram.record(duration_ms, {**base, "status": "failed"})
            self._total_duration_ms += duration_ms
        self._failed_count += 1

    def record_lag(self, lag_events: int) -> None:
        self._lag_value = max(0, lag_events)

    def record_state(self, state: str) -> None:
        self._state_name = state.lower()
        self._state_value = STATE_MAPPING.get(self._state_name, StateValue.UNKNOWN)

    @contextmanager
    def time_processing(self) -> Generator[_Timer]:
        timer = _Timer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()

    def get_snapshot(self) -> MetricSnapshot:
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
        return self.enable_metrics and OTEL_METRICS_AVAILABLE

    @property
    def current_lag(self) -> int:
        return self._lag_value

    @property
    def current_state(self) -> str:
        return self._state_name


__all__ = ["SubscriptionMetrics"]
