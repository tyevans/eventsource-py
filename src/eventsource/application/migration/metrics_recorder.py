"""MigrationMetrics container and metric recording instruments."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from eventsource.application.migration.metrics_types import (
    OTEL_METRICS_AVAILABLE,
    MigrationMetricSnapshot,
    NoOpCounter,
    NoOpHistogram,
    _CutoverTimer,
    _get_meter,
    _PhaseTimer,
)


@dataclass
class MigrationMetrics:
    """Container for migration metrics instruments (no-op safe when OpenTelemetry is absent)."""

    migration_id: str
    tenant_id: str
    enable_metrics: bool = True

    _meter: Any = field(default=None, init=False, repr=False)
    _events_copied_counter: Any = field(default=None, init=False, repr=False)
    _events_copied_rate_value: float = field(default=0.0, init=False, repr=False)
    _sync_lag_value: int = field(default=0, init=False, repr=False)
    _phase_duration_histogram: Any = field(default=None, init=False, repr=False)
    _cutover_duration_histogram: Any = field(default=None, init=False, repr=False)
    _failed_target_writes_counter: Any = field(default=None, init=False, repr=False)
    _verification_failures_counter: Any = field(default=None, init=False, repr=False)

    _events_copied_count: int = field(default=0, init=False, repr=False)
    _failed_writes_count: int = field(default=0, init=False, repr=False)
    _verification_failures_count: int = field(default=0, init=False, repr=False)
    _phase_durations: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _cutover_durations: list[float] = field(default_factory=list, init=False, repr=False)

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

        self._events_copied_counter = self._meter.create_counter(
            name="migration.events.copied",
            unit="events",
            description="Total number of events copied during bulk copy phase",
        )
        self._meter.create_observable_gauge(
            name="migration.events.copied.rate",
            callbacks=[self._observe_events_copied_rate],
            unit="events/s",
            description="Current rate of events being copied (events per second)",
        )
        self._meter.create_observable_gauge(
            name="migration.sync.lag",
            callbacks=[self._observe_sync_lag],
            unit="events",
            description="Current sync lag in number of events during dual-write",
        )
        self._phase_duration_histogram = self._meter.create_histogram(
            name="migration.phase.duration",
            unit="s",
            description="Time spent in each migration phase in seconds",
        )
        self._cutover_duration_histogram = self._meter.create_histogram(
            name="migration.cutover.duration",
            unit="ms",
            description="Time taken for cutover operations in milliseconds",
        )
        self._failed_target_writes_counter = self._meter.create_counter(
            name="migration.target.writes.failed",
            unit="writes",
            description="Number of failed writes to target store during dual-write",
        )
        self._verification_failures_counter = self._meter.create_counter(
            name="migration.verification.failures",
            unit="failures",
            description="Number of consistency verification failures",
        )

    def _setup_noop(self) -> None:
        self._events_copied_counter = NoOpCounter()
        self._phase_duration_histogram = NoOpHistogram()
        self._cutover_duration_histogram = NoOpHistogram()
        self._failed_target_writes_counter = NoOpCounter()
        self._verification_failures_counter = NoOpCounter()

    def _base_attributes(self) -> dict[str, str]:
        return {"migration_id": self.migration_id, "tenant_id": self.tenant_id}

    def _observe_events_copied_rate(self, options: Any) -> Any:
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(
                value=self._events_copied_rate_value, attributes=self._base_attributes()
            )

    def _observe_sync_lag(self, options: Any) -> Any:
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(value=self._sync_lag_value, attributes=self._base_attributes())

    def record_events_copied(self, count: int, rate_events_per_sec: float | None = None) -> None:
        attrs = self._base_attributes()
        self._events_copied_counter.add(count, attrs)
        if rate_events_per_sec is not None:
            self._events_copied_rate_value = rate_events_per_sec
        self._events_copied_count += count

    def record_sync_lag(self, lag_events: int) -> None:
        self._sync_lag_value = max(0, lag_events)

    def record_phase_duration(self, phase: str, duration_seconds: float) -> None:
        attrs = {**self._base_attributes(), "phase": phase}
        self._phase_duration_histogram.record(duration_seconds, attrs)
        self._phase_durations[phase] = self._phase_durations.get(phase, 0.0) + duration_seconds

    def record_cutover_duration(self, duration_ms: float, success: bool = True) -> None:
        attrs = {**self._base_attributes(), "success": str(success).lower()}
        self._cutover_duration_histogram.record(duration_ms, attrs)
        self._cutover_durations.append(duration_ms)

    def record_failed_target_write(self, error_type: str | None = None) -> None:
        attrs = self._base_attributes()
        if error_type:
            attrs["error_type"] = error_type
        self._failed_target_writes_counter.add(1, attrs)
        self._failed_writes_count += 1

    def record_verification_failure(self, failure_type: str | None = None) -> None:
        attrs = self._base_attributes()
        if failure_type:
            attrs["failure_type"] = failure_type
        self._verification_failures_counter.add(1, attrs)
        self._verification_failures_count += 1

    @contextmanager
    def time_phase(self, phase: str) -> Generator[_PhaseTimer]:
        timer = _PhaseTimer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()
            self.record_phase_duration(phase, timer.duration_seconds)

    @contextmanager
    def time_cutover(self) -> Generator[_CutoverTimer]:
        timer = _CutoverTimer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()
            self.record_cutover_duration(timer.duration_ms, timer.success)

    def get_snapshot(self) -> MigrationMetricSnapshot:
        return MigrationMetricSnapshot(
            events_copied=self._events_copied_count,
            events_copied_rate=self._events_copied_rate_value,
            sync_lag_events=self._sync_lag_value,
            failed_target_writes=self._failed_writes_count,
            verification_failures=self._verification_failures_count,
            phase_durations=dict(self._phase_durations),
            cutover_durations=list(self._cutover_durations),
        )

    @property
    def metrics_enabled(self) -> bool:
        return self.enable_metrics and OTEL_METRICS_AVAILABLE

    @property
    def current_sync_lag(self) -> int:
        return self._sync_lag_value

    @property
    def current_copy_rate(self) -> float:
        return self._events_copied_rate_value


__all__ = ["MigrationMetrics"]
