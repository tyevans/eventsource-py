"""
MigrationMetrics container and metric recording instruments.
"""

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
    """
    Container for migration metrics instruments.

    Provides methods to record events copied, sync lag, phase duration,
    cutover timing, and error counts. All methods are safe to call
    even when OpenTelemetry is not installed - they become no-ops.

    Attributes:
        migration_id: Unique migration identifier for metric labels
        tenant_id: Tenant identifier for metric labels
        enable_metrics: Whether metrics are enabled (default True)

    Example:
        >>> metrics = MigrationMetrics("migration-123", "tenant-456")
        >>> metrics.record_events_copied(1000, 100.0)
        >>> metrics.record_sync_lag(50)
        >>> metrics.record_phase_duration("bulk_copy", 300.0)
        >>> metrics.record_cutover_duration(45.0, success=True)
    """

    migration_id: str
    tenant_id: str
    enable_metrics: bool = True

    # Internal state
    _meter: Any = field(default=None, init=False, repr=False)
    _events_copied_counter: Any = field(default=None, init=False, repr=False)
    _events_copied_rate_value: float = field(default=0.0, init=False, repr=False)
    _sync_lag_value: int = field(default=0, init=False, repr=False)
    _phase_duration_histogram: Any = field(default=None, init=False, repr=False)
    _cutover_duration_histogram: Any = field(default=None, init=False, repr=False)
    _failed_target_writes_counter: Any = field(default=None, init=False, repr=False)
    _verification_failures_counter: Any = field(default=None, init=False, repr=False)

    # Internal counters for snapshot
    _events_copied_count: int = field(default=0, init=False, repr=False)
    _failed_writes_count: int = field(default=0, init=False, repr=False)
    _verification_failures_count: int = field(default=0, init=False, repr=False)
    _phase_durations: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _cutover_durations: list[float] = field(default_factory=list, init=False, repr=False)

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

        # Counter: events copied
        self._events_copied_counter = self._meter.create_counter(
            name="migration.events.copied",
            unit="events",
            description="Total number of events copied during bulk copy phase",
        )

        # Observable Gauge: events copied rate
        self._meter.create_observable_gauge(
            name="migration.events.copied.rate",
            callbacks=[self._observe_events_copied_rate],
            unit="events/s",
            description="Current rate of events being copied (events per second)",
        )

        # Observable Gauge: sync lag
        self._meter.create_observable_gauge(
            name="migration.sync.lag",
            callbacks=[self._observe_sync_lag],
            unit="events",
            description="Current sync lag in number of events during dual-write",
        )

        # Histogram: phase duration
        self._phase_duration_histogram = self._meter.create_histogram(
            name="migration.phase.duration",
            unit="s",
            description="Time spent in each migration phase in seconds",
        )

        # Histogram: cutover duration
        self._cutover_duration_histogram = self._meter.create_histogram(
            name="migration.cutover.duration",
            unit="ms",
            description="Time taken for cutover operations in milliseconds",
        )

        # Counter: failed target writes
        self._failed_target_writes_counter = self._meter.create_counter(
            name="migration.target.writes.failed",
            unit="writes",
            description="Number of failed writes to target store during dual-write",
        )

        # Counter: verification failures
        self._verification_failures_counter = self._meter.create_counter(
            name="migration.verification.failures",
            unit="failures",
            description="Number of consistency verification failures",
        )

    def _setup_noop(self) -> None:
        """Set up no-op instruments when OTel not available."""
        self._events_copied_counter = NoOpCounter()
        self._phase_duration_histogram = NoOpHistogram()
        self._cutover_duration_histogram = NoOpHistogram()
        self._failed_target_writes_counter = NoOpCounter()
        self._verification_failures_counter = NoOpCounter()

    def _base_attributes(self) -> dict[str, str]:
        """Get base attributes for all metrics."""
        return {
            "migration_id": self.migration_id,
            "tenant_id": self.tenant_id,
        }

    def _observe_events_copied_rate(self, options: Any) -> Any:
        """
        Callback for observable events copied rate gauge.

        Called by OpenTelemetry during metric collection.

        Args:
            options: OpenTelemetry callback options

        Yields:
            Observation with rate value and attributes
        """
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(
                value=self._events_copied_rate_value,
                attributes=self._base_attributes(),
            )

    def _observe_sync_lag(self, options: Any) -> Any:
        """
        Callback for observable sync lag gauge.

        Called by OpenTelemetry during metric collection.

        Args:
            options: OpenTelemetry callback options

        Yields:
            Observation with sync lag value and attributes
        """
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(
                value=self._sync_lag_value,
                attributes=self._base_attributes(),
            )

    def record_events_copied(
        self,
        count: int,
        rate_events_per_sec: float | None = None,
    ) -> None:
        """
        Record events copied during bulk copy phase.

        Args:
            count: Number of events copied in this batch
            rate_events_per_sec: Current copy rate in events per second
        """
        attrs = self._base_attributes()
        self._events_copied_counter.add(count, attrs)

        if rate_events_per_sec is not None:
            self._events_copied_rate_value = rate_events_per_sec

        # Update internal counter for snapshot
        self._events_copied_count += count

    def record_sync_lag(self, lag_events: int) -> None:
        """
        Update the current sync lag value.

        This updates the internal state that will be reported
        by the observable gauge during metric collection.

        Args:
            lag_events: Number of events the target is behind source
        """
        self._sync_lag_value = max(0, lag_events)

    def record_phase_duration(
        self,
        phase: str,
        duration_seconds: float,
    ) -> None:
        """
        Record duration for a migration phase.

        Args:
            phase: Phase name (e.g., 'bulk_copy', 'dual_write', 'cutover')
            duration_seconds: Duration in seconds
        """
        attrs = {**self._base_attributes(), "phase": phase}
        self._phase_duration_histogram.record(duration_seconds, attrs)

        # Update internal tracking for snapshot
        if phase not in self._phase_durations:
            self._phase_durations[phase] = 0.0
        self._phase_durations[phase] += duration_seconds

    def record_cutover_duration(
        self,
        duration_ms: float,
        success: bool = True,
    ) -> None:
        """
        Record cutover operation duration.

        Args:
            duration_ms: Duration in milliseconds
            success: Whether the cutover was successful
        """
        attrs = {
            **self._base_attributes(),
            "success": str(success).lower(),
        }
        self._cutover_duration_histogram.record(duration_ms, attrs)

        # Update internal tracking for snapshot
        self._cutover_durations.append(duration_ms)

    def record_failed_target_write(
        self,
        error_type: str | None = None,
    ) -> None:
        """
        Record a failed write to the target store during dual-write.

        Args:
            error_type: Type of error that caused the failure
        """
        attrs = self._base_attributes()
        if error_type:
            attrs["error_type"] = error_type
        self._failed_target_writes_counter.add(1, attrs)

        # Update internal counter for snapshot
        self._failed_writes_count += 1

    def record_verification_failure(
        self,
        failure_type: str | None = None,
    ) -> None:
        """
        Record a consistency verification failure.

        Args:
            failure_type: Type of verification failure
        """
        attrs = self._base_attributes()
        if failure_type:
            attrs["failure_type"] = failure_type
        self._verification_failures_counter.add(1, attrs)

        # Update internal counter for snapshot
        self._verification_failures_count += 1

    @contextmanager
    def time_phase(self, phase: str) -> Generator[_PhaseTimer]:
        """
        Context manager for timing a migration phase.

        Automatically records the phase duration when the context exits.

        Args:
            phase: Phase name (e.g., 'bulk_copy', 'dual_write')

        Example:
            >>> with metrics.time_phase("bulk_copy"):
            ...     await do_bulk_copy()

        Yields:
            PhaseTimer object with duration_seconds property
        """
        timer = _PhaseTimer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()
            self.record_phase_duration(phase, timer.duration_seconds)

    @contextmanager
    def time_cutover(self) -> Generator[_CutoverTimer]:
        """
        Context manager for timing a cutover operation.

        Automatically records the cutover duration when the context exits.
        The success status can be set on the timer before exit.

        Example:
            >>> with metrics.time_cutover() as timer:
            ...     success = await do_cutover()
            ...     timer.success = success

        Yields:
            CutoverTimer object with duration_ms property and success attribute
        """
        timer = _CutoverTimer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()
            self.record_cutover_duration(timer.duration_ms, timer.success)

    def get_snapshot(self) -> MigrationMetricSnapshot:
        """
        Get a snapshot of current metric values.

        Useful for testing and debugging to see accumulated values.

        Returns:
            MigrationMetricSnapshot with current values
        """
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
        """
        Check if metrics are currently enabled.

        Returns:
            True if metrics are enabled and OTel is available
        """
        return self.enable_metrics and OTEL_METRICS_AVAILABLE

    @property
    def current_sync_lag(self) -> int:
        """Get current sync lag value."""
        return self._sync_lag_value

    @property
    def current_copy_rate(self) -> float:
        """Get current events copied rate."""
        return self._events_copied_rate_value


__all__ = ["MigrationMetrics"]
