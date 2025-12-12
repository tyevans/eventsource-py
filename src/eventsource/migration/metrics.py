"""
OpenTelemetry metrics for migration operations.

This module provides metrics instrumentation for the multi-tenant live
migration system, tracking events copied, sync lag, phase durations,
cutover timing, and error counts.

The metrics gracefully degrade when OpenTelemetry is not installed -
all operations become no-ops without raising errors.

Example:
    >>> from eventsource.migration.metrics import MigrationMetrics
    >>>
    >>> metrics = MigrationMetrics("migration-123", "tenant-456")
    >>> metrics.record_events_copied(1000, 100.0)
    >>> metrics.record_sync_lag(50)
    >>> metrics.record_phase_duration("bulk_copy", 300.0)
    >>> metrics.record_cutover_duration(45.0, success=True)

Metrics Exposed:
    - migration.events.copied (Counter): Total events copied during bulk copy
    - migration.events.copied.rate (Gauge): Current rate of events being copied
    - migration.sync.lag (Gauge): Current sync lag during dual-write
    - migration.phase.duration (Histogram): Time spent in each phase
    - migration.cutover.duration (Histogram): Time taken for cutover operations
    - migration.active (Gauge): Number of active migrations
    - migration.target.writes.failed (Counter): Failed writes to target during dual-write
    - migration.verification.failures (Counter): Consistency verification failures

All metrics include the 'migration_id' and 'tenant_id' attributes for filtering.

See Also:
    - Task: P4-003-migration-metrics.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
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
        >>>
        >>> # Record bulk copy progress
        >>> metrics.record_events_copied(1000, 100.0)
        >>>
        >>> # Record sync lag during dual-write
        >>> metrics.record_sync_lag(50)
        >>>
        >>> # Record phase duration
        >>> metrics.record_phase_duration("bulk_copy", 300.0)
        >>>
        >>> # Time a phase automatically
        >>> with metrics.time_phase("dual_write"):
        ...     await do_dual_write()
        >>>
        >>> # Record cutover
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
    def time_phase(self, phase: str) -> Generator[_PhaseTimer, None, None]:
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
    def time_cutover(self) -> Generator[_CutoverTimer, None, None]:
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


# Global active migrations tracking
_active_migrations: dict[str, MigrationMetrics] = {}


class ActiveMigrationsTracker:
    """
    Tracks active migrations for the active_migrations gauge.

    This is a singleton that maintains the count of active migrations
    and exposes it as an observable gauge.

    Example:
        >>> tracker = ActiveMigrationsTracker.get_instance()
        >>> tracker.register_migration("migration-123")
        >>> # ... migration runs ...
        >>> tracker.unregister_migration("migration-123")
    """

    _instance: ActiveMigrationsTracker | None = None
    _initialized: bool = False

    def __init__(self) -> None:
        """Initialize tracker."""
        self._active_count: int = 0
        self._migrations: set[str] = set()
        self._setup_gauge()

    @classmethod
    def get_instance(cls) -> ActiveMigrationsTracker:
        """
        Get the singleton instance.

        Returns:
            The singleton ActiveMigrationsTracker instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """
        Reset the singleton instance.

        Useful for testing to ensure fresh state.
        """
        cls._instance = None
        cls._initialized = False

    def _setup_gauge(self) -> None:
        """Set up the active migrations gauge."""
        if not OTEL_METRICS_AVAILABLE:
            return

        if ActiveMigrationsTracker._initialized:
            return

        meter = _get_meter()
        if meter is None:
            return

        meter.create_observable_gauge(
            name="migration.active",
            callbacks=[self._observe_active_count],
            unit="migrations",
            description="Number of currently active migrations",
        )
        ActiveMigrationsTracker._initialized = True

    def _observe_active_count(self, options: Any) -> Any:
        """
        Callback for observable active migrations gauge.

        Args:
            options: OpenTelemetry callback options

        Yields:
            Observation with active count
        """
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(value=self._active_count)

    def register_migration(self, migration_id: str) -> None:
        """
        Register a migration as active.

        Args:
            migration_id: Unique migration identifier
        """
        if migration_id not in self._migrations:
            self._migrations.add(migration_id)
            self._active_count = len(self._migrations)

    def unregister_migration(self, migration_id: str) -> None:
        """
        Unregister a migration (no longer active).

        Args:
            migration_id: Unique migration identifier
        """
        if migration_id in self._migrations:
            self._migrations.discard(migration_id)
            self._active_count = len(self._migrations)

    @property
    def active_count(self) -> int:
        """Get current count of active migrations."""
        return self._active_count

    @property
    def active_migrations(self) -> set[str]:
        """Get set of active migration IDs."""
        return set(self._migrations)


# Global metrics registry for tracking all migration metrics instances
_metrics_registry: dict[str, MigrationMetrics] = {}


def get_migration_metrics(
    migration_id: str,
    tenant_id: str,
    enable_metrics: bool = True,
) -> MigrationMetrics:
    """
    Get or create metrics instance for a migration.

    Creates a new MigrationMetrics instance if one doesn't exist
    for the given migration ID, or returns the existing one.
    Also registers the migration as active.

    Args:
        migration_id: Unique migration identifier
        tenant_id: Tenant identifier
        enable_metrics: Whether to enable metrics (default True)

    Returns:
        MigrationMetrics instance for the migration
    """
    if migration_id not in _metrics_registry:
        _metrics_registry[migration_id] = MigrationMetrics(
            migration_id=migration_id,
            tenant_id=tenant_id,
            enable_metrics=enable_metrics,
        )
        # Register as active
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.register_migration(migration_id)

    return _metrics_registry[migration_id]


def release_migration_metrics(migration_id: str) -> None:
    """
    Release metrics instance for a completed migration.

    Removes the migration from the registry and unregisters it as active.

    Args:
        migration_id: Unique migration identifier
    """
    if migration_id in _metrics_registry:
        del _metrics_registry[migration_id]
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.unregister_migration(migration_id)


def clear_metrics_registry() -> None:
    """
    Clear the metrics registry.

    Useful for testing to reset state between tests.
    """
    global _metrics_registry
    _metrics_registry = {}
    reset_meter()
    ActiveMigrationsTracker.reset()


__all__ = [
    # Constants
    "OTEL_METRICS_AVAILABLE",
    # Classes
    "MigrationMetrics",
    "MigrationMetricSnapshot",
    "ActiveMigrationsTracker",
    "NoOpCounter",
    "NoOpHistogram",
    "NoOpGauge",
    # Functions
    "get_migration_metrics",
    "release_migration_metrics",
    "clear_metrics_registry",
    "reset_meter",
]
