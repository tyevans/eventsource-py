"""
OpenTelemetry metrics for migration operations.

This module provides metrics instrumentation for the multi-tenant live
migration system, tracking events copied, sync lag, phase durations,
cutover timing, and error counts.

The metrics gracefully degrade when OpenTelemetry is not installed -
all operations become no-ops without raising errors.

Example:
    >>> from eventsource.application.migration.metrics import MigrationMetrics
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

from eventsource.application.migration.metrics_recorder import MigrationMetrics
from eventsource.application.migration.metrics_registry import (
    ActiveMigrationsTracker,
    clear_metrics_registry,
    get_migration_metrics,
    release_migration_metrics,
)
from eventsource.application.migration.metrics_types import (
    OTEL_METRICS_AVAILABLE,
    MigrationMetricSnapshot,
    NoOpCounter,
    NoOpGauge,
    NoOpHistogram,
    _CutoverTimer,
    _get_meter,
    _PhaseTimer,
    reset_meter,
)

__all__ = [
    # Constants
    "OTEL_METRICS_AVAILABLE",
    # Classes
    "ActiveMigrationsTracker",
    "MigrationMetrics",
    "MigrationMetricSnapshot",
    "NoOpCounter",
    "NoOpGauge",
    "NoOpHistogram",
    "_CutoverTimer",
    "_PhaseTimer",
    # Functions
    "_get_meter",
    "clear_metrics_registry",
    "get_migration_metrics",
    "release_migration_metrics",
    "reset_meter",
]
