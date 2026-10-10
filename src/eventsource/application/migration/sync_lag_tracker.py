"""
SyncLagTracker - Monitor synchronization lag between source and target stores.

The SyncLagTracker counts how many source events the target has not yet
copied during the dual-write phase of migration. It calculates lag metrics,
tracks lag history, and determines when synchronization is close enough for
cutover.

Responsibilities:
    - Count source events not yet copied to the target
    - Track lag metrics over time (current, average, max)
    - Provide convergence detection for cutover eligibility
    - Integrate with DualWriteInterceptor for real-time updates
    - Support configurable sync thresholds from MigrationConfig

Usage:
    >>> from eventsource.application.migration import SyncLagTracker, MigrationConfig
    >>>
    >>> tracker = SyncLagTracker(
    ...     source_store=source,
    ...     target_store=target,
    ...     config=MigrationConfig(),  # cutover_max_lag_events defaults to 0 (strict)
    ... )
    >>>
    >>> # Calculate current lag (since = last copied source position)
    >>> lag = await tracker.calculate_lag(since=migration.last_source_position)
    >>> print(f"Lag: {lag.events} events")
    >>>
    >>> # Check if ready for cutover
    >>> if tracker.is_sync_ready():
    ...     print("Ready for cutover!")
    >>>
    >>> # Get lag statistics
    >>> stats = tracker.get_lag_stats()
    >>> print(f"Avg lag: {stats['average_lag']}, Max lag: {stats['max_lag']}")

See Also:
    - Task: P2-002-sync-lag-tracking.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import UTC, datetime
from uuid import UUID

from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.application.migration.sync_lag_tracker_analysis import SyncLagAnalysisMixin
from eventsource.application.migration.sync_lag_tracker_types import (
    ATTR_SYNC_THRESHOLD,
    LagSample,
    LagStats,
)
from eventsource.observability import (
    ATTR_TENANT_ID,
    Tracer,
    create_tracer,
)
from eventsource.observability.attributes import ATTR_MIGRATION_SYNC_LAG_EVENTS
from eventsource.ports import FeedReadOptions, FullEventStore, Position
from eventsource.ports.migration.models import MigrationConfig, SyncLag

logger = logging.getLogger(__name__)


class SyncLagTracker(SyncLagAnalysisMixin):
    """
    Tracks synchronization lag between source and target stores.

    Counts the source events the target has not yet copied during the
    dual-write phase of migration. Provides lag metrics, convergence
    detection, and cutover eligibility checks.

    The tracker maintains a sliding window of lag samples to calculate
    statistics like average and max lag. It also detects convergence
    trends to help determine optimal cutover timing.

    Example:
        >>> tracker = SyncLagTracker(
        ...     source_store=shared_store,
        ...     target_store=dedicated_store,
        ...     config=MigrationConfig(cutover_max_lag_events=50),  # nonzero: accepts up to 50 events lost at the switch
        ...     tenant_id=tenant_uuid,
        ... )
        >>>
        >>> # Take a lag measurement
        >>> lag = await tracker.calculate_lag(since=last_copied_position)
        >>> print(f"Lag: {lag.events} events")
        >>>
        >>> # Check readiness for cutover
        >>> if tracker.is_sync_ready():
        ...     print("Synchronization lag is within threshold")

    Attributes:
        _source: The source (authoritative) event store.
        _target: The target event store being migrated to.
        _config: Migration configuration with sync threshold.
        _tenant_id: Optional tenant ID for multi-tenant migrations.
        _lag_samples: Deque of recent lag samples for statistics.
        _current_lag: Most recent lag measurement.
    """

    def __init__(
        self,
        source_store: FullEventStore,
        target_store: FullEventStore,
        config: MigrationConfig | None = None,
        tenant_id: UUID | None = None,
        *,
        migration_id: UUID | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        max_sample_history: int = 100,
    ) -> None:
        """
        Initialize the sync lag tracker.

        Args:
            source_store: The authoritative source event store.
            target_store: The target event store being migrated to.
            config: Migration configuration (defaults to MigrationConfig()).
            tenant_id: Optional tenant ID for logging and tracing.
            migration_id: Optional migration ID. When set, each
                `calculate_lag()` call reports its result to that
                migration's `MigrationMetrics` (the `migration.sync.lag`
                gauge). Omitted by tests and any caller not tracking a
                specific migration -- lag is still computed and returned
                either way, only the metric emission is skipped.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
            max_sample_history: Maximum number of lag samples to retain
                for statistics calculation.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store
        self._config = config or MigrationConfig()
        self._tenant_id = tenant_id
        self._migration_id = migration_id
        self._max_sample_history = max_sample_history

        # Lag tracking state
        self._lag_samples: deque[LagSample] = deque(maxlen=max_sample_history)
        self._current_lag: SyncLag | None = None

    # =========================================================================
    # Public Properties
    # =========================================================================

    @property
    def source_store(self) -> FullEventStore:
        """Get the source (authoritative) event store."""
        return self._source

    @property
    def target_store(self) -> FullEventStore:
        """Get the target event store."""
        return self._target

    @property
    def config(self) -> MigrationConfig:
        """Get the migration configuration."""
        return self._config

    @property
    def tenant_id(self) -> UUID | None:
        """Get the tenant ID this tracker is monitoring."""
        return self._tenant_id

    @property
    def current_lag(self) -> SyncLag | None:
        """Get the most recent lag measurement."""
        return self._current_lag

    @property
    def sync_threshold(self) -> int:
        """Get the maximum lag events allowed for cutover."""
        return self._config.cutover_max_lag_events

    # =========================================================================
    # Lag Calculation
    # =========================================================================

    async def calculate_lag(self, *, since: Position | None = None) -> SyncLag:
        """Count source events not yet copied, bounded by the sync threshold.

        `since` is the last source position the target has copied (the
        migration's `last_source_position`); None means nothing has been
        copied and the count starts at the head of the source feed. The
        count is exact up to `cutover_max_lag_events + 1`; beyond that it
        reports the bound and stops reading, which is all a convergence
        decision needs.

        The reported `source_position` and `target_position` are each
        store's own current position, carried for reporting only -- they
        come from different stores and are never compared with each other.

        Args:
            since: The anchor to count from -- the furthest source
                position provably present in the target. During
                dual-write this is `DualWriteInterceptor.safe_lag_anchor`
                applied to the migration's `last_source_position`, which
                advances over mirrored writes but never past a mirroring
                failure. None counts from the head of the source feed.

                The interceptor's watermarks are in memory and are not
                persisted, so after an orchestrator restart the anchor
                falls back to the bulk-copy checkpoint and CUTOVER WILL
                REFUSE until a fresh copy pass advances that checkpoint.
                That trade is accepted deliberately -- persisting a
                watermark per event would cost more than the failure mode
                it avoids.

        Returns:
            SyncLag with the count behind and both stores' positions.
        """
        with self._tracer.span(
            "eventsource.sync_lag.calculate_lag",
            {
                ATTR_TENANT_ID: str(self._tenant_id) if self._tenant_id else None,
                ATTR_SYNC_THRESHOLD: self._config.cutover_max_lag_events,
            },
        ) as span:
            threshold = self._config.cutover_max_lag_events

            # Read one past the bound so "at the bound" and "over the
            # bound" are distinguishable, then stop.
            lag_events = 0
            async for _ in self._source.read_all(
                since,
                FeedReadOptions(tenant_id=self._tenant_id, limit=threshold + 1),
            ):
                lag_events += 1

            if span is not None:
                span.set_attribute(ATTR_MIGRATION_SYNC_LAG_EVENTS, lag_events)

            count_is_bounded = lag_events > threshold

            # Reporting only: each store's own head position.
            source_position = await self._source.current_position()
            target_position = await self._target.current_position()

            lag = SyncLag(
                events=lag_events,
                source_position=source_position,
                target_position=target_position,
                timestamp=datetime.now(UTC),
                count_is_bounded=count_is_bounded,
            )

            # Store and sample
            self._current_lag = lag
            self._add_sample(lag)

            if self._migration_id is not None:
                get_migration_metrics(
                    str(self._migration_id),
                    str(self._tenant_id) if self._tenant_id else "unknown",
                ).record_sync_lag(lag_events)

            # Log the measurement
            logger.debug(
                f"Sync lag calculated: {lag_events} events behind"
                + (" (bounded)" if count_is_bounded else "")
                + (f" for tenant {self._tenant_id}" if self._tenant_id else "")
            )

            return lag

    def _add_sample(self, lag: SyncLag) -> None:
        """
        Add a lag measurement to the sample history.

        Args:
            lag: The lag measurement to record.
        """
        sample = LagSample(lag=lag, sampled_at=datetime.now(UTC))
        self._lag_samples.append(sample)


__all__ = [
    "LagSample",
    "LagStats",
    "SyncLagTracker",
]
