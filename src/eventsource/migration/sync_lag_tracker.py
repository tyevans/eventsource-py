"""
SyncLagTracker - Monitor synchronization lag between source and target stores.

The SyncLagTracker monitors the difference between source and target store
positions during the dual-write phase of migration. It calculates lag metrics,
tracks lag history, and determines when synchronization is close enough for
cutover.

Responsibilities:
    - Calculate current lag between source and target positions
    - Track lag metrics over time (current, average, max)
    - Provide convergence detection for cutover eligibility
    - Integrate with DualWriteInterceptor for real-time updates
    - Support configurable sync thresholds from MigrationConfig

Usage:
    >>> from eventsource.migration import SyncLagTracker, MigrationConfig
    >>>
    >>> tracker = SyncLagTracker(
    ...     source_store=source,
    ...     target_store=target,
    ...     config=MigrationConfig(cutover_max_lag_events=100),
    ... )
    >>>
    >>> # Calculate current lag
    >>> lag = await tracker.calculate_lag()
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
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.migration.models import MigrationConfig, SyncLag
from eventsource.observability import (
    ATTR_TENANT_ID,
    Tracer,
    create_tracer,
)

if TYPE_CHECKING:
    from eventsource.stores.interface import EventStore

logger = logging.getLogger(__name__)


# Custom attribute keys for sync lag tracing
ATTR_SOURCE_POSITION = "eventsource.sync_lag.source_position"
ATTR_TARGET_POSITION = "eventsource.sync_lag.target_position"
ATTR_LAG_EVENTS = "eventsource.sync_lag.lag_events"
ATTR_IS_CONVERGED = "eventsource.sync_lag.is_converged"
ATTR_IS_SYNC_READY = "eventsource.sync_lag.is_sync_ready"
ATTR_SYNC_THRESHOLD = "eventsource.sync_lag.sync_threshold"


@dataclass
class LagSample:
    """
    A single lag measurement sample.

    Attributes:
        lag: The SyncLag measurement.
        sampled_at: When the sample was taken.
    """

    lag: SyncLag
    sampled_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class LagStats:
    """
    Aggregate statistics about sync lag over time.

    Provides summary metrics for monitoring sync lag trends.

    Attributes:
        current_lag: Most recent lag measurement (events).
        average_lag: Average lag over the sample window.
        max_lag: Maximum lag observed in the sample window.
        min_lag: Minimum lag observed in the sample window.
        sample_count: Number of samples in the window.
        first_sample_at: Timestamp of the first sample.
        last_sample_at: Timestamp of the most recent sample.
        is_converging: Whether lag is trending downward.
    """

    current_lag: int
    average_lag: float
    max_lag: int
    min_lag: int
    sample_count: int
    first_sample_at: datetime | None
    last_sample_at: datetime | None
    is_converging: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "current_lag": self.current_lag,
            "average_lag": self.average_lag,
            "max_lag": self.max_lag,
            "min_lag": self.min_lag,
            "sample_count": self.sample_count,
            "first_sample_at": (self.first_sample_at.isoformat() if self.first_sample_at else None),
            "last_sample_at": (self.last_sample_at.isoformat() if self.last_sample_at else None),
            "is_converging": self.is_converging,
        }


class SyncLagTracker:
    """
    Tracks synchronization lag between source and target stores.

    Monitors the difference between source and target store positions
    during the dual-write phase of migration. Provides lag metrics,
    convergence detection, and cutover eligibility checks.

    The tracker maintains a sliding window of lag samples to calculate
    statistics like average and max lag. It also detects convergence
    trends to help determine optimal cutover timing.

    Example:
        >>> tracker = SyncLagTracker(
        ...     source_store=shared_store,
        ...     target_store=dedicated_store,
        ...     config=MigrationConfig(cutover_max_lag_events=50),
        ...     tenant_id=tenant_uuid,
        ... )
        >>>
        >>> # Take a lag measurement
        >>> lag = await tracker.calculate_lag()
        >>> print(f"Source: {lag.source_position}, Target: {lag.target_position}")
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
        source_store: EventStore,
        target_store: EventStore,
        config: MigrationConfig | None = None,
        tenant_id: UUID | None = None,
        *,
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
        self._max_sample_history = max_sample_history

        # Lag tracking state
        self._lag_samples: deque[LagSample] = deque(maxlen=max_sample_history)
        self._current_lag: SyncLag | None = None

    # =========================================================================
    # Public Properties
    # =========================================================================

    @property
    def source_store(self) -> EventStore:
        """Get the source (authoritative) event store."""
        return self._source

    @property
    def target_store(self) -> EventStore:
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

    async def calculate_lag(self) -> SyncLag:
        """
        Calculate the current lag between source and target stores.

        Queries both stores for their current global position and
        calculates the difference. The result is stored and added
        to the sample history for statistics.

        Returns:
            SyncLag with current positions and event lag.

        Example:
            >>> lag = await tracker.calculate_lag()
            >>> print(f"Source at {lag.source_position}")
            >>> print(f"Target at {lag.target_position}")
            >>> print(f"Lag: {lag.events} events")
        """
        with self._tracer.span(
            "eventsource.sync_lag.calculate_lag",
            {
                ATTR_TENANT_ID: str(self._tenant_id) if self._tenant_id else None,
                ATTR_SYNC_THRESHOLD: self._config.cutover_max_lag_events,
            },
        ):
            # Get current positions from both stores
            source_position = await self._source.get_global_position()
            target_position = await self._target.get_global_position()

            # Calculate lag (can be negative if target is somehow ahead)
            lag_events = max(0, source_position - target_position)

            # Create lag measurement
            lag = SyncLag(
                events=lag_events,
                source_position=source_position,
                target_position=target_position,
                timestamp=datetime.now(UTC),
            )

            # Store and sample
            self._current_lag = lag
            self._add_sample(lag)

            # Log the measurement
            logger.debug(
                f"Sync lag calculated: {lag_events} events "
                f"(source={source_position}, target={target_position})"
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

    # =========================================================================
    # Convergence Detection
    # =========================================================================

    def is_converged(self, max_lag: int | None = None) -> bool:
        """
        Check if stores are synchronized within the specified threshold.

        Args:
            max_lag: Maximum acceptable lag in events. If None, uses
                the configured cutover_max_lag_events threshold.

        Returns:
            True if current lag is within the threshold.

        Example:
            >>> if tracker.is_converged():
            ...     print("Stores are synchronized!")
            >>>
            >>> # Use custom threshold
            >>> if tracker.is_converged(max_lag=10):
            ...     print("Within 10 events!")
        """
        if self._current_lag is None:
            return False

        threshold = max_lag if max_lag is not None else self._config.cutover_max_lag_events
        return self._current_lag.events <= threshold

    def is_sync_ready(self) -> bool:
        """
        Check if synchronization is ready for cutover.

        This is the primary check for cutover eligibility. It verifies
        that the current lag is within the configured threshold.

        The method uses the cutover_max_lag_events from MigrationConfig
        as the threshold.

        Returns:
            True if lag is within cutover_max_lag_events threshold.

        Example:
            >>> if tracker.is_sync_ready():
            ...     await cutover_manager.execute_cutover()
        """
        with self._tracer.span(
            "eventsource.sync_lag.is_sync_ready",
            {
                ATTR_TENANT_ID: str(self._tenant_id) if self._tenant_id else None,
                ATTR_SYNC_THRESHOLD: self._config.cutover_max_lag_events,
                ATTR_IS_SYNC_READY: self.is_converged(),
            },
        ):
            ready = self.is_converged()

            if ready:
                logger.info(
                    f"Sync ready for cutover: lag={self._current_lag.events if self._current_lag else 0} events, "
                    f"threshold={self._config.cutover_max_lag_events}"
                    + (f" for tenant {self._tenant_id}" if self._tenant_id else "")
                )

            return ready

    def is_fully_converged(self) -> bool:
        """
        Check if stores are fully synchronized (zero lag).

        Returns:
            True if current lag is exactly zero.

        Example:
            >>> if tracker.is_fully_converged():
            ...     print("Perfect sync achieved!")
        """
        if self._current_lag is None:
            return False
        return self._current_lag.is_converged

    # =========================================================================
    # Lag Statistics
    # =========================================================================

    def get_lag_stats(self) -> LagStats:
        """
        Get aggregate statistics about sync lag over the sample window.

        Calculates average, max, and min lag from the sample history.
        Also determines if lag is trending downward (converging).

        Returns:
            LagStats with summary metrics.

        Example:
            >>> stats = tracker.get_lag_stats()
            >>> print(f"Average lag: {stats.average_lag:.1f} events")
            >>> print(f"Max lag: {stats.max_lag} events")
            >>> if stats.is_converging:
            ...     print("Lag is decreasing!")
        """
        if not self._lag_samples:
            return LagStats(
                current_lag=0,
                average_lag=0.0,
                max_lag=0,
                min_lag=0,
                sample_count=0,
                first_sample_at=None,
                last_sample_at=None,
                is_converging=False,
            )

        lags = [s.lag.events for s in self._lag_samples]
        current = lags[-1] if lags else 0
        avg = sum(lags) / len(lags)
        max_lag = max(lags)
        min_lag = min(lags)

        # Determine if converging (recent samples are lower than earlier ones)
        is_converging = self._is_converging()

        return LagStats(
            current_lag=current,
            average_lag=avg,
            max_lag=max_lag,
            min_lag=min_lag,
            sample_count=len(self._lag_samples),
            first_sample_at=self._lag_samples[0].sampled_at,
            last_sample_at=self._lag_samples[-1].sampled_at,
            is_converging=is_converging,
        )

    def _is_converging(self) -> bool:
        """
        Determine if lag is trending downward.

        Compares the average of the first half of samples to the
        average of the second half. If the second half is lower,
        lag is considered to be converging.

        Returns:
            True if lag is decreasing over time.
        """
        if len(self._lag_samples) < 4:
            return False

        samples = list(self._lag_samples)
        mid = len(samples) // 2

        first_half = [s.lag.events for s in samples[:mid]]
        second_half = [s.lag.events for s in samples[mid:]]

        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)

        return second_avg < first_avg

    def get_sample_history(self) -> list[SyncLag]:
        """
        Get the history of lag samples.

        Returns:
            List of SyncLag measurements in chronological order.

        Example:
            >>> history = tracker.get_sample_history()
            >>> for lag in history[-5:]:  # Last 5 samples
            ...     print(f"{lag.timestamp}: {lag.events} events")
        """
        return [s.lag for s in self._lag_samples]

    def clear_history(self) -> int:
        """
        Clear the lag sample history.

        Useful when starting a new measurement period.

        Returns:
            Number of samples cleared.

        Example:
            >>> cleared = tracker.clear_history()
            >>> print(f"Cleared {cleared} samples")
        """
        count = len(self._lag_samples)
        self._lag_samples.clear()
        self._current_lag = None
        return count

    # =========================================================================
    # Manual Lag Updates
    # =========================================================================

    def record_lag(self, lag: SyncLag) -> None:
        """
        Manually record a lag measurement.

        Allows external components (like DualWriteInterceptor) to
        provide lag updates without querying stores.

        Args:
            lag: The lag measurement to record.

        Example:
            >>> # DualWriteInterceptor can update lag after writes
            >>> lag = SyncLag(
            ...     events=5,
            ...     source_position=1000,
            ...     target_position=995,
            ...     timestamp=datetime.now(UTC),
            ... )
            >>> tracker.record_lag(lag)
        """
        self._current_lag = lag
        self._add_sample(lag)

        logger.debug(
            f"Lag recorded: {lag.events} events "
            f"(source={lag.source_position}, target={lag.target_position})"
            + (f" for tenant {self._tenant_id}" if self._tenant_id else "")
        )


__all__ = [
    "SyncLagTracker",
    "LagSample",
    "LagStats",
]
