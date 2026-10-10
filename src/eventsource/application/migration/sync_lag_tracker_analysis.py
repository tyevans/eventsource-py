"""
Sync lag convergence detection, statistics analysis, and history tracking.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.sync_lag_tracker_types import (
    ATTR_IS_SYNC_READY,
    ATTR_SYNC_THRESHOLD,
    LagSample,
    LagStats,
)
from eventsource.observability import ATTR_TENANT_ID

if TYPE_CHECKING:
    from eventsource.observability import Tracer
    from eventsource.ports.migration.models import MigrationConfig, SyncLag

logger = logging.getLogger(__name__)


class SyncLagAnalysisMixin:
    """Mixin providing convergence detection, statistics, and history tracking."""

    _config: MigrationConfig
    _current_lag: SyncLag | None
    _tracer: Tracer
    _tenant_id: UUID | None
    _lag_samples: deque[LagSample]

    def _add_sample(self, lag: SyncLag) -> None:
        raise NotImplementedError

    def is_converged(self, max_lag: int | None = None) -> bool:
        """
        Check if stores are synchronized within the specified threshold.

        A bounded count NEVER converges: it is a lower bound standing for
        an unknown larger backlog, and a lower bound cannot satisfy a
        threshold.

        `max_lag` may only TIGHTEN the configured threshold, for the same
        reason -- a looser override could otherwise be satisfied by a
        bounded count.

        Args:
            max_lag: Maximum acceptable lag in events. If None, uses
                the configured cutover_max_lag_events threshold.

        Returns:
            True if current lag is within the threshold.

        Raises:
            ValueError: If max_lag exceeds the configured threshold.

        Example:
            >>> if tracker.is_converged():
            ...     print("Stores are synchronized!")
            >>>
            >>> # Use a tighter threshold
            >>> if tracker.is_converged(max_lag=10):
            ...     print("Within 10 events!")
        """
        threshold = self._config.cutover_max_lag_events
        if max_lag is not None:
            if max_lag > threshold:
                raise ValueError(
                    f"max_lag={max_lag} exceeds the configured "
                    f"cutover_max_lag_events={threshold}; the count behind is "
                    f"bounded at {threshold + 1}, so a looser threshold cannot "
                    f"be evaluated honestly"
                )
            threshold = max_lag

        if self._current_lag is None:
            return False

        return self._current_lag.is_within_threshold(threshold)

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
            ...     source_position=source_pos,
            ...     target_position=target_pos,
            ...     timestamp=datetime.now(UTC),
            ... )
            >>> tracker.record_lag(lag)
        """
        self._current_lag = lag
        self._add_sample(lag)

        logger.debug(
            f"Lag recorded: {lag.events} events behind"
            + (f" for tenant {self._tenant_id}" if self._tenant_id else "")
        )


__all__ = ["SyncLagAnalysisMixin"]
