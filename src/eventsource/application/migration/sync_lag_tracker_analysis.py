"""Sync lag convergence detection, statistics analysis, and history tracking."""

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
        """Check if stores are synchronized within the specified threshold."""
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
        return (
            False if self._current_lag is None else self._current_lag.is_within_threshold(threshold)
        )

    def is_sync_ready(self) -> bool:
        """Check if synchronization lag is within cutover_max_lag_events threshold."""
        attrs = {
            ATTR_TENANT_ID: str(self._tenant_id) if self._tenant_id else None,
            ATTR_SYNC_THRESHOLD: self._config.cutover_max_lag_events,
            ATTR_IS_SYNC_READY: self.is_converged(),
        }
        with self._tracer.span("eventsource.sync_lag.is_sync_ready", attrs):
            ready = self.is_converged()
            if ready:
                suffix = f" for tenant {self._tenant_id}" if self._tenant_id else ""
                lag_events = self._current_lag.events if self._current_lag else 0
                logger.info(
                    f"Sync ready for cutover: lag={lag_events} events, "
                    f"threshold={self._config.cutover_max_lag_events}{suffix}"
                )
            return ready

    def is_fully_converged(self) -> bool:
        """Check if stores are fully synchronized (zero lag)."""
        return False if self._current_lag is None else self._current_lag.is_converged

    def get_lag_stats(self) -> LagStats:
        """Get aggregate statistics about sync lag over the sample window."""
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
        return LagStats(
            current_lag=lags[-1] if lags else 0,
            average_lag=sum(lags) / len(lags),
            max_lag=max(lags),
            min_lag=min(lags),
            sample_count=len(self._lag_samples),
            first_sample_at=self._lag_samples[0].sampled_at,
            last_sample_at=self._lag_samples[-1].sampled_at,
            is_converging=self._is_converging(),
        )

    def _is_converging(self) -> bool:
        """Determine if lag is trending downward across the sample window."""
        if len(self._lag_samples) < 4:
            return False
        samples = list(self._lag_samples)
        mid = len(samples) // 2
        first_half = [s.lag.events for s in samples[:mid]]
        second_half = [s.lag.events for s in samples[mid:]]
        return (sum(second_half) / len(second_half)) < (sum(first_half) / len(first_half))

    def get_sample_history(self) -> list[SyncLag]:
        """Get the history of lag samples in chronological order."""
        return [s.lag for s in self._lag_samples]

    def clear_history(self) -> int:
        """Clear the lag sample history and return the number of samples cleared."""
        count = len(self._lag_samples)
        self._lag_samples.clear()
        self._current_lag = None
        return count

    def record_lag(self, lag: SyncLag) -> None:
        """Manually record a lag measurement."""
        self._current_lag = lag
        self._add_sample(lag)
        suffix = f" for tenant {self._tenant_id}" if self._tenant_id else ""
        logger.debug(f"Lag recorded: {lag.events} events behind{suffix}")


__all__ = ["SyncLagAnalysisMixin"]
