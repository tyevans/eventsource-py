"""SyncLagTracker - Monitor synchronization lag between source and target stores."""

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
from eventsource.observability import ATTR_TENANT_ID, Tracer, create_tracer
from eventsource.observability.attributes import ATTR_MIGRATION_SYNC_LAG_EVENTS
from eventsource.ports import FeedReadOptions, FullEventStore, Position
from eventsource.ports.migration.models import MigrationConfig, SyncLag

logger = logging.getLogger(__name__)


class SyncLagTracker(SyncLagAnalysisMixin):
    """Tracks synchronization lag between source and target stores during migration."""

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
        """Initialize the sync lag tracker."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store
        self._config = config or MigrationConfig()
        self._tenant_id = tenant_id
        self._migration_id = migration_id
        self._max_sample_history = max_sample_history
        self._lag_samples: deque[LagSample] = deque(maxlen=max_sample_history)
        self._current_lag: SyncLag | None = None

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

    async def calculate_lag(self, *, since: Position | None = None) -> SyncLag:
        """Count source events not yet copied since `since`, bounded by the sync threshold."""
        with self._tracer.span(
            "eventsource.sync_lag.calculate_lag",
            {
                ATTR_TENANT_ID: str(self._tenant_id) if self._tenant_id else None,
                ATTR_SYNC_THRESHOLD: self._config.cutover_max_lag_events,
            },
        ) as span:
            threshold = self._config.cutover_max_lag_events
            lag_events = 0
            async for _ in self._source.read_all(
                since,
                FeedReadOptions(tenant_id=self._tenant_id, limit=threshold + 1),
            ):
                lag_events += 1

            if span is not None:
                span.set_attribute(ATTR_MIGRATION_SYNC_LAG_EVENTS, lag_events)

            count_is_bounded = lag_events > threshold
            source_position = await self._source.current_position()
            target_position = await self._target.current_position()

            lag = SyncLag(
                events=lag_events,
                source_position=source_position,
                target_position=target_position,
                timestamp=datetime.now(UTC),
                count_is_bounded=count_is_bounded,
            )
            self._current_lag = lag
            self._add_sample(lag)

            if self._migration_id is not None:
                get_migration_metrics(
                    str(self._migration_id),
                    str(self._tenant_id) if self._tenant_id else "unknown",
                ).record_sync_lag(lag_events)

            logger.debug(
                f"Sync lag calculated: {lag_events} events behind"
                + (" (bounded)" if count_is_bounded else "")
                + (f" for tenant {self._tenant_id}" if self._tenant_id else "")
            )
            return lag

    def _add_sample(self, lag: SyncLag) -> None:
        """Add a lag measurement to the sample history."""
        self._lag_samples.append(LagSample(lag=lag, sampled_at=datetime.now(UTC)))


__all__ = ["LagSample", "LagStats", "SyncLagTracker"]
