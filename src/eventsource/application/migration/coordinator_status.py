"""Status streaming, status building, and observer queue management for MigrationCoordinator."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.ports.migration.models import (
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
    MigrationStatus,
)

if TYPE_CHECKING:
    from eventsource.application.migration.status_streamer import StatusStreamer
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.migration.repositories import (
        MigrationAuditLogRepository,
        MigrationRepository,
    )

logger = logging.getLogger(__name__)


class CoordinatorStatusMixin:
    """Mixin providing status streaming, progress calculations, and queue management."""

    _tracer: Tracer
    _enable_tracing: bool
    _audit_log_repo: MigrationAuditLogRepository | None
    _lag_trackers: dict[UUID, SyncLagTracker]
    _status_queues: dict[UUID, list[asyncio.Queue[UUID]]]
    _migration_repo: MigrationRepository

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        """Write an audit entry if an audit log repository was configured.

        No-ops silently when `audit_log_repo` was omitted -- auditing is
        an optional capability, not a required dependency. A failed
        write is logged, not raised: an audit trail gap must not fail
        the migration operation that produced it.
        """
        if self._audit_log_repo is None:
            return
        try:
            await self._audit_log_repo.record(entry)
        except Exception:
            logger.exception(
                "Failed to record audit entry %s for migration %s",
                entry.event_type.value,
                entry.migration_id,
            )

    def create_status_streamer(
        self,
        migration_id: UUID,
    ) -> StatusStreamer:
        """
        Create a StatusStreamer for real-time migration status streaming.

        Creates a new StatusStreamer instance that can be used to stream
        MigrationStatus updates as an async iterator. Multiple streamers
        can be created for the same migration to support multiple subscribers.

        Args:
            migration_id: UUID of the migration to stream status for

        Returns:
            StatusStreamer instance for streaming status updates
        """
        from eventsource.application.migration.status_streamer import StatusStreamer

        return StatusStreamer(
            coordinator=self,  # type: ignore[arg-type]
            migration_id=migration_id,
            enable_tracing=self._enable_tracing,
        )

    async def stream_status(
        self,
        migration_id: UUID,
        *,
        update_interval: float = 1.0,
        include_initial: bool = True,
    ) -> AsyncIterator[MigrationStatus]:
        """
        Stream migration status updates as an async iterator.

        Convenience method that creates a StatusStreamer and yields status
        updates. For more control over streaming configuration, use
        create_status_streamer() instead.

        Args:
            migration_id: UUID of the migration to stream
            update_interval: Seconds between forced status checks (default 1.0)
            include_initial: Whether to yield the initial status immediately

        Yields:
            MigrationStatus: Current migration status on each update

        Raises:
            MigrationNotFoundError: If migration not found
            ValueError: If update_interval is <= 0
        """
        with self._tracer.span(
            "eventsource.coordinator.stream_status",
            {
                ATTR_MIGRATION_ID: str(migration_id),
                "update_interval": update_interval,
            },
        ):
            streamer = self.create_status_streamer(migration_id)

            try:
                async for status in streamer.stream_status(
                    update_interval=update_interval,
                    include_initial=include_initial,
                ):
                    yield status
            finally:
                await streamer.close()

    def _build_status(self, migration: Migration) -> MigrationStatus:
        """
        Build MigrationStatus from Migration.

        Calculates derived metrics like rate and estimated completion.
        In dual-write phase, includes sync lag from the lag tracker.

        Args:
            migration: Migration instance

        Returns:
            MigrationStatus with current progress and metrics
        """
        phase_started_at = None
        if migration.phase == MigrationPhase.BULK_COPY:
            phase_started_at = migration.bulk_copy_started_at
        elif migration.phase == MigrationPhase.DUAL_WRITE:
            phase_started_at = migration.dual_write_started_at
        elif migration.phase == MigrationPhase.CUTOVER:
            phase_started_at = migration.cutover_started_at

        # Calculate rate
        rate = 0.0
        if migration.started_at and migration.events_copied > 0:
            elapsed = (datetime.now(UTC) - migration.started_at).total_seconds()
            if elapsed > 0:
                rate = migration.events_copied / elapsed

        # Estimate completion
        estimated_completion = None
        if rate > 0 and migration.events_remaining > 0:
            remaining_seconds = migration.events_remaining / rate
            estimated_completion = datetime.now(UTC) + timedelta(seconds=remaining_seconds)

        # Get sync lag from tracker if available (P2-005)
        sync_lag_events = 0
        sync_lag_ms = 0.0
        lag_tracker = self._lag_trackers.get(migration.id)
        if lag_tracker and lag_tracker.current_lag:
            sync_lag_events = lag_tracker.current_lag.events
            sync_lag_ms = lag_tracker.current_lag.lag_ms

        return MigrationStatus(
            migration_id=migration.id,
            tenant_id=migration.tenant_id,
            phase=migration.phase,
            progress_percent=migration.progress_percent,
            events_total=migration.events_total,
            events_copied=migration.events_copied,
            events_remaining=migration.events_remaining,
            sync_lag_events=sync_lag_events,
            sync_lag_ms=sync_lag_ms,
            error_count=migration.error_count,
            started_at=migration.started_at,
            phase_started_at=phase_started_at,
            estimated_completion=estimated_completion,
            current_rate_events_per_sec=rate,
            is_paused=migration.is_paused,
        )

    def _calculate_duration(self, migration: Migration | None) -> float:
        """
        Calculate migration duration in seconds.

        Args:
            migration: Migration instance or None

        Returns:
            Duration in seconds, or 0.0 if migration is None or not started
        """
        if migration is None or migration.started_at is None:
            return 0.0
        end = migration.completed_at or datetime.now(UTC)
        return (end - migration.started_at).total_seconds()

    async def _notify_status_update(self, migration_id: UUID) -> None:
        """
        Notify observers of status update.

        Sends migration_id to all registered queues for this migration.

        Args:
            migration_id: UUID of the migration that was updated
        """
        queues = self._status_queues.get(migration_id, [])
        for queue in queues:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(migration_id)

    def _cleanup_status_queues(self, migration_id: UUID) -> None:
        """
        Clean up status queues for a completed/aborted migration.

        Args:
            migration_id: UUID of the migration to clean up
        """
        self._status_queues.pop(migration_id, None)

    def register_status_queue(self, migration_id: UUID, queue: asyncio.Queue[UUID]) -> None:
        """
        Register a queue for status updates.

        The queue will receive migration_id whenever the migration
        status is updated. Used for implementing status streaming.

        Args:
            migration_id: UUID of the migration to observe
            queue: Queue to receive update notifications
        """
        if migration_id not in self._status_queues:
            self._status_queues[migration_id] = []
        self._status_queues[migration_id].append(queue)

    def unregister_status_queue(self, migration_id: UUID, queue: asyncio.Queue[UUID]) -> None:
        """
        Unregister a queue from status updates.

        Args:
            migration_id: UUID of the migration
            queue: Queue to remove
        """
        queues = self._status_queues.get(migration_id, [])
        if queue in queues:
            queues.remove(queue)


__all__ = [
    "CoordinatorStatusMixin",
]
