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
        """Write an audit entry if an audit log repository was configured."""
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

    def create_status_streamer(self, migration_id: UUID) -> StatusStreamer:
        """Create a StatusStreamer for real-time migration status streaming."""
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
        """Stream migration status updates as an async iterator."""
        attrs = {ATTR_MIGRATION_ID: str(migration_id), "update_interval": update_interval}
        with self._tracer.span("eventsource.coordinator.stream_status", attrs):
            streamer = self.create_status_streamer(migration_id)
            try:
                async for status in streamer.stream_status(
                    update_interval=update_interval, include_initial=include_initial
                ):
                    yield status
            finally:
                await streamer.close()

    def _build_status(self, migration: Migration) -> MigrationStatus:
        """Build MigrationStatus from Migration with derived rate and lag metrics."""
        phase_map = {
            MigrationPhase.BULK_COPY: migration.bulk_copy_started_at,
            MigrationPhase.DUAL_WRITE: migration.dual_write_started_at,
            MigrationPhase.CUTOVER: migration.cutover_started_at,
        }
        phase_started_at = phase_map.get(migration.phase)

        rate = 0.0
        if migration.started_at and migration.events_copied > 0:
            elapsed = (datetime.now(UTC) - migration.started_at).total_seconds()
            if elapsed > 0:
                rate = migration.events_copied / elapsed

        estimated_completion = None
        if rate > 0 and migration.events_remaining > 0:
            estimated_completion = datetime.now(UTC) + timedelta(
                seconds=migration.events_remaining / rate
            )

        lag_tracker = self._lag_trackers.get(migration.id)
        lag = lag_tracker.current_lag if lag_tracker else None
        return MigrationStatus(
            migration_id=migration.id,
            tenant_id=migration.tenant_id,
            phase=migration.phase,
            progress_percent=migration.progress_percent,
            events_total=migration.events_total,
            events_copied=migration.events_copied,
            events_remaining=migration.events_remaining,
            sync_lag_events=lag.events if lag else 0,
            sync_lag_ms=lag.lag_ms if lag else 0.0,
            error_count=migration.error_count,
            started_at=migration.started_at,
            phase_started_at=phase_started_at,
            estimated_completion=estimated_completion,
            current_rate_events_per_sec=rate,
            is_paused=migration.is_paused,
        )

    def _calculate_duration(self, migration: Migration | None) -> float:
        """Calculate migration duration in seconds, or 0.0 if None or not started."""
        if migration is None or migration.started_at is None:
            return 0.0
        return (
            (migration.completed_at or datetime.now(UTC)) - migration.started_at
        ).total_seconds()

    async def _notify_status_update(self, migration_id: UUID) -> None:
        """Notify registered observer queues of a status update."""
        for queue in self._status_queues.get(migration_id, []):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(migration_id)

    def _cleanup_status_queues(self, migration_id: UUID) -> None:
        """Clean up status queues for a completed/aborted migration."""
        self._status_queues.pop(migration_id, None)

    def register_status_queue(self, migration_id: UUID, queue: asyncio.Queue[UUID]) -> None:
        """Register a queue for status updates."""
        self._status_queues.setdefault(migration_id, []).append(queue)

    def unregister_status_queue(self, migration_id: UUID, queue: asyncio.Queue[UUID]) -> None:
        """Unregister a queue from status updates."""
        queues = self._status_queues.get(migration_id, [])
        if queue in queues:
            queues.remove(queue)


__all__ = ["CoordinatorStatusMixin"]
