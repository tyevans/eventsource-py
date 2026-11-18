"""Cutover triggering, rollback, and sync lag management for MigrationCoordinator."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from functools import partial
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.coordinator_cutover_completion import (
    CoordinatorCutoverCompletionMixin,
)
from eventsource.application.migration.cutover import CutoverManager

from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError, MigrationStateError  # fmt: skip  # isort: skip
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID, ATTR_MIGRATION_PHASE
from eventsource.ports import Position
from eventsource.ports.migration import models as m

if TYPE_CHECKING:
    from eventsource.application.migration.dual_write import DualWriteInterceptor
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import MigrationRepository, TenantRoutingRepository  # fmt: skip  # isort: skip

logger = logging.getLogger(__name__)


class CoordinatorCutoverMixin(CoordinatorCutoverCompletionMixin):
    """Mixin providing cutover triggering, rollback, and lag checks."""

    _tracer: Tracer
    _migration_repo: MigrationRepository
    _routing_repo: TenantRoutingRepository
    _router: TenantStoreRouter
    _lag_trackers: dict[UUID, SyncLagTracker]
    _interceptors: dict[UUID, DualWriteInterceptor]
    _cutover_manager: CutoverManager | None
    _lock_manager: DistributedLock | None
    _enable_tracing: bool

    if TYPE_CHECKING:

        async def _record_audit(self, entry: m.MigrationAuditEntry) -> None: ...
        async def _notify_status_update(self, migration_id: UUID) -> None: ...

    async def _audit_cutover(self, migration_id: UUID, event_type: m.AuditEventType, new_phase: m.MigrationPhase, details: dict[str, object] | None = None, *, old_phase: m.MigrationPhase = m.MigrationPhase.CUTOVER) -> None:  # fmt: skip
        await self._record_audit(m.MigrationAuditEntry(None, migration_id, event_type, old_phase, new_phase, details, None, datetime.now(UTC)))  # fmt: skip

    async def trigger_cutover(self, migration_id: UUID, *, timeout_ms: float | None = None) -> m.CutoverResult:  # fmt: skip
        """Trigger cutover for a migration in dual-write phase; rolls back on failure."""
        span_attrs = {ATTR_MIGRATION_ID: str(migration_id), ATTR_MIGRATION_PHASE: m.MigrationPhase.CUTOVER.value}  # fmt: skip
        with self._tracer.span("eventsource.coordinator.trigger_cutover", span_attrs):
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if (phase := migration.phase) != m.MigrationPhase.DUAL_WRITE:
                msg = f"Cannot trigger cutover: migration is in {phase.value} phase"
                raise MigrationStateError(msg, migration_id, phase, [m.MigrationPhase.DUAL_WRITE], "trigger_cutover")  # fmt: skip
            if (lag_tracker := self._lag_trackers.get(migration_id)) is None:
                raise MigrationError("No sync lag tracker found for migration", migration_id=migration_id)  # fmt: skip
            mgr = self._get_cutover_manager()
            await self._migration_repo.update_phase(migration_id, m.MigrationPhase.CUTOVER)
            logger.info("Starting cutover for %s, tenant %s", migration_id, migration.tenant_id)
            await self._audit_cutover(migration_id, m.AuditEventType.CUTOVER_INITIATED, m.MigrationPhase.CUTOVER, old_phase=m.MigrationPhase.DUAL_WRITE)  # fmt: skip
            still_in_cutover = partial(self._is_still_in_cutover, migration_id)
            result = await mgr.execute_cutover(migration_id, migration.tenant_id, lag_tracker, migration.target_store_id, config=migration.config, timeout_ms=timeout_ms, since=self._lag_anchor(migration), still_in_cutover=still_in_cutover)  # fmt: skip
            if result.success:
                return await self._complete_cutover(migration, result)
            self._record_cutover_duration(migration, result, success=False)
            if await self._was_aborted(migration_id):
                logger.warning("Cutover for %s stopped: migration was aborted", migration_id)
                return result
            await self._rollback_cutover(migration, result)
            return result

    async def _is_still_in_cutover(self, migration_id: UUID) -> bool:
        """Re-read the phase; cutover must not switch routing once the migration left CUTOVER."""
        current = await self._migration_repo.get(migration_id)
        return current is not None and current.phase == m.MigrationPhase.CUTOVER

    async def _was_aborted(self, migration_id: UUID) -> bool:
        current = await self._migration_repo.get(migration_id)
        return current is not None and current.phase == m.MigrationPhase.ABORTED

    async def _rollback_cutover(self, migration: m.Migration, result: m.CutoverResult) -> None:
        await self._migration_repo.update_phase(migration.id, m.MigrationPhase.DUAL_WRITE)
        if msg := result.error_message:
            await self._migration_repo.record_error(migration.id, f"Cutover failed: {msg}")
        logger.warning("Cutover rolled back for %s: %s", migration.id, result.error_message)
        await self._audit_cutover(migration.id, m.AuditEventType.CUTOVER_ROLLED_BACK, m.MigrationPhase.DUAL_WRITE, {"error_message": result.error_message})  # fmt: skip
        await self._notify_status_update(migration.id)

    def _get_cutover_manager(self) -> CutoverManager:
        if self._cutover_manager is None:
            if (lock := self._lock_manager) is None:
                raise MigrationError("Cannot perform cutover: lock_manager not provided to coordinator. Provide a lock manager (e.g. PostgreSQLLockManager) when creating the coordinator to enable cutover operations.")  # fmt: skip
            self._cutover_manager = CutoverManager(lock, self._router, self._routing_repo, enable_tracing=self._enable_tracing)  # fmt: skip
        return self._cutover_manager

    def _lag_anchor(self, migration: m.Migration) -> Position | None:
        if (interceptor := self._interceptors.get(migration.id)) is None:
            return migration.last_source_position
        return interceptor.safe_lag_anchor(migration.last_source_position)

    async def get_sync_lag(self, migration_id: UUID) -> m.SyncLag | None:
        if (migration := await self._migration_repo.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        if (lag_tracker := self._lag_trackers.get(migration_id)) is None:
            return None
        return await lag_tracker.calculate_lag(since=self._lag_anchor(migration))

    async def is_cutover_ready(self, migration_id: UUID) -> tuple[bool, str | None]:
        if (migration := await self._migration_repo.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        if migration.phase != m.MigrationPhase.DUAL_WRITE:
            return False, f"Migration is in {migration.phase.value} phase, expected DUAL_WRITE"
        if (lag_tracker := self._lag_trackers.get(migration_id)) is None:
            return False, "No sync lag tracker found for migration"
        await lag_tracker.calculate_lag(since=self._lag_anchor(migration))
        if lag_tracker.is_sync_ready():
            return True, None
        lag = lag_tracker.current_lag.events if lag_tracker.current_lag else "unknown"
        return False, f"Sync lag too high: {lag} events (max: {migration.config.cutover_max_lag_events})"  # fmt: skip


__all__ = ["CoordinatorCutoverMixin"]
