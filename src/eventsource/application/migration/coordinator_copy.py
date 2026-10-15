"""Bulk copy and dual-write operations mixin for MigrationCoordinator."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.bulk_copier import BulkCopier, BulkCopyProgress
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.observability import Tracer
from eventsource.observability import attributes as _attr
from eventsource.ports import FullEventStore
from eventsource.ports.migration.models import (
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
    TenantMigrationState,
)

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.ports.migration.repositories import (
        MigrationRepository,
        TenantRoutingRepository,
    )

logger = logging.getLogger(__name__)


class CoordinatorCopyMixin:
    """Mixin providing bulk copy, resync pass, and dual-write setup for MigrationCoordinator."""

    _tracer: Tracer
    _source_store: FullEventStore
    _target_stores: dict[UUID, FullEventStore]
    _migration_repo: MigrationRepository
    _routing_repo: TenantRoutingRepository
    _router: TenantStoreRouter
    _position_mapper: PositionMapper | None
    _enable_tracing: bool
    _active_copiers: dict[UUID, BulkCopier]
    _active_tasks: dict[UUID, asyncio.Task[None]]
    _interceptors: dict[UUID, DualWriteInterceptor]
    _lag_trackers: dict[UUID, SyncLagTracker]
    _MAX_CATCHUP_ROUNDS = 10

    if TYPE_CHECKING:

        async def _fail_migration(self, migration: Migration, error: str) -> None: ...
        async def _record_audit(self, entry: MigrationAuditEntry) -> None: ...
        async def _notify_status_update(self, migration_id: UUID) -> None: ...

    def _phase_span_attrs(self, m: Migration, phase: MigrationPhase) -> dict[str, str]:
        return {
            _attr.ATTR_MIGRATION_ID: str(m.id),
            _attr.ATTR_MIGRATION_TENANT_ID: str(m.tenant_id),
            _attr.ATTR_MIGRATION_PHASE: phase.value,
        }

    async def _run_bulk_copy(self, migration: Migration, target_store: FullEventStore) -> None:
        attrs = self._phase_span_attrs(migration, MigrationPhase.BULK_COPY)
        with self._tracer.span("eventsource.coordinator.run_bulk_copy", attrs):
            interceptor = self._install_interceptor(migration, target_store)
            copier = self._build_copier(migration, target_store)
            self._active_copiers[migration.id] = copier
            phase_start = time.monotonic()
            try:
                completed = await self._run_copy_pass(copier, migration)
                rounds = 0
                while completed:
                    if (current := await self._migration_repo.get(migration.id)) is None:
                        raise MigrationNotFoundError(migration.id)
                    remaining = interceptor.mark_copy_pass_complete(current.last_source_position)
                    if remaining == 0:
                        break
                    if rounds >= self._MAX_CATCHUP_ROUNDS:
                        msg = (
                            "Migration %s: %d mirror failures remain unabsorbed after %d catch-up "
                            "rounds; the lag anchor stays clamped at the checkpoint until another "
                            "copy pass absorbs them"
                        )
                        logger.warning(msg, migration.id, remaining, rounds)
                        break
                    rounds += 1
                    msg = "Migration %s: catch-up round %d to absorb %d mirror failure(s)"
                    logger.info(msg, migration.id, rounds, remaining)
                    completed = await self._run_copy_pass(copier, current)
                if completed:
                    await self._transition_to_dual_write(migration, target_store)
            except asyncio.CancelledError:
                logger.info("Bulk copy cancelled for migration %s", migration.id)
                raise
            except Exception as e:
                logger.error("Bulk copy failed for migration %s: %s", migration.id, e)
                await self._fail_migration(migration, str(e))
            finally:
                metrics = get_migration_metrics(str(migration.id), str(migration.tenant_id))
                metrics.record_phase_duration("bulk_copy", time.monotonic() - phase_start)
                self._active_copiers.pop(migration.id, None)
                self._active_tasks.pop(migration.id, None)

    def _build_copier(self, m: Migration, target: FullEventStore | None = None) -> BulkCopier:
        store = target or self._target_stores.get(m.id)
        if store is None:
            msg = (
                "No target store registered for migration; the coordinator that started it holds "
                "that registry in memory, so a restarted coordinator must re-register before copying."
            )
            raise MigrationError(msg, migration_id=m.id)
        mapper = self._position_mapper if m.config.position_mapping_enabled else None
        src, repo, tr = self._source_store, self._migration_repo, self._enable_tracing
        return BulkCopier(src, store, repo, position_mapper=mapper, enable_tracing=tr)

    async def run_resync_pass(self, migration_id: UUID) -> int:
        """Run one bounded catch-up copy pass while in DUAL_WRITE."""
        span_attrs = {_attr.ATTR_MIGRATION_ID: str(migration_id)}
        with self._tracer.span("eventsource.coordinator.run_resync_pass", span_attrs):
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if migration.phase != MigrationPhase.DUAL_WRITE:
                msg = f"Cannot run a resync pass: migration is in {migration.phase.value} phase"
                allowed = [MigrationPhase.DUAL_WRITE]
                raise MigrationStateError(
                    msg, migration_id, migration.phase, allowed, "run_resync_pass"
                )
            if migration_id in self._active_copiers:
                msg = "A copy pass is already running for this migration"
                raise MigrationError(msg, migration_id=migration_id)
            self._active_copiers[migration_id] = (copier := self._build_copier(migration))
            try:
                completed = await self._run_copy_pass(copier, migration)
            finally:
                self._active_copiers.pop(migration_id, None)
            if not completed:
                msg = (
                    "Resync pass did not run to completion; the lag anchor stays clamped "
                    "(an incomplete pass attests nothing)"
                )
                raise MigrationError(msg, migration_id=migration_id)
            if (current := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if (interceptor := self._interceptors.get(migration_id)) is None:
                msg = (
                    "Resync pass complete for migration %s: no interceptor registered "
                    "(coordinator restarted since dual-write began), so no failures could be "
                    "absorbed; the pass advanced the persisted checkpoint"
                )
                logger.info(msg, migration_id)
                return 0
            absorbed = interceptor.mark_copy_pass_complete(current.last_source_position)
            msg = "Resync pass complete for migration %s: %d mirror failures absorbed"
            logger.info(msg, migration_id, absorbed)
            return absorbed

    async def _run_copy_pass(self, copier: BulkCopier, migration: Migration) -> bool:
        last_progress: BulkCopyProgress | None = None
        async for progress in copier.run(migration):
            last_progress = progress
            await self._notify_status_update(migration.id)
        if last_progress and last_progress.is_complete:
            msg = "Bulk copy pass complete for migration %s: %d events"
            logger.info(msg, migration.id, last_progress.events_copied)
            return True
        return False

    def _install_interceptor(self, m: Migration, target: FullEventStore) -> DualWriteInterceptor:
        if (interceptor := self._interceptors.get(m.id)) is not None:
            return interceptor
        src, tr = self._source_store, self._enable_tracing
        interceptor = DualWriteInterceptor(
            src, target, m.tenant_id, migration_id=m.id, enable_tracing=tr
        )
        self._router.set_dual_write_interceptor(m.tenant_id, interceptor)
        self._interceptors[m.id] = interceptor
        return interceptor

    async def _transition_to_dual_write(self, m: Migration, target: FullEventStore) -> None:
        attrs = self._phase_span_attrs(m, MigrationPhase.DUAL_WRITE)
        with self._tracer.span("eventsource.coordinator.transition_to_dual_write", attrs):
            self._install_interceptor(m, target)
            src, tr = self._source_store, self._enable_tracing
            self._lag_trackers[m.id] = SyncLagTracker(
                src, target, m.config, m.tenant_id, migration_id=m.id, enable_tracing=tr
            )
            await self._migration_repo.update_phase(m.id, MigrationPhase.DUAL_WRITE)
            state = TenantMigrationState.DUAL_WRITE
            await self._routing_repo.set_migration_state(m.tenant_id, state, m.id)
            msg = "Migration %s transitioned to dual-write phase for tenant %s"
            logger.info(msg, m.id, m.tenant_id)
            old_p, new_p = MigrationPhase.BULK_COPY, MigrationPhase.DUAL_WRITE
            entry = MigrationAuditEntry.phase_change(m.id, old_p, new_p, datetime.now(UTC))
            await self._record_audit(entry)
            await self._notify_status_update(m.id)


__all__ = ["CoordinatorCopyMixin"]
