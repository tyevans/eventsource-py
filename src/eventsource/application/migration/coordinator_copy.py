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
from eventsource.application.migration.exceptions import (  # fmt: skip
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.observability import Tracer
from eventsource.observability import attributes as _attr
from eventsource.ports import FullEventStore
from eventsource.ports.migration import (  # fmt: skip
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
    MigrationRepository,
    TenantMigrationState,
    TenantRoutingRepository,
)

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.application.migration.router import TenantStoreRouter

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
        return {_attr.ATTR_MIGRATION_ID: str(m.id), _attr.ATTR_MIGRATION_TENANT_ID: str(m.tenant_id), _attr.ATTR_MIGRATION_PHASE: phase.value}  # fmt: skip

    async def _run_bulk_copy(self, m: Migration, target_store: FullEventStore) -> None:
        with self._tracer.span("eventsource.coordinator.run_bulk_copy", self._phase_span_attrs(m, MigrationPhase.BULK_COPY)):  # fmt: skip
            interceptor = self._install_interceptor(m, target_store)
            self._active_copiers[m.id] = (copier := self._build_copier(m, target_store))
            phase_start = time.monotonic()
            try:
                completed, rounds = await self._run_copy_pass(copier, m), 0
                while completed:
                    if (current := await self._migration_repo.get(m.id)) is None:
                        raise MigrationNotFoundError(m.id)
                    if (rem := interceptor.mark_copy_pass_complete(current.last_source_position)) == 0 or rounds >= self._MAX_CATCHUP_ROUNDS:  # fmt: skip
                        if rem:
                            logger.warning("Migration %s: %d mirror failures remain unabsorbed after %d rounds", m.id, rem, rounds)  # fmt: skip
                        break
                    rounds += 1
                    logger.info("Migration %s: catch-up round %d (%d failures)", m.id, rounds, rem)
                    completed = await self._run_copy_pass(copier, current)
                if completed:
                    await self._transition_to_dual_write(m, target_store)
            except asyncio.CancelledError:
                logger.info("Bulk copy cancelled for migration %s", m.id)
                raise
            except Exception as e:
                logger.error("Bulk copy failed for migration %s: %s", m.id, e)
                await self._fail_migration(m, str(e))
            finally:
                get_migration_metrics(str(m.id), str(m.tenant_id)).record_phase_duration("bulk_copy", time.monotonic() - phase_start)  # fmt: skip
                self._active_copiers.pop(m.id, None)
                self._active_tasks.pop(m.id, None)

    def _build_copier(self, m: Migration, target: FullEventStore | None = None) -> BulkCopier:
        if (store := target if target is not None else self._target_stores.get(m.id)) is None:
            raise MigrationError("No target store registered for migration; re-register before copying.", migration_id=m.id)  # fmt: skip
        mapper = self._position_mapper if m.config.position_mapping_enabled else None
        return BulkCopier(self._source_store, store, self._migration_repo, position_mapper=mapper, enable_tracing=self._enable_tracing)  # fmt: skip

    async def run_resync_pass(self, migration_id: UUID) -> int:
        """Run one bounded catch-up copy pass while in DUAL_WRITE."""
        with self._tracer.span("eventsource.coordinator.run_resync_pass", {_attr.ATTR_MIGRATION_ID: str(migration_id)}):  # fmt: skip
            if (m := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if m.phase != MigrationPhase.DUAL_WRITE:
                raise MigrationStateError(f"Cannot run a resync pass: migration is in {m.phase.value} phase", migration_id, m.phase, [MigrationPhase.DUAL_WRITE], "run_resync_pass")  # fmt: skip
            if migration_id in self._active_copiers:
                raise MigrationError("A copy pass is already running for this migration", migration_id=migration_id)  # fmt: skip
            self._active_copiers[migration_id] = (copier := self._build_copier(m))
            try:
                completed = await self._run_copy_pass(copier, m)
            finally:
                self._active_copiers.pop(migration_id, None)
            if not completed:
                raise MigrationError("Resync pass did not run to completion; the lag anchor stays clamped", migration_id=migration_id)  # fmt: skip
            if (current := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            absorbed = ic.mark_copy_pass_complete(current.last_source_position) if (ic := self._interceptors.get(migration_id)) else 0  # fmt: skip
            logger.info("Resync pass complete for %s: %s", migration_id, f"{absorbed} failures absorbed" if ic else "no interceptor")  # fmt: skip
            return absorbed

    async def _run_copy_pass(self, copier: BulkCopier, migration: Migration) -> bool:
        _last: BulkCopyProgress | None = None
        async for _last in copier.run(migration):
            await self._notify_status_update(migration.id)
        if _last and _last.is_complete:
            logger.info("Bulk copy complete for %s: %d events", migration.id, _last.events_copied)
        return bool(_last and _last.is_complete)

    def _install_interceptor(self, m: Migration, target: FullEventStore) -> DualWriteInterceptor:
        if (ic := self._interceptors.get(m.id)) is None:
            self._interceptors[m.id] = ic = DualWriteInterceptor(self._source_store, target, m.tenant_id, migration_id=m.id, enable_tracing=self._enable_tracing)  # fmt: skip
            self._router.set_dual_write_interceptor(m.tenant_id, ic)
        return ic

    async def _transition_to_dual_write(self, m: Migration, target: FullEventStore) -> None:
        with self._tracer.span("eventsource.coordinator.transition_to_dual_write", self._phase_span_attrs(m, MigrationPhase.DUAL_WRITE)):  # fmt: skip
            self._install_interceptor(m, target)
            self._lag_trackers[m.id] = SyncLagTracker(self._source_store, target, m.config, m.tenant_id, migration_id=m.id, enable_tracing=self._enable_tracing)  # fmt: skip
            await self._migration_repo.update_phase(m.id, MigrationPhase.DUAL_WRITE)
            await self._routing_repo.set_migration_state(m.tenant_id, TenantMigrationState.DUAL_WRITE, m.id)  # fmt: skip
            logger.info("Migration %s transitioned to dual-write phase for tenant %s", m.id, m.tenant_id)  # fmt: skip
            await self._record_audit(MigrationAuditEntry.phase_change(m.id, MigrationPhase.BULK_COPY, MigrationPhase.DUAL_WRITE, datetime.now(UTC)))  # fmt: skip
            await self._notify_status_update(m.id)


__all__ = ["CoordinatorCopyMixin"]
