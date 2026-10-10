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
from eventsource.observability.attributes import (
    ATTR_MIGRATION_ID,
    ATTR_MIGRATION_PHASE,
    ATTR_MIGRATION_TENANT_ID,
)
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

    # Bounded catch-up rounds after the main copy pass.
    _MAX_CATCHUP_ROUNDS = 10

    async def _fail_migration(self, migration: Migration, error: str) -> None:
        raise NotImplementedError

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        raise NotImplementedError

    async def _notify_status_update(self, migration_id: UUID) -> None:
        raise NotImplementedError

    async def _run_bulk_copy(
        self,
        migration: Migration,
        target_store: FullEventStore,
    ) -> None:
        """
        Run the bulk copy phase.

        Installs the DualWriteInterceptor FIRST, then streams historical
        events from source to target store.
        """
        with self._tracer.span(
            "eventsource.coordinator.run_bulk_copy",
            {
                ATTR_MIGRATION_ID: str(migration.id),
                ATTR_MIGRATION_TENANT_ID: str(migration.tenant_id),
                ATTR_MIGRATION_PHASE: MigrationPhase.BULK_COPY.value,
            },
        ):
            # Install the interceptor BEFORE the copy pass starts
            interceptor = self._install_interceptor(migration, target_store)

            copier = self._build_copier(migration, target_store)

            self._active_copiers[migration.id] = copier
            phase_start = time.monotonic()

            try:
                completed = await self._run_copy_pass(copier, migration)

                rounds = 0
                while completed:
                    current = await self._migration_repo.get(migration.id)
                    if current is None:
                        raise MigrationNotFoundError(migration.id)

                    remaining = interceptor.mark_copy_pass_complete(current.last_source_position)
                    if remaining == 0:
                        break
                    if rounds >= self._MAX_CATCHUP_ROUNDS:
                        logger.warning(
                            "Migration %s: %d mirror failures remain unabsorbed "
                            "after %d catch-up rounds; the lag anchor stays "
                            "clamped at the checkpoint until another copy pass "
                            "absorbs them",
                            migration.id,
                            remaining,
                            rounds,
                        )
                        break
                    rounds += 1
                    logger.info(
                        "Migration %s: catch-up round %d to absorb %d mirror failure(s)",
                        migration.id,
                        rounds,
                        remaining,
                    )
                    completed = await self._run_copy_pass(copier, current)

                if completed:
                    # Transition to dual-write phase (P2-005)
                    await self._transition_to_dual_write(migration, target_store)

            except asyncio.CancelledError:
                logger.info("Bulk copy cancelled for migration %s", migration.id)
                raise

            except Exception as e:
                logger.error(
                    "Bulk copy failed for migration %s: %s",
                    migration.id,
                    e,
                )
                await self._fail_migration(migration, str(e))

            finally:
                get_migration_metrics(
                    str(migration.id), str(migration.tenant_id)
                ).record_phase_duration("bulk_copy", time.monotonic() - phase_start)
                self._active_copiers.pop(migration.id, None)
                self._active_tasks.pop(migration.id, None)

    def _build_copier(
        self,
        migration: Migration,
        target_store: FullEventStore | None = None,
    ) -> BulkCopier:
        """Construct the migration's BulkCopier -- the only place that does."""
        store = target_store or self._target_stores.get(migration.id)
        if store is None:
            raise MigrationError(
                "No target store registered for migration; the coordinator "
                "that started it holds that registry in memory, so a "
                "restarted coordinator must re-register before copying.",
                migration_id=migration.id,
            )

        mapper = self._position_mapper if migration.config.position_mapping_enabled else None

        return BulkCopier(
            self._source_store,
            store,
            self._migration_repo,
            position_mapper=mapper,
            enable_tracing=self._enable_tracing,
        )

    async def run_resync_pass(self, migration_id: UUID) -> int:
        """Run one bounded catch-up copy pass while in DUAL_WRITE."""
        with self._tracer.span(
            "eventsource.coordinator.run_resync_pass",
            {ATTR_MIGRATION_ID: str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if migration.phase != MigrationPhase.DUAL_WRITE:
                raise MigrationStateError(
                    message=(
                        f"Cannot run a resync pass: migration is in {migration.phase.value} phase"
                    ),
                    migration_id=migration_id,
                    current_phase=migration.phase,
                    expected_phases=[MigrationPhase.DUAL_WRITE],
                    operation="run_resync_pass",
                )

            if migration_id in self._active_copiers:
                raise MigrationError(
                    "A copy pass is already running for this migration",
                    migration_id=migration_id,
                )

            copier = self._build_copier(migration)
            self._active_copiers[migration_id] = copier
            try:
                completed = await self._run_copy_pass(copier, migration)
            finally:
                self._active_copiers.pop(migration_id, None)

            if not completed:
                raise MigrationError(
                    "Resync pass did not run to completion; the lag anchor "
                    "stays clamped (an incomplete pass attests nothing)",
                    migration_id=migration_id,
                )

            current = await self._migration_repo.get(migration_id)
            if current is None:
                raise MigrationNotFoundError(migration_id)

            interceptor = self._interceptors.get(migration_id)
            if interceptor is None:
                logger.info(
                    "Resync pass complete for migration %s: no interceptor "
                    "registered (coordinator restarted since dual-write "
                    "began), so no failures could be absorbed; the pass "
                    "advanced the persisted checkpoint",
                    migration_id,
                )
                return 0

            absorbed = interceptor.mark_copy_pass_complete(current.last_source_position)
            logger.info(
                "Resync pass complete for migration %s: %d mirror failures absorbed",
                migration_id,
                absorbed,
            )
            return absorbed

    async def _run_copy_pass(self, copier: BulkCopier, migration: Migration) -> bool:
        """Run one copier pass, streaming progress to status observers."""
        last_progress: BulkCopyProgress | None = None

        async for progress in copier.run(migration):
            last_progress = progress
            await self._notify_status_update(migration.id)

        if last_progress and last_progress.is_complete:
            logger.info(
                "Bulk copy pass complete for migration %s: %d events",
                migration.id,
                last_progress.events_copied,
            )
            return True
        return False

    def _install_interceptor(
        self,
        migration: Migration,
        target_store: FullEventStore,
    ) -> DualWriteInterceptor:
        """Create and register the dual-write interceptor for a migration."""
        interceptor = self._interceptors.get(migration.id)
        if interceptor is not None:
            return interceptor

        interceptor = DualWriteInterceptor(
            source_store=self._source_store,
            target_store=target_store,
            tenant_id=migration.tenant_id,
            migration_id=migration.id,
            enable_tracing=self._enable_tracing,
        )
        self._router.set_dual_write_interceptor(migration.tenant_id, interceptor)
        self._interceptors[migration.id] = interceptor
        return interceptor

    async def _transition_to_dual_write(
        self,
        migration: Migration,
        target_store: FullEventStore,
    ) -> None:
        """Transition from bulk copy to dual-write phase."""
        with self._tracer.span(
            "eventsource.coordinator.transition_to_dual_write",
            {
                ATTR_MIGRATION_ID: str(migration.id),
                ATTR_MIGRATION_TENANT_ID: str(migration.tenant_id),
                ATTR_MIGRATION_PHASE: MigrationPhase.DUAL_WRITE.value,
            },
        ):
            self._install_interceptor(migration, target_store)

            lag_tracker = SyncLagTracker(
                source_store=self._source_store,
                target_store=target_store,
                config=migration.config,
                tenant_id=migration.tenant_id,
                migration_id=migration.id,
                enable_tracing=self._enable_tracing,
            )
            self._lag_trackers[migration.id] = lag_tracker

            await self._migration_repo.update_phase(
                migration.id,
                MigrationPhase.DUAL_WRITE,
            )

            await self._routing_repo.set_migration_state(
                migration.tenant_id,
                TenantMigrationState.DUAL_WRITE,
                migration.id,
            )

            logger.info(
                "Migration %s transitioned to dual-write phase for tenant %s",
                migration.id,
                migration.tenant_id,
            )

            await self._record_audit(
                MigrationAuditEntry.phase_change(
                    migration_id=migration.id,
                    old_phase=MigrationPhase.BULK_COPY,
                    new_phase=MigrationPhase.DUAL_WRITE,
                    occurred_at=datetime.now(UTC),
                )
            )

            await self._notify_status_update(migration.id)


__all__ = [
    "CoordinatorCopyMixin",
]
