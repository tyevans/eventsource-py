"""Cutover triggering, completion, rollback, and sync lag management for MigrationCoordinator."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.consistency import VerificationLevel, VerificationReport
from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.application.migration.metrics import release_migration_metrics
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID, ATTR_MIGRATION_PHASE
from eventsource.ports import FullEventStore, Position
from eventsource.ports.migration.models import (
    AuditEventType,
    CutoverResult,
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
    SyncLag,
)

if TYPE_CHECKING:
    from eventsource.application.migration.dual_write import DualWriteInterceptor
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.application.migration.subscription_migrator import MigrationSummary
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.checkpoints import CheckpointRepository
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import (
        MigrationRepository,
        TenantRoutingRepository,
    )

logger = logging.getLogger(__name__)


class CoordinatorCutoverMixin:
    """Mixin providing cutover execution, completion, rollback, and lag checks."""

    _tracer: Tracer
    _migration_repo: MigrationRepository
    _routing_repo: TenantRoutingRepository
    _router: TenantStoreRouter
    _lag_trackers: dict[UUID, SyncLagTracker]
    _target_stores: dict[UUID, FullEventStore]
    _interceptors: dict[UUID, DualWriteInterceptor]
    _cutover_manager: CutoverManager | None
    _lock_manager: DistributedLock | None
    _position_mapper: PositionMapper | None
    _checkpoint_repo: CheckpointRepository | None
    _consistency_reports: dict[UUID, VerificationReport]
    _subscription_summaries: dict[UUID, MigrationSummary]
    _enable_tracing: bool

    if TYPE_CHECKING:

        async def _record_audit(self, entry: MigrationAuditEntry) -> None: ...
        async def _notify_status_update(self, migration_id: UUID) -> None: ...
        def _cleanup_status_queues(self, migration_id: UUID) -> None: ...
        async def verify_consistency(
            self,
            migration_id: UUID,
            *,
            level: VerificationLevel = ...,
            sample_percentage: float = ...,
        ) -> VerificationReport: ...
        async def migrate_subscriptions(self, migration_id: UUID) -> MigrationSummary: ...

    async def _audit_cutover(
        self,
        migration_id: UUID,
        event_type: AuditEventType,
        new_phase: MigrationPhase,
        details: dict[str, object] | None = None,
        *,
        old_phase: MigrationPhase = MigrationPhase.CUTOVER,
    ) -> None:
        entry = MigrationAuditEntry(
            None, migration_id, event_type, old_phase, new_phase, details, None, datetime.now(UTC)
        )
        await self._record_audit(entry)

    async def trigger_cutover(
        self, migration_id: UUID, *, timeout_ms: float | None = None
    ) -> CutoverResult:
        """Trigger cutover for a migration in dual-write phase; rolls back to dual-write on failure."""
        span_attrs = {
            ATTR_MIGRATION_ID: str(migration_id),
            ATTR_MIGRATION_PHASE: MigrationPhase.CUTOVER.value,
        }
        with self._tracer.span("eventsource.coordinator.trigger_cutover", span_attrs):
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if migration.phase != MigrationPhase.DUAL_WRITE:
                raise MigrationStateError(
                    f"Cannot trigger cutover: migration is in {migration.phase.value} phase",
                    migration_id,
                    migration.phase,
                    [MigrationPhase.DUAL_WRITE],
                    "trigger_cutover",
                )
            if (lag_tracker := self._lag_trackers.get(migration_id)) is None:
                raise MigrationError(
                    "No sync lag tracker found for migration", migration_id=migration_id
                )

            cutover_manager = self._get_cutover_manager()
            await self._migration_repo.update_phase(migration_id, MigrationPhase.CUTOVER)
            logger.info(
                f"Starting cutover for migration {migration_id}, tenant {migration.tenant_id}"
            )
            await self._audit_cutover(
                migration_id,
                AuditEventType.CUTOVER_INITIATED,
                MigrationPhase.CUTOVER,
                old_phase=MigrationPhase.DUAL_WRITE,
            )
            result = await cutover_manager.execute_cutover(
                migration_id=migration_id,
                tenant_id=migration.tenant_id,
                lag_tracker=lag_tracker,
                target_store_id=migration.target_store_id,
                config=migration.config,
                timeout_ms=timeout_ms,
                since=self._lag_anchor(migration),
            )
            if result.success:
                await self._complete_cutover(migration)
            else:
                await self._rollback_cutover(migration, result)
            return result

    async def _complete_cutover(self, migration: Migration) -> None:
        consistency_verified = False
        if migration.config.verify_consistency:
            try:
                if self._target_stores.get(migration.id) is not None:
                    report = await self.verify_consistency(
                        migration.id, level=VerificationLevel.HASH, sample_percentage=100.0
                    )
                    consistency_verified = report.is_consistent
                    if not report.is_consistent:
                        logger.warning(
                            f"Post-cutover consistency verification found {len(report.violations)} violations for migration {migration.id} (non-fatal, migration proceeding)"
                        )
                else:
                    logger.warning(
                        f"Cannot verify consistency: target store not found for migration {migration.id}"
                    )
            except Exception as e:
                logger.error(
                    f"Consistency verification failed for migration {migration.id}: {e} (non-fatal, migration proceeding)"
                )

        subscriptions_migrated = 0
        if migration.config.migrate_subscriptions:
            try:
                if self._position_mapper is not None and self._checkpoint_repo is not None:
                    summary = await self.migrate_subscriptions(migration.id)
                    subscriptions_migrated = summary.successful_count
                    if summary.failed_count > 0:
                        logger.warning(
                            f"Subscription migration had {summary.failed_count} failures for migration {migration.id} (non-fatal, migration proceeding)"
                        )
                else:
                    logger.debug(
                        f"Subscription migration skipped: position_mapper or checkpoint_repo not configured for migration {migration.id}"
                    )
            except Exception as e:
                logger.error(
                    f"Subscription migration failed for migration {migration.id}: {e} (non-fatal, migration proceeding)"
                )

        await self._migration_repo.update_phase(migration.id, MigrationPhase.COMPLETED)
        logger.info(
            f"Cutover completed for migration {migration.id}: tenant {migration.tenant_id} now routes to {migration.target_store_id} (consistency_verified={consistency_verified}, subscriptions_migrated={subscriptions_migrated})"
        )
        details: dict[str, object] = {
            "target_store_id": migration.target_store_id,
            "consistency_verified": consistency_verified,
            "subscriptions_migrated": subscriptions_migrated,
        }
        await self._audit_cutover(
            migration.id, AuditEventType.CUTOVER_COMPLETED, MigrationPhase.COMPLETED, details
        )
        self._lag_trackers.pop(migration.id, None)
        self._target_stores.pop(migration.id, None)
        self._cleanup_status_queues(migration.id)
        release_migration_metrics(str(migration.id))

    async def _rollback_cutover(self, migration: Migration, result: CutoverResult) -> None:
        await self._migration_repo.update_phase(migration.id, MigrationPhase.DUAL_WRITE)
        if msg := result.error_message:
            await self._migration_repo.record_error(migration.id, f"Cutover failed: {msg}")
        logger.warning(f"Cutover rolled back for migration {migration.id}: {result.error_message}")
        await self._audit_cutover(
            migration.id,
            AuditEventType.CUTOVER_ROLLED_BACK,
            MigrationPhase.DUAL_WRITE,
            {"error_message": result.error_message},
        )
        await self._notify_status_update(migration.id)

    def _get_cutover_manager(self) -> CutoverManager:
        if self._cutover_manager is None:
            if self._lock_manager is None:
                raise MigrationError(
                    "Cannot perform cutover: lock_manager not provided to coordinator. Provide a lock manager (e.g. PostgreSQLLockManager) when creating the coordinator to enable cutover operations."
                )
            self._cutover_manager = CutoverManager(
                self._lock_manager,
                self._router,
                self._routing_repo,
                enable_tracing=self._enable_tracing,
            )
        return self._cutover_manager

    def _lag_anchor(self, migration: Migration) -> Position | None:
        if (interceptor := self._interceptors.get(migration.id)) is None:
            return migration.last_source_position
        return interceptor.safe_lag_anchor(migration.last_source_position)

    async def get_sync_lag(self, migration_id: UUID) -> SyncLag | None:
        """Get current sync lag for a migration."""
        if (migration := await self._migration_repo.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        if (lag_tracker := self._lag_trackers.get(migration_id)) is None:
            return None
        return await lag_tracker.calculate_lag(since=self._lag_anchor(migration))

    async def is_cutover_ready(self, migration_id: UUID) -> tuple[bool, str | None]:
        """Check if a migration is ready for cutover."""
        if (migration := await self._migration_repo.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        if migration.phase != MigrationPhase.DUAL_WRITE:
            return False, f"Migration is in {migration.phase.value} phase, expected DUAL_WRITE"
        if (lag_tracker := self._lag_trackers.get(migration_id)) is None:
            return False, "No sync lag tracker found for migration"
        await lag_tracker.calculate_lag(since=self._lag_anchor(migration))
        if lag_tracker.is_sync_ready():
            return True, None
        lag = lag_tracker.current_lag.events if lag_tracker.current_lag else "unknown"
        max_lag = migration.config.cutover_max_lag_events
        return False, f"Sync lag too high: {lag} events (max: {max_lag})"


__all__ = ["CoordinatorCutoverMixin"]
