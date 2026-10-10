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
from eventsource.observability.attributes import (
    ATTR_MIGRATION_ID,
    ATTR_MIGRATION_PHASE,
)
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

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        raise NotImplementedError

    async def _notify_status_update(self, migration_id: UUID) -> None:
        raise NotImplementedError

    def _cleanup_status_queues(self, migration_id: UUID) -> None:
        raise NotImplementedError

    async def verify_consistency(
        self,
        migration_id: UUID,
        *,
        level: VerificationLevel = VerificationLevel.HASH,
        sample_percentage: float = 100.0,
    ) -> VerificationReport:
        raise NotImplementedError

    async def migrate_subscriptions(
        self,
        migration_id: UUID,
        subscription_names: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> MigrationSummary:
        raise NotImplementedError

    async def trigger_cutover(
        self,
        migration_id: UUID,
        *,
        timeout_ms: float | None = None,
    ) -> CutoverResult:
        """
        Trigger cutover for a migration in dual-write phase.

        Executes the cutover operation, which:
        1. Pauses writes briefly
        2. Verifies sync lag is within threshold
        3. Switches routing to target store
        4. Resumes writes to new target

        If cutover fails, automatically rolls back to dual-write phase.
        """
        with self._tracer.span(
            "eventsource.coordinator.trigger_cutover",
            {
                ATTR_MIGRATION_ID: str(migration_id),
                ATTR_MIGRATION_PHASE: MigrationPhase.CUTOVER.value,
            },
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if migration.phase != MigrationPhase.DUAL_WRITE:
                raise MigrationStateError(
                    message=f"Cannot trigger cutover: migration is in {migration.phase.value} phase",
                    migration_id=migration_id,
                    current_phase=migration.phase,
                    expected_phases=[MigrationPhase.DUAL_WRITE],
                    operation="trigger_cutover",
                )

            # Get lag tracker
            lag_tracker = self._lag_trackers.get(migration_id)
            if lag_tracker is None:
                raise MigrationError(
                    "No sync lag tracker found for migration",
                    migration_id=migration_id,
                )

            # Get or create cutover manager
            cutover_manager = self._get_cutover_manager()

            # Update migration phase to CUTOVER
            await self._migration_repo.update_phase(
                migration_id,
                MigrationPhase.CUTOVER,
            )

            logger.info(
                "Starting cutover for migration %s, tenant %s",
                migration_id,
                migration.tenant_id,
            )

            await self._record_audit(
                MigrationAuditEntry(
                    id=None,
                    migration_id=migration_id,
                    event_type=AuditEventType.CUTOVER_INITIATED,
                    old_phase=MigrationPhase.DUAL_WRITE,
                    new_phase=MigrationPhase.CUTOVER,
                    details=None,
                    operator=None,
                    occurred_at=datetime.now(UTC),
                )
            )

            # Execute cutover
            result = await cutover_manager.execute_cutover(
                migration_id=migration_id,
                tenant_id=migration.tenant_id,
                lag_tracker=lag_tracker,
                target_store_id=migration.target_store_id,
                config=migration.config,
                timeout_ms=timeout_ms,
                since=self._lag_anchor(migration),
            )

            # Handle result
            if result.success:
                await self._complete_cutover(migration)
            else:
                await self._rollback_cutover(migration, result)

            return result

    async def _complete_cutover(self, migration: Migration) -> None:
        """Complete the migration after successful cutover."""
        # Phase 3 (P3-005): Run consistency verification if enabled
        consistency_verified = False
        if migration.config.verify_consistency:
            try:
                target_store = self._target_stores.get(migration.id)
                if target_store is not None:
                    from eventsource.application.migration.consistency import VerificationLevel

                    report = await self.verify_consistency(
                        migration.id,
                        level=VerificationLevel.HASH,
                        sample_percentage=100.0,
                    )
                    consistency_verified = report.is_consistent
                    if not report.is_consistent:
                        logger.warning(
                            "Post-cutover consistency verification found %d violations "
                            "for migration %s (non-fatal, migration proceeding)",
                            len(report.violations),
                            migration.id,
                        )
                else:
                    logger.warning(
                        "Cannot verify consistency: target store not found for migration %s",
                        migration.id,
                    )
            except Exception as e:
                logger.error(
                    "Consistency verification failed for migration %s: %s "
                    "(non-fatal, migration proceeding)",
                    migration.id,
                    e,
                )

        # Phase 3 (P3-005): Migrate subscriptions if enabled
        subscriptions_migrated = 0
        if migration.config.migrate_subscriptions:
            try:
                if self._position_mapper is not None and self._checkpoint_repo is not None:
                    summary = await self.migrate_subscriptions(migration.id)
                    subscriptions_migrated = summary.successful_count
                    if summary.failed_count > 0:
                        logger.warning(
                            "Subscription migration had %d failures for migration %s "
                            "(non-fatal, migration proceeding)",
                            summary.failed_count,
                            migration.id,
                        )
                else:
                    logger.debug(
                        "Subscription migration skipped: position_mapper or "
                        "checkpoint_repo not configured for migration %s",
                        migration.id,
                    )
            except Exception as e:
                logger.error(
                    "Subscription migration failed for migration %s: %s "
                    "(non-fatal, migration proceeding)",
                    migration.id,
                    e,
                )

        # Update migration phase to COMPLETED
        await self._migration_repo.update_phase(
            migration.id,
            MigrationPhase.COMPLETED,
        )

        logger.info(
            "Cutover completed for migration %s: tenant %s now routes to %s "
            "(consistency_verified=%s, subscriptions_migrated=%d)",
            migration.id,
            migration.tenant_id,
            migration.target_store_id,
            consistency_verified,
            subscriptions_migrated,
        )

        await self._record_audit(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.CUTOVER_COMPLETED,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.COMPLETED,
                details={
                    "target_store_id": migration.target_store_id,
                    "consistency_verified": consistency_verified,
                    "subscriptions_migrated": subscriptions_migrated,
                },
                operator=None,
                occurred_at=datetime.now(UTC),
            )
        )

        # Clean up resources (but keep reports and summaries for retrieval)
        self._lag_trackers.pop(migration.id, None)
        self._target_stores.pop(migration.id, None)
        self._cleanup_status_queues(migration.id)
        release_migration_metrics(str(migration.id))

    async def _rollback_cutover(
        self,
        migration: Migration,
        result: CutoverResult,
    ) -> None:
        """Rollback to dual-write phase after cutover failure."""
        await self._migration_repo.update_phase(
            migration.id,
            MigrationPhase.DUAL_WRITE,
        )

        if result.error_message:
            await self._migration_repo.record_error(
                migration.id,
                f"Cutover failed: {result.error_message}",
            )

        logger.warning(
            "Cutover rolled back for migration %s: %s",
            migration.id,
            result.error_message,
        )

        await self._record_audit(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.CUTOVER_ROLLED_BACK,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.DUAL_WRITE,
                details={"error_message": result.error_message},
                operator=None,
                occurred_at=datetime.now(UTC),
            )
        )

        await self._notify_status_update(migration.id)

    def _get_cutover_manager(self) -> CutoverManager:
        """Get or create the CutoverManager instance."""
        if self._cutover_manager is not None:
            return self._cutover_manager

        if self._lock_manager is None:
            raise MigrationError(
                "Cannot perform cutover: lock_manager not provided to coordinator. "
                "Provide a lock manager (e.g. PostgreSQLLockManager) when creating "
                "the coordinator to enable cutover operations.",
            )

        self._cutover_manager = CutoverManager(
            lock_manager=self._lock_manager,
            router=self._router,
            routing_repo=self._routing_repo,
            enable_tracing=self._enable_tracing,
        )

        return self._cutover_manager

    def _lag_anchor(self, migration: Migration) -> Position | None:
        """The position to count lag from for this migration."""
        interceptor = self._interceptors.get(migration.id)
        if interceptor is None:
            return migration.last_source_position
        return interceptor.safe_lag_anchor(migration.last_source_position)

    async def get_sync_lag(self, migration_id: UUID) -> SyncLag | None:
        """Get current sync lag for a migration."""
        migration = await self._migration_repo.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        lag_tracker = self._lag_trackers.get(migration_id)
        if lag_tracker is None:
            return None

        return await lag_tracker.calculate_lag(since=self._lag_anchor(migration))

    async def is_cutover_ready(self, migration_id: UUID) -> tuple[bool, str | None]:
        """Check if a migration is ready for cutover."""
        migration = await self._migration_repo.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        if migration.phase != MigrationPhase.DUAL_WRITE:
            return False, f"Migration is in {migration.phase.value} phase, expected DUAL_WRITE"

        lag_tracker = self._lag_trackers.get(migration_id)
        if lag_tracker is None:
            return False, "No sync lag tracker found for migration"

        await lag_tracker.calculate_lag(since=self._lag_anchor(migration))
        if lag_tracker.is_sync_ready():
            return True, None
        current_lag = lag_tracker.current_lag
        lag_events = current_lag.events if current_lag else "unknown"
        return (
            False,
            f"Sync lag too high: {lag_events} events (max: {migration.config.cutover_max_lag_events})",
        )


__all__ = [
    "CoordinatorCutoverMixin",
]
