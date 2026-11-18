"""Post-switch completion (verification, subscriptions, COMPLETED or FAILED) for MigrationCoordinator."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.consistency import VerificationLevel
from eventsource.application.migration.metrics import (
    get_migration_metrics,
    release_migration_metrics,
)
from eventsource.observability.error_summary import error_summary
from eventsource.ports.migration import models as m

if TYPE_CHECKING:
    from eventsource.application.migration.consistency import VerificationReport
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.application.migration.subscription_migrator import MigrationSummary
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports import FullEventStore
    from eventsource.ports.checkpoints import CheckpointRepository
    from eventsource.ports.migration.repositories import MigrationRepository

# Logger name kept from the module this step was split out of: log names are
# part of the telemetry surface, so they do not follow the import path.
logger = logging.getLogger("eventsource.application.migration.coordinator_cutover")


def _outcome(
    result: m.CutoverResult,
    *,
    success: bool,
    error_message: str | None = None,
    rolled_back: bool = False,
    consistency_verified: bool | None = None,
) -> m.CutoverResult:
    """Return the cutover result with the post-switch outcome applied."""
    return m.CutoverResult(
        success=success,
        duration_ms=result.duration_ms,
        events_synced=result.events_synced,
        error_message=error_message,
        rolled_back=rolled_back,
        consistency_verified=consistency_verified,
    )


class CoordinatorCutoverCompletionMixin:
    """Mixin providing the post-switch verification and COMPLETED or FAILED transition."""

    _migration_repo: MigrationRepository
    _target_stores: dict[UUID, FullEventStore]
    _lag_trackers: dict[UUID, SyncLagTracker]
    _position_mapper: PositionMapper | None
    _checkpoint_repo: CheckpointRepository | None

    if TYPE_CHECKING:

        async def _audit_cutover(self, migration_id: UUID, event_type: m.AuditEventType, new_phase: m.MigrationPhase, details: dict[str, object] | None = None) -> None: ...  # fmt: skip
        async def _fail_migration(self, migration: m.Migration, error: str, details: dict[str, object] | None = None) -> None: ...  # fmt: skip
        def _cleanup_status_queues(self, migration_id: UUID) -> None: ...
        async def verify_consistency(self, migration_id: UUID, *, level: VerificationLevel = ..., sample_percentage: float = ...) -> VerificationReport: ...  # fmt: skip
        async def migrate_subscriptions(self, migration_id: UUID) -> MigrationSummary: ...  # fmt: skip

    def _record_cutover_duration(
        self, migration: m.Migration, result: m.CutoverResult, *, success: bool
    ) -> None:
        """Record the cutover duration once per terminal outcome.

        Called only after the outcome is known. A cutover that switched routing
        but ended in FAILED records success=False, never success=True.
        """
        get_migration_metrics(str(migration.id), str(migration.tenant_id)).record_cutover_duration(result.duration_ms, success=success)  # fmt: skip

    async def _complete_cutover(
        self, migration: m.Migration, result: m.CutoverResult | None = None
    ) -> m.CutoverResult:
        """Verify, migrate subscriptions, then COMPLETED or FAILED.

        Routing has already switched to the target when this runs. A failed
        verification or subscription migration therefore ends in FAILED, never
        COMPLETED. The tenant keeps the target store; the failure is visible in
        the returned result, the migration error, and the audit entry.
        """
        result = result or m.CutoverResult(success=True, duration_ms=0.0)
        mid = migration.id
        failures: list[str] = []
        consistency_verified: bool | None = None
        subscriptions_migrated = 0

        if migration.config.verify_consistency:
            consistency_verified = False
            if self._target_stores.get(mid) is None:
                failures.append("consistency verification could not run: target store not found")
            else:
                try:
                    rep = await self.verify_consistency(
                        mid, level=VerificationLevel.HASH, sample_percentage=100.0
                    )
                    consistency_verified = rep.is_consistent
                    if not rep.is_consistent:
                        failures.append(
                            f"consistency verification found {len(rep.violations)} violations"
                        )
                except Exception as e:
                    err_text = error_summary(e)
                    logger.error("Consistency verification failed for %s: %s", mid, err_text)
                    failures.append(f"consistency verification raised: {err_text}")

        if migration.config.migrate_subscriptions:
            if self._position_mapper is not None and self._checkpoint_repo is not None:
                try:
                    summary = await self.migrate_subscriptions(mid)
                    subscriptions_migrated = summary.successful_count
                    if (n_fail := summary.failed_count) > 0:
                        failures.append(f"subscription migration failed for {n_fail} subscriptions")
                except Exception as e:
                    err_text = error_summary(e)
                    logger.error("Subscription migration failed for %s: %s", mid, err_text)
                    failures.append(f"subscription migration raised: {err_text}")
            else:
                logger.debug("Subscription migration skipped for %s", mid)

        if failures:
            error = "; ".join(failures)
            details: dict[str, object] = {"consistency_verified": consistency_verified, "subscriptions_migrated": subscriptions_migrated, "target_store_id": migration.target_store_id}  # fmt: skip
            # Record before _fail_migration: it releases the metrics instance.
            self._record_cutover_duration(migration, result, success=False)
            get_migration_metrics(str(mid), str(migration.tenant_id)).record_cutover_failure(reason="post_switch_checks")  # fmt: skip
            await self._fail_migration(migration, error, details)
            return _outcome(result, success=False, error_message=error, rolled_back=False, consistency_verified=consistency_verified)  # fmt: skip

        await self._migration_repo.update_phase(mid, m.MigrationPhase.COMPLETED)
        logger.info("Cutover completed for %s -> %s", mid, migration.target_store_id)
        details = {"target_store_id": migration.target_store_id, "consistency_verified": consistency_verified, "subscriptions_migrated": subscriptions_migrated}  # fmt: skip
        await self._audit_cutover(mid, m.AuditEventType.CUTOVER_COMPLETED, m.MigrationPhase.COMPLETED, details)  # fmt: skip
        self._lag_trackers.pop(mid, None)
        self._target_stores.pop(mid, None)
        self._cleanup_status_queues(mid)
        self._record_cutover_duration(migration, result, success=True)
        release_migration_metrics(str(mid))
        return _outcome(result, success=True, consistency_verified=consistency_verified)


__all__ = ["CoordinatorCutoverCompletionMixin"]
