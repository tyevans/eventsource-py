"""Post-cutover consistency verification and subscription migration mixin for MigrationCoordinator."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
    VerificationReport,
)
from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.application.migration.subscription_migrator import (
    MigrationSummary,
    SubscriptionMigrator,
)
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.ports import FullEventStore
from eventsource.ports.migration.models import AuditEventType, MigrationAuditEntry

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.ports.checkpoints import CheckpointRepository
    from eventsource.ports.migration.repositories import MigrationRepository

logger = logging.getLogger(__name__)


class CoordinatorPostCutoverMixin:
    """Mixin providing consistency verification and subscription migration."""

    _tracer: Tracer
    _migration_repo: MigrationRepository
    _target_stores: dict[UUID, FullEventStore]
    _source_store: FullEventStore
    _position_mapper: PositionMapper | None
    _checkpoint_repo: CheckpointRepository | None
    _consistency_reports: dict[UUID, VerificationReport]
    _subscription_summaries: dict[UUID, MigrationSummary]
    _enable_tracing: bool

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        raise NotImplementedError

    async def verify_consistency(
        self,
        migration_id: UUID,
        *,
        level: VerificationLevel = VerificationLevel.HASH,
        sample_percentage: float = 100.0,
    ) -> VerificationReport:
        """
        Verify data consistency between source and target stores.

        Performs consistency verification for the migrated tenant data
        using the specified verification level.
        """
        with self._tracer.span(
            "eventsource.coordinator.verify_consistency",
            {
                ATTR_MIGRATION_ID: str(migration_id),
                "level": level.value,
                "sample_percentage": sample_percentage,
            },
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            target_store = self._target_stores.get(migration_id)
            if target_store is None:
                raise MigrationError(
                    "Target store not found for migration. "
                    "Verify consistency is typically called after migration is started.",
                    migration_id=migration_id,
                )

            verifier_cls = getattr(
                sys.modules.get("eventsource.application.migration.coordinator"),
                "ConsistencyVerifier",
                ConsistencyVerifier,
            )
            verifier = verifier_cls(
                source_store=self._source_store,
                target_store=target_store,
                enable_tracing=self._enable_tracing,
            )

            logger.info(
                "Starting consistency verification for migration %s, tenant %s, level=%s",
                migration_id,
                migration.tenant_id,
                level.value,
            )

            report = await verifier.verify_tenant_consistency(
                tenant_id=migration.tenant_id,
                level=level,
                sample_percentage=sample_percentage,
            )

            self._consistency_reports[migration_id] = report

            if report.is_consistent:
                logger.info(
                    "Consistency verification passed for migration %s: "
                    "%d events, %d streams verified",
                    migration_id,
                    report.source_event_count,
                    report.streams_verified,
                )
            else:
                logger.warning(
                    "Consistency verification FAILED for migration %s: %d violations found",
                    migration_id,
                    len(report.violations),
                )
                metrics = get_migration_metrics(str(migration_id), str(migration.tenant_id))
                for violation in report.violations:
                    metrics.record_verification_failure(failure_type=violation.violation_type)

            await self._record_audit(
                MigrationAuditEntry(
                    id=None,
                    migration_id=migration_id,
                    event_type=(
                        AuditEventType.VERIFICATION_COMPLETED
                        if report.is_consistent
                        else AuditEventType.VERIFICATION_FAILED
                    ),
                    old_phase=None,
                    new_phase=None,
                    details={
                        "level": level.value,
                        "is_consistent": report.is_consistent,
                        "source_event_count": report.source_event_count,
                        "target_event_count": report.target_event_count,
                        "violation_count": len(report.violations),
                    },
                    operator=None,
                    occurred_at=datetime.now(UTC),
                )
            )

            return report

    async def migrate_subscriptions(
        self,
        migration_id: UUID,
        subscription_names: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> MigrationSummary:
        """Migrate subscription checkpoints to target store positions."""
        with self._tracer.span(
            "eventsource.coordinator.migrate_subscriptions",
            {
                ATTR_MIGRATION_ID: str(migration_id),
                "dry_run": dry_run,
            },
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if self._position_mapper is None:
                raise MigrationError(
                    "Cannot migrate subscriptions: position_mapper not provided "
                    "to coordinator. Provide a PositionMapper when creating the "
                    "coordinator to enable subscription migration.",
                    migration_id=migration_id,
                )

            if self._checkpoint_repo is None:
                raise MigrationError(
                    "Cannot migrate subscriptions: checkpoint_repo not provided "
                    "to coordinator. Provide a CheckpointRepository when creating "
                    "the coordinator to enable subscription migration.",
                    migration_id=migration_id,
                )

            migrator_cls = getattr(
                sys.modules.get("eventsource.application.migration.coordinator"),
                "SubscriptionMigrator",
                SubscriptionMigrator,
            )
            migrator = migrator_cls(
                position_mapper=self._position_mapper,
                checkpoint_repo=self._checkpoint_repo,
                enable_tracing=self._enable_tracing,
            )

            logger.info(
                "Starting subscription migration for migration %s, tenant %s",
                migration_id,
                migration.tenant_id,
            )

            summary = await migrator.migrate_tenant_subscriptions(
                migration_id=migration_id,
                tenant_id=migration.tenant_id,
                subscription_names=subscription_names,
                dry_run=dry_run,
            )

            self._subscription_summaries[migration_id] = summary

            if summary.all_successful:
                logger.info(
                    "Subscription migration completed for migration %s: %d subscriptions migrated",
                    migration_id,
                    summary.successful_count,
                )
            else:
                logger.warning(
                    "Subscription migration completed with failures for migration %s: "
                    "%d successful, %d failed, %d skipped",
                    migration_id,
                    summary.successful_count,
                    summary.failed_count,
                    summary.skipped_count,
                )

            return summary

    def get_consistency_report(self, migration_id: UUID) -> VerificationReport | None:
        """Get the consistency verification report for a migration."""
        return self._consistency_reports.get(migration_id)

    def get_subscription_summary(self, migration_id: UUID) -> MigrationSummary | None:
        """Get the subscription migration summary for a migration."""
        return self._subscription_summaries.get(migration_id)


__all__ = [
    "CoordinatorPostCutoverMixin",
]
