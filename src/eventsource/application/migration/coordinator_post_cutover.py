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
from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError
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
        span_attrs = {ATTR_MIGRATION_ID: str(migration_id), "level": level.value, "sample_percentage": sample_percentage}  # fmt: skip
        with self._tracer.span("eventsource.coordinator.verify_consistency", span_attrs):
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if (target_store := self._target_stores.get(migration_id)) is None:
                raise MigrationError(
                    "Target store not found for migration. "
                    "Verify consistency is typically called after migration is started.",
                    migration_id=migration_id,
                )

            coord_mod = sys.modules.get("eventsource.application.migration.coordinator")
            verifier_cls = getattr(coord_mod, "ConsistencyVerifier", ConsistencyVerifier)
            verifier = verifier_cls(source_store=self._source_store, target_store=target_store, enable_tracing=self._enable_tracing)  # fmt: skip
            logger.info("Starting consistency verification for migration %s, tenant %s, level=%s", migration_id, migration.tenant_id, level.value)  # fmt: skip
            report = await verifier.verify_tenant_consistency(tenant_id=migration.tenant_id, level=level, sample_percentage=sample_percentage)  # fmt: skip
            self._consistency_reports[migration_id] = report

            if report.is_consistent:
                logger.info("Consistency verification passed for migration %s: %d events, %d streams verified", migration_id, report.source_event_count, report.streams_verified)  # fmt: skip
            else:
                logger.warning("Consistency verification FAILED for migration %s: %d violations found", migration_id, len(report.violations))  # fmt: skip
                metrics = get_migration_metrics(str(migration_id), str(migration.tenant_id))
                for violation in report.violations:
                    metrics.record_verification_failure(failure_type=violation.violation_type)

            event_type = AuditEventType.VERIFICATION_COMPLETED if report.is_consistent else AuditEventType.VERIFICATION_FAILED  # fmt: skip
            details = {
                "level": level.value,
                "is_consistent": report.is_consistent,
                "source_event_count": report.source_event_count,
                "target_event_count": report.target_event_count,
                "violation_count": len(report.violations),
            }
            await self._record_audit(MigrationAuditEntry(None, migration_id, event_type, None, None, details, None, datetime.now(UTC)))  # fmt: skip
            return report

    async def migrate_subscriptions(
        self,
        migration_id: UUID,
        subscription_names: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> MigrationSummary:
        span_attrs = {ATTR_MIGRATION_ID: str(migration_id), "dry_run": dry_run}
        with self._tracer.span("eventsource.coordinator.migrate_subscriptions", span_attrs):
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if self._position_mapper is None:
                raise MigrationError("Cannot migrate subscriptions: position_mapper not provided to coordinator.", migration_id=migration_id)  # fmt: skip
            if self._checkpoint_repo is None:
                raise MigrationError("Cannot migrate subscriptions: checkpoint_repo not provided to coordinator.", migration_id=migration_id)  # fmt: skip

            coord_mod = sys.modules.get("eventsource.application.migration.coordinator")
            migrator_cls = getattr(coord_mod, "SubscriptionMigrator", SubscriptionMigrator)
            migrator = migrator_cls(position_mapper=self._position_mapper, checkpoint_repo=self._checkpoint_repo, enable_tracing=self._enable_tracing)  # fmt: skip
            logger.info("Starting subscription migration for migration %s, tenant %s", migration_id, migration.tenant_id)  # fmt: skip
            summary = await migrator.migrate_tenant_subscriptions(migration_id=migration_id, tenant_id=migration.tenant_id, subscription_names=subscription_names, dry_run=dry_run)  # fmt: skip
            self._subscription_summaries[migration_id] = summary
            if summary.all_successful:
                logger.info("Subscription migration completed for migration %s: %d subscriptions migrated", migration_id, summary.successful_count)  # fmt: skip
            else:
                logger.warning("Subscription migration completed with failures for migration %s: %d successful, %d failed, %d skipped", migration_id, summary.successful_count, summary.failed_count, summary.skipped_count)  # fmt: skip
            return summary

    def get_consistency_report(self, migration_id: UUID) -> VerificationReport | None:
        return self._consistency_reports.get(migration_id)

    def get_subscription_summary(self, migration_id: UUID) -> MigrationSummary | None:
        return self._subscription_summaries.get(migration_id)


__all__ = ["CoordinatorPostCutoverMixin"]
