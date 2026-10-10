"""
Subscription migration execution and checkpoint transition operations.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.subscription_migrator_types import (
    MigrationPlan,
    MigrationSummary,
    SubscriptionMigrationError,
    SubscriptionMigrationResult,
)

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.observability import Tracer
    from eventsource.ports.checkpoints import CheckpointRepository

logger = logging.getLogger(__name__)


class SubscriptionMigratorExecutionMixin:
    """Mixin providing subscription migration execution and checkpoint transition operations."""

    _position_mapper: PositionMapper
    _checkpoint_repo: CheckpointRepository
    _tracer: Tracer

    # Stub for method provided by SubscriptionMigratorPlanningMixin
    async def plan_migration(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        subscription_names: list[str],
    ) -> MigrationPlan:
        raise NotImplementedError

    async def migrate_subscriptions(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        subscription_names: list[str],
        *,
        dry_run: bool = False,
    ) -> MigrationSummary:
        """
        Migrate subscription checkpoints to target store positions.

        Translates each subscription's checkpoint position from source
        to target store and updates the checkpoint atomically.

        Args:
            migration_id: ID of the migration.
            tenant_id: Tenant being migrated.
            subscription_names: Names of subscriptions to migrate.
            dry_run: If True, return plan without executing changes.

        Returns:
            MigrationSummary with results of all migrations.

        Raises:
            SubscriptionMigrationError: If a critical error occurs.

        Example:
            >>> summary = await migrator.migrate_subscriptions(
            ...     migration_id=migration_id,
            ...     tenant_id=tenant_id,
            ...     subscription_names=["OrderProjection"],
            ... )
            >>> if summary.all_successful:
            ...     print("All subscriptions migrated successfully")
        """
        with self._tracer.span(
            "eventsource.subscription_migrator.migrate_subscriptions",
            {
                "migration.id": str(migration_id),
                "tenant.id": str(tenant_id),
                "subscription_count": len(subscription_names),
                "dry_run": dry_run,
            },
        ):
            start_time = datetime.now(UTC)

            logger.info(
                "Starting subscription migration",
                extra={
                    "migration_id": str(migration_id),
                    "tenant_id": str(tenant_id),
                    "subscription_count": len(subscription_names),
                    "dry_run": dry_run,
                },
            )

            # If dry-run, just return the plan as a summary
            if dry_run:
                plan = await self.plan_migration(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    subscription_names=subscription_names,
                )
                end_time = datetime.now(UTC)
                duration_ms = (end_time - start_time).total_seconds() * 1000

                # Convert plan to summary format
                dry_run_results = [
                    SubscriptionMigrationResult(
                        subscription_name=pm.subscription_name,
                        success=pm.planned_target_position is not None,
                        source_position=pm.current_position,
                        target_position=pm.planned_target_position,
                        is_exact_translation=pm.is_exact_translation,
                        nearest_source_position=pm.nearest_source_position,
                        error_message=pm.warning if pm.planned_target_position is None else None,
                    )
                    for pm in plan.planned_migrations
                ]

                return MigrationSummary(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    results=dry_run_results,
                    successful_count=plan.migratable_count,
                    failed_count=0,
                    skipped_count=len(plan.skipped_subscriptions),
                    started_at=start_time,
                    completed_at=end_time,
                    duration_ms=duration_ms,
                )

            # Execute actual migrations
            results: list[SubscriptionMigrationResult] = []
            successful_count = 0
            failed_count = 0
            skipped_count = 0

            for name in subscription_names:
                try:
                    result = await self._migrate_single_subscription(
                        migration_id=migration_id,
                        subscription_name=name,
                    )
                except Exception as e:
                    # Position translation and checkpoint-save failures are
                    # already caught inside _migrate_single_subscription and
                    # turned into a failed SubscriptionMigrationResult, so
                    # they never reach here -- what does is something
                    # unexpected (a broken checkpoint repository, for
                    # instance), which is exactly the "critical error" this
                    # method's docstring documents raising for.
                    raise SubscriptionMigrationError(
                        f"Critical error migrating subscription {name}: {e}",
                        migration_id=migration_id,
                        subscription_name=name,
                        reason=str(e),
                    ) from e

                if result is None:
                    skipped_count += 1
                elif result.success:
                    results.append(result)
                    successful_count += 1
                else:
                    results.append(result)
                    failed_count += 1

            end_time = datetime.now(UTC)
            duration_ms = (end_time - start_time).total_seconds() * 1000

            summary = MigrationSummary(
                migration_id=migration_id,
                tenant_id=tenant_id,
                results=results,
                successful_count=successful_count,
                failed_count=failed_count,
                skipped_count=skipped_count,
                started_at=start_time,
                completed_at=end_time,
                duration_ms=duration_ms,
            )

            logger.info(
                "Subscription migration completed",
                extra={
                    "migration_id": str(migration_id),
                    "successful_count": successful_count,
                    "failed_count": failed_count,
                    "skipped_count": skipped_count,
                    "duration_ms": duration_ms,
                },
            )

            return summary

    async def _migrate_single_subscription(
        self,
        migration_id: UUID,
        subscription_name: str,
    ) -> SubscriptionMigrationResult | None:
        """
        Migrate a single subscription's checkpoint.

        Args:
            migration_id: ID of the migration.
            subscription_name: Name of the subscription to migrate.

        Returns:
            SubscriptionMigrationResult or None if subscription was skipped.
        """
        with self._tracer.span(
            "eventsource.subscription_migrator.migrate_single",
            {
                "migration.id": str(migration_id),
                "subscription.name": subscription_name,
            },
        ):
            # The checkpoint repo returns a Position, and the mapper is
            # token-keyed, so it flows straight through with no conversion.
            current_position = await self._checkpoint_repo.get_position(subscription_name)

            if current_position is None:
                logger.debug(
                    "Subscription has no checkpoint, skipping",
                    extra={
                        "subscription": subscription_name,
                        "migration_id": str(migration_id),
                    },
                )
                return None

            # Get the current checkpoint data for event_id and event_type
            checkpoints = await self._checkpoint_repo.get_all_checkpoints()
            checkpoint_data = next(
                (c for c in checkpoints if c.projection_name == subscription_name),
                None,
            )

            if checkpoint_data is None or checkpoint_data.last_event_id is None:
                logger.warning(
                    "Checkpoint data incomplete, skipping",
                    extra={
                        "subscription": subscription_name,
                        "migration_id": str(migration_id),
                    },
                )
                return None

            # Translate position
            try:
                translation = await self._position_mapper.translate_position(
                    migration_id=migration_id,
                    source_position=current_position,
                    use_nearest=True,
                )
            except PositionMappingError as e:
                logger.error(
                    "Failed to translate position",
                    extra={
                        "subscription": subscription_name,
                        "migration_id": str(migration_id),
                        "position": current_position.to_str(),
                        "error": str(e),
                    },
                )
                return SubscriptionMigrationResult(
                    subscription_name=subscription_name,
                    success=False,
                    source_position=current_position,
                    error_message=f"Position translation failed: {e}",
                )

            # Update checkpoint with translated position. translation.target_position
            # is already a Position -- no codec needed to save it.
            try:
                await self._checkpoint_repo.save_position(
                    subscription_id=subscription_name,
                    position=translation.target_position,
                    event_id=checkpoint_data.last_event_id,
                    event_type=checkpoint_data.last_event_type or "Unknown",
                )

                logger.info(
                    "Subscription checkpoint migrated",
                    extra={
                        "subscription": subscription_name,
                        "migration_id": str(migration_id),
                        "source_position": current_position.to_str(),
                        "target_position": translation.target_position.to_str(),
                        "is_exact": translation.is_exact,
                    },
                )

                return SubscriptionMigrationResult(
                    subscription_name=subscription_name,
                    success=True,
                    source_position=current_position,
                    target_position=translation.target_position,
                    is_exact_translation=translation.is_exact,
                    nearest_source_position=translation.nearest_source_position,
                    migrated_at=datetime.now(UTC),
                )

            except Exception as e:
                logger.error(
                    "Failed to update checkpoint",
                    extra={
                        "subscription": subscription_name,
                        "migration_id": str(migration_id),
                        "error": str(e),
                    },
                )
                return SubscriptionMigrationResult(
                    subscription_name=subscription_name,
                    success=False,
                    source_position=current_position,
                    target_position=translation.target_position,
                    error_message=f"Checkpoint update failed: {e}",
                )

    async def migrate_tenant_subscriptions(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        subscription_names: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> MigrationSummary:
        """
        Migrate all subscriptions for a tenant.

        Convenience method that migrates all specified subscriptions
        for a tenant during migration cutover.

        Args:
            migration_id: ID of the migration.
            tenant_id: Tenant being migrated.
            subscription_names: Optional list of subscription names.
                If None, discovers subscriptions from checkpoints.
            dry_run: If True, preview changes without executing.

        Returns:
            MigrationSummary with results.

        Example:
            >>> summary = await migrator.migrate_tenant_subscriptions(
            ...     migration_id=migration.id,
            ...     tenant_id=tenant_id,
            ... )
        """
        with self._tracer.span(
            "eventsource.subscription_migrator.migrate_tenant_subscriptions",
            {
                "migration.id": str(migration_id),
                "tenant.id": str(tenant_id),
                "dry_run": dry_run,
            },
        ):
            # If no subscription names provided, get all from checkpoints
            if subscription_names is None:
                checkpoints = await self._checkpoint_repo.get_all_checkpoints()
                subscription_names = [c.projection_name for c in checkpoints]

            logger.info(
                "Migrating tenant subscriptions",
                extra={
                    "migration_id": str(migration_id),
                    "tenant_id": str(tenant_id),
                    "subscription_count": len(subscription_names),
                    "dry_run": dry_run,
                },
            )

            return await self.migrate_subscriptions(
                migration_id=migration_id,
                tenant_id=tenant_id,
                subscription_names=subscription_names,
                dry_run=dry_run,
            )


__all__ = ["SubscriptionMigratorExecutionMixin"]
