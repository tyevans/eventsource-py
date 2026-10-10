"""
Subscription migration planning and dry-run preview operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.subscription_migrator_types import (
    MigrationPlan,
    PlannedMigration,
)
from eventsource.application.subscriptions.subscription import render_position

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.observability import Tracer
    from eventsource.ports.checkpoints import CheckpointRepository

logger = logging.getLogger(__name__)


class SubscriptionMigratorPlanningMixin:
    """Mixin providing subscription migration planning and dry-run preview capabilities."""

    _position_mapper: PositionMapper
    _checkpoint_repo: CheckpointRepository
    _tracer: Tracer

    async def plan_migration(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        subscription_names: list[str],
    ) -> MigrationPlan:
        """
        Plan subscription migrations without executing them (dry-run).

        Creates a preview of what migrations would be performed,
        including position translations and any warnings.

        Args:
            migration_id: ID of the migration.
            tenant_id: Tenant being migrated.
            subscription_names: Names of subscriptions to migrate.

        Returns:
            MigrationPlan with details of planned migrations.

        Example:
            >>> plan = await migrator.plan_migration(
            ...     migration_id=migration_id,
            ...     tenant_id=tenant_id,
            ...     subscription_names=["OrderProjection", "InventoryProjection"],
            ... )
            >>> for m in plan.planned_migrations:
            ...     print(f"{m.subscription_name}: {m.current_position} -> {m.planned_target_position}")
        """
        with self._tracer.span(
            "eventsource.subscription_migrator.plan_migration",
            {
                "migration.id": str(migration_id),
                "tenant.id": str(tenant_id),
                "subscription_count": len(subscription_names),
            },
        ):
            logger.info(
                "Planning subscription migration",
                extra={
                    "migration_id": str(migration_id),
                    "tenant_id": str(tenant_id),
                    "subscription_count": len(subscription_names),
                },
            )

            planned_migrations: list[PlannedMigration] = []
            skipped_subscriptions: list[str] = []

            for name in subscription_names:
                planned = await self._plan_single_migration(
                    migration_id=migration_id,
                    subscription_name=name,
                )

                if planned is not None:
                    planned_migrations.append(planned)
                else:
                    skipped_subscriptions.append(name)

            plan = MigrationPlan(
                migration_id=migration_id,
                tenant_id=tenant_id,
                planned_migrations=planned_migrations,
                skipped_subscriptions=skipped_subscriptions,
                total_subscriptions=len(subscription_names),
                migratable_count=len(planned_migrations),
            )

            logger.info(
                "Migration plan created",
                extra={
                    "migration_id": str(migration_id),
                    "migratable_count": plan.migratable_count,
                    "skipped_count": len(skipped_subscriptions),
                },
            )

            return plan

    async def _plan_single_migration(
        self,
        migration_id: UUID,
        subscription_name: str,
    ) -> PlannedMigration | None:
        """
        Plan migration for a single subscription.

        Args:
            migration_id: ID of the migration.
            subscription_name: Name of the subscription.

        Returns:
            PlannedMigration or None if subscription should be skipped.
        """
        # The checkpoint repo returns a Position, and the mapper is token-keyed,
        # so it flows straight through with no conversion.
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

        # Try to translate position
        try:
            translation = await self._position_mapper.translate_position(
                migration_id=migration_id,
                source_position=current_position,
                use_nearest=True,
            )

            warning = None
            if not translation.is_exact:
                warning = (
                    f"Using nearest position mapping: source {current_position.to_str()} "
                    f"-> nearest {render_position(translation.nearest_source_position)}"
                )

            return PlannedMigration(
                subscription_name=subscription_name,
                current_position=current_position,
                planned_target_position=translation.target_position,
                is_exact_translation=translation.is_exact,
                nearest_source_position=translation.nearest_source_position,
                warning=warning,
            )

        except PositionMappingError as e:
            logger.warning(
                "Cannot translate position for subscription",
                extra={
                    "subscription": subscription_name,
                    "migration_id": str(migration_id),
                    "position": current_position.to_str(),
                    "error": str(e),
                },
            )
            return PlannedMigration(
                subscription_name=subscription_name,
                current_position=current_position,
                warning=f"Cannot translate position: {e}",
            )


__all__ = ["SubscriptionMigratorPlanningMixin"]
