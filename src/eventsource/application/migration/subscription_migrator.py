"""
SubscriptionMigrator - Migrates subscriptions with position translation.

The SubscriptionMigrator handles the migration of active subscriptions
from source to target store, ensuring subscribers continue from the
correct position without missing events or processing duplicates.

This module provides:
- SubscriptionMigrator: Main class for subscription migration
- SubscriptionMigrationResult: Result of a single subscription migration
- MigrationPlan: Preview of planned migrations (dry-run)
- MigrationSummary: Summary of completed migration operations

Responsibilities:
    - Identify active subscriptions for migrating tenant
    - Translate checkpoint positions using PositionMapper
    - Update subscription configurations atomically
    - Handle subscription handoff with minimal disruption
    - Support dry-run mode to preview changes

Migration Strategy:
    - Pause subscription processing briefly during cutover
    - Translate last processed position to target store
    - Update subscription to point to target store
    - Resume processing from translated position

Usage:
    >>> from eventsource.application.migration import SubscriptionMigrator
    >>>
    >>> migrator = SubscriptionMigrator(
    ...     position_mapper=position_mapper,
    ...     checkpoint_repo=checkpoint_repo,
    ... )
    >>>
    >>> # Preview changes (dry-run)
    >>> plan = await migrator.plan_migration(
    ...     migration_id=migration.id,
    ...     tenant_id=tenant_id,
    ...     subscription_names=["OrderProjection", "InventoryProjection"],
    ... )
    >>>
    >>> # Execute migration
    >>> summary = await migrator.migrate_subscriptions(
    ...     migration_id=migration.id,
    ...     tenant_id=tenant_id,
    ...     subscription_names=["OrderProjection", "InventoryProjection"],
    ... )

See Also:
    - Task: P3-004-subscription-migrator.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventsource.application.migration.subscription_migrator_execution import (
    SubscriptionMigratorExecutionMixin,
)
from eventsource.application.migration.subscription_migrator_planning import (
    SubscriptionMigratorPlanningMixin,
)
from eventsource.application.migration.subscription_migrator_types import (
    MigrationPlan,
    MigrationSummary,
    PlannedMigration,
    SubscriptionMigrationError,
    SubscriptionMigrationResult,
)
from eventsource.application.migration.subscription_migrator_verification import (
    SubscriptionMigratorVerificationMixin,
)
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.ports.checkpoints import CheckpointRepository


class SubscriptionMigrator(
    SubscriptionMigratorPlanningMixin,
    SubscriptionMigratorExecutionMixin,
    SubscriptionMigratorVerificationMixin,
):
    """
    Migrates subscription checkpoints from source to target store positions.

    Ensures subscription continuity by translating positions and
    updating checkpoints atomically during migration cutover.

    The migrator supports:
    - Dry-run mode to preview changes without applying them
    - Batch migration of multiple subscriptions
    - Atomic checkpoint updates with rollback on failure
    - Detailed logging and tracing for observability

    Example:
        >>> migrator = SubscriptionMigrator(
        ...     position_mapper=position_mapper,
        ...     checkpoint_repo=checkpoint_repo,
        ... )
        >>>
        >>> # Preview migrations (dry-run)
        >>> plan = await migrator.plan_migration(
        ...     migration_id=migration_id,
        ...     tenant_id=tenant_id,
        ...     subscription_names=["OrderProjection"],
        ... )
        >>> print(f"Would migrate {plan.migratable_count} subscriptions")
        >>>
        >>> # Execute migrations
        >>> summary = await migrator.migrate_subscriptions(
        ...     migration_id=migration_id,
        ...     tenant_id=tenant_id,
        ...     subscription_names=["OrderProjection"],
        ... )
        >>> print(f"Migrated {summary.successful_count} subscriptions")

    Attributes:
        _position_mapper: Mapper for translating positions.
        _checkpoint_repo: Repository for checkpoint operations.
    """

    def __init__(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: CheckpointRepository,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the subscription migrator.

        Args:
            position_mapper: Position mapper for checkpoint translation.
            checkpoint_repo: Repository for reading/writing checkpoints.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._position_mapper = position_mapper
        self._checkpoint_repo = checkpoint_repo


__all__ = [
    "MigrationPlan",
    "MigrationSummary",
    "PlannedMigration",
    "SubscriptionMigrationError",
    "SubscriptionMigrationResult",
    "SubscriptionMigrator",
]
