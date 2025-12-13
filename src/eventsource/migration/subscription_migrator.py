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
    >>> from eventsource.migration import SubscriptionMigrator
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

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.migration.exceptions import MigrationError, PositionMappingError
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.migration.position_mapper import PositionMapper
    from eventsource.repositories.checkpoint import CheckpointRepository

logger = logging.getLogger(__name__)


class SubscriptionMigrationError(MigrationError):
    """
    Raised when subscription migration fails.

    This error indicates a failure during the subscription checkpoint
    migration process, such as position translation or checkpoint update.

    Attributes:
        subscription_name: The subscription that failed to migrate.
        reason: Detailed reason for the failure.
    """

    def __init__(
        self,
        message: str,
        migration_id: UUID,
        subscription_name: str,
        reason: str | None = None,
    ) -> None:
        self.subscription_name = subscription_name
        self.reason = reason
        super().__init__(
            message=message,
            migration_id=migration_id,
            recoverable=True,
            suggested_action="Check position mappings and retry subscription migration",
        )


@dataclass(frozen=True)
class SubscriptionMigrationResult:
    """
    Result of migrating a single subscription.

    Contains details about the checkpoint translation and
    whether the update was successful.

    Attributes:
        subscription_name: Name of the migrated subscription.
        success: Whether the migration succeeded.
        source_position: Original position in source store.
        target_position: Translated position in target store.
        is_exact_translation: Whether the position translation was exact.
        nearest_source_position: Source position used if not exact.
        error_message: Error message if migration failed.
        migrated_at: When the migration was completed.
    """

    subscription_name: str
    success: bool
    source_position: int
    target_position: int | None = None
    is_exact_translation: bool = False
    nearest_source_position: int | None = None
    error_message: str | None = None
    migrated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "subscription_name": self.subscription_name,
            "success": self.success,
            "source_position": self.source_position,
            "target_position": self.target_position,
            "is_exact_translation": self.is_exact_translation,
            "nearest_source_position": self.nearest_source_position,
            "error_message": self.error_message,
            "migrated_at": self.migrated_at.isoformat() if self.migrated_at else None,
        }


@dataclass(frozen=True)
class PlannedMigration:
    """
    A planned subscription migration (dry-run preview).

    Represents what would happen if migration were executed.

    Attributes:
        subscription_name: Name of the subscription.
        current_position: Current checkpoint position in source store.
        planned_target_position: Position that would be set in target store.
        is_exact_translation: Whether translation would be exact.
        nearest_source_position: Source position that would be used if not exact.
        warning: Any warnings about the planned migration.
    """

    subscription_name: str
    current_position: int
    planned_target_position: int | None = None
    is_exact_translation: bool = False
    nearest_source_position: int | None = None
    warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "subscription_name": self.subscription_name,
            "current_position": self.current_position,
            "planned_target_position": self.planned_target_position,
            "is_exact_translation": self.is_exact_translation,
            "nearest_source_position": self.nearest_source_position,
            "warning": self.warning,
        }


@dataclass(frozen=True)
class MigrationPlan:
    """
    Plan for subscription migrations (dry-run result).

    Provides a preview of what migrations would be performed
    without actually executing them.

    Attributes:
        migration_id: ID of the migration.
        tenant_id: Tenant being migrated.
        planned_migrations: List of planned migrations.
        skipped_subscriptions: Subscriptions that would be skipped.
        total_subscriptions: Total subscriptions found.
        migratable_count: Number of subscriptions that can be migrated.
        created_at: When the plan was created.
    """

    migration_id: UUID
    tenant_id: UUID
    planned_migrations: list[PlannedMigration]
    skipped_subscriptions: list[str] = field(default_factory=list)
    total_subscriptions: int = 0
    migratable_count: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "migration_id": str(self.migration_id),
            "tenant_id": str(self.tenant_id),
            "planned_migrations": [m.to_dict() for m in self.planned_migrations],
            "skipped_subscriptions": self.skipped_subscriptions,
            "total_subscriptions": self.total_subscriptions,
            "migratable_count": self.migratable_count,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class MigrationSummary:
    """
    Summary of completed subscription migrations.

    Provides aggregate results and details about each
    subscription that was migrated.

    Attributes:
        migration_id: ID of the migration.
        tenant_id: Tenant that was migrated.
        results: Individual results for each subscription.
        successful_count: Number of successful migrations.
        failed_count: Number of failed migrations.
        skipped_count: Number of skipped subscriptions.
        started_at: When the migration started.
        completed_at: When the migration completed.
        duration_ms: Duration in milliseconds.
    """

    migration_id: UUID
    tenant_id: UUID
    results: list[SubscriptionMigrationResult]
    successful_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    duration_ms: float = 0.0

    @property
    def all_successful(self) -> bool:
        """Check if all migrations succeeded."""
        return self.failed_count == 0 and self.successful_count > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "migration_id": str(self.migration_id),
            "tenant_id": str(self.tenant_id),
            "results": [r.to_dict() for r in self.results],
            "successful_count": self.successful_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "all_successful": self.all_successful,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
        }


class SubscriptionMigrator:
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
        # Get current checkpoint position
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
                    f"Using nearest position mapping: source {current_position} "
                    f"-> nearest {translation.nearest_source_position}"
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
                    "position": current_position,
                    "error": str(e),
                },
            )
            return PlannedMigration(
                subscription_name=subscription_name,
                current_position=current_position,
                warning=f"Cannot translate position: {e}",
            )

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
                result = await self._migrate_single_subscription(
                    migration_id=migration_id,
                    subscription_name=name,
                )

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
            # Get current checkpoint
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
                        "position": current_position,
                        "error": str(e),
                    },
                )
                return SubscriptionMigrationResult(
                    subscription_name=subscription_name,
                    success=False,
                    source_position=current_position,
                    error_message=f"Position translation failed: {e}",
                )

            # Update checkpoint with translated position
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
                        "source_position": current_position,
                        "target_position": translation.target_position,
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

    async def verify_migration(
        self,
        migration_id: UUID,
        subscription_names: list[str],
    ) -> dict[str, bool]:
        """
        Verify that subscription checkpoints were migrated correctly.

        Checks that each subscription's checkpoint position exists
        and is valid after migration.

        Args:
            migration_id: ID of the migration.
            subscription_names: Names of subscriptions to verify.

        Returns:
            Dictionary mapping subscription names to verification status.

        Example:
            >>> verification = await migrator.verify_migration(
            ...     migration_id=migration_id,
            ...     subscription_names=["OrderProjection"],
            ... )
            >>> all_verified = all(verification.values())
        """
        with self._tracer.span(
            "eventsource.subscription_migrator.verify_migration",
            {
                "migration.id": str(migration_id),
                "subscription_count": len(subscription_names),
            },
        ):
            results: dict[str, bool] = {}

            for name in subscription_names:
                position = await self._checkpoint_repo.get_position(name)
                results[name] = position is not None

                if position is None:
                    logger.warning(
                        "Subscription checkpoint not found after migration",
                        extra={
                            "subscription": name,
                            "migration_id": str(migration_id),
                        },
                    )
                else:
                    logger.debug(
                        "Subscription checkpoint verified",
                        extra={
                            "subscription": name,
                            "migration_id": str(migration_id),
                            "position": position,
                        },
                    )

            verified_count = sum(1 for v in results.values() if v)
            logger.info(
                "Migration verification completed",
                extra={
                    "migration_id": str(migration_id),
                    "verified_count": verified_count,
                    "total_count": len(subscription_names),
                },
            )

            return results


__all__ = [
    "SubscriptionMigrator",
    "SubscriptionMigrationResult",
    "PlannedMigration",
    "MigrationPlan",
    "MigrationSummary",
    "SubscriptionMigrationError",
]
