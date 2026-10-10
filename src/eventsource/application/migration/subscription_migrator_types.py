"""
Subscription migration data structures and exceptions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.migration.exceptions import MigrationError

if TYPE_CHECKING:
    from eventsource.ports.positions import Position


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
    source_position: Position
    target_position: Position | None = None
    is_exact_translation: bool = False
    nearest_source_position: Position | None = None
    error_message: str | None = None
    migrated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "subscription_name": self.subscription_name,
            "success": self.success,
            "source_position": self.source_position.to_str(),
            "target_position": self.target_position.to_str() if self.target_position else None,
            "is_exact_translation": self.is_exact_translation,
            "nearest_source_position": (
                self.nearest_source_position.to_str() if self.nearest_source_position else None
            ),
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
    current_position: Position
    planned_target_position: Position | None = None
    is_exact_translation: bool = False
    nearest_source_position: Position | None = None
    warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "subscription_name": self.subscription_name,
            "current_position": self.current_position.to_str(),
            "planned_target_position": (
                self.planned_target_position.to_str() if self.planned_target_position else None
            ),
            "is_exact_translation": self.is_exact_translation,
            "nearest_source_position": (
                self.nearest_source_position.to_str() if self.nearest_source_position else None
            ),
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


__all__ = [
    "MigrationPlan",
    "MigrationSummary",
    "PlannedMigration",
    "SubscriptionMigrationError",
    "SubscriptionMigrationResult",
]
