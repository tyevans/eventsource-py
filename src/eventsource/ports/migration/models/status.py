"""
Migration monitoring status and outcome result models.

This module defines models for real-time migration status reporting
and final migration completion results.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from eventsource.ports.migration.models.migration import Migration
from eventsource.ports.migration.models.phases import MigrationPhase
from eventsource.ports.migration.models.routing import SyncLag


@dataclass(frozen=True)
class MigrationStatus:
    """
    Real-time migration status.

    Provides comprehensive status information for monitoring.
    This class is immutable because it represents a snapshot.

    Attributes:
        migration_id: Migration identifier.
        tenant_id: Tenant being migrated.
        phase: Current migration phase.
        progress_percent: Progress percentage (0-100).
        events_total: Total events to migrate.
        events_copied: Events copied so far.
        events_remaining: Events remaining to copy.
        sync_lag_events: Current sync lag in events.
        sync_lag_ms: Estimated sync lag in milliseconds.
        error_count: Number of errors encountered.
        started_at: When migration started.
        phase_started_at: When current phase started.
        estimated_completion: Estimated completion time.
        current_rate_events_per_sec: Current processing rate.
        is_paused: Whether migration is paused.
    """

    migration_id: UUID
    tenant_id: UUID
    phase: MigrationPhase
    progress_percent: float
    events_total: int
    events_copied: int
    events_remaining: int
    sync_lag_events: int
    sync_lag_ms: float
    error_count: int
    started_at: datetime | None
    phase_started_at: datetime | None
    estimated_completion: datetime | None
    current_rate_events_per_sec: float
    is_paused: bool

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation suitable for JSON.
        """
        return {
            "migration_id": str(self.migration_id),
            "tenant_id": str(self.tenant_id),
            "phase": self.phase.value,
            "progress_percent": self.progress_percent,
            "events_total": self.events_total,
            "events_copied": self.events_copied,
            "events_remaining": self.events_remaining,
            "sync_lag_events": self.sync_lag_events,
            "sync_lag_ms": self.sync_lag_ms,
            "error_count": self.error_count,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "phase_started_at": (
                self.phase_started_at.isoformat() if self.phase_started_at else None
            ),
            "estimated_completion": (
                self.estimated_completion.isoformat() if self.estimated_completion else None
            ),
            "current_rate_events_per_sec": self.current_rate_events_per_sec,
            "is_paused": self.is_paused,
        }

    @classmethod
    def from_migration(
        cls,
        migration: Migration,
        sync_lag: SyncLag | None = None,
        rate_events_per_sec: float = 0.0,
        estimated_completion: datetime | None = None,
    ) -> MigrationStatus:
        """
        Create status from a Migration instance.

        Args:
            migration: The migration to create status for.
            sync_lag: Current sync lag, if available.
            rate_events_per_sec: Current processing rate.
            estimated_completion: Estimated completion time.

        Returns:
            MigrationStatus instance.
        """
        lag_events = sync_lag.events if sync_lag else 0
        lag_ms = sync_lag.lag_ms if sync_lag else 0.0

        return cls(
            migration_id=migration.id,
            tenant_id=migration.tenant_id,
            phase=migration.phase,
            progress_percent=migration.progress_percent,
            events_total=migration.events_total,
            events_copied=migration.events_copied,
            events_remaining=migration.events_remaining,
            sync_lag_events=lag_events,
            sync_lag_ms=lag_ms,
            error_count=migration.error_count,
            started_at=migration.started_at,
            phase_started_at=migration.current_phase_started_at,
            estimated_completion=estimated_completion,
            current_rate_events_per_sec=rate_events_per_sec,
            is_paused=migration.is_paused,
        )


@dataclass(frozen=True)
class MigrationResult:
    """
    Final result of a completed migration.

    Contains summary information about the migration outcome.
    This class is immutable because it represents a completed operation.

    Attributes:
        migration_id: Migration identifier.
        success: Whether migration succeeded.
        duration_seconds: Total duration in seconds.
        events_migrated: Total events migrated.
        final_phase: Final migration phase.
        error_message: Error message if failed.
        consistency_verified: Whether consistency was verified.
        subscriptions_migrated: Number of subscriptions migrated.
    """

    migration_id: UUID
    success: bool
    duration_seconds: float
    events_migrated: int
    final_phase: MigrationPhase
    error_message: str | None = None
    consistency_verified: bool = False
    subscriptions_migrated: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation suitable for JSON.
        """
        return {
            "migration_id": str(self.migration_id),
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "events_migrated": self.events_migrated,
            "final_phase": self.final_phase.value,
            "error_message": self.error_message,
            "consistency_verified": self.consistency_verified,
            "subscriptions_migrated": self.subscriptions_migrated,
        }

    @classmethod
    def from_migration(cls, migration: Migration) -> MigrationResult:
        """
        Create result from a completed Migration instance.

        Args:
            migration: The completed migration.

        Returns:
            MigrationResult instance.
        """
        duration = migration.duration
        duration_seconds = duration.total_seconds() if duration else 0.0

        return cls(
            migration_id=migration.id,
            success=migration.phase == MigrationPhase.COMPLETED,
            duration_seconds=duration_seconds,
            events_migrated=migration.events_copied,
            final_phase=migration.phase,
            error_message=migration.last_error,
            consistency_verified=migration.config.verify_consistency,
        )


__all__ = [
    "MigrationResult",
    "MigrationStatus",
]
