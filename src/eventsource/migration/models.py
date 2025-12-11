"""
Data models for the multi-tenant live migration system.

This module defines the core data structures used throughout the migration
system, including migration state, tenant routing, position mappings, and
status tracking.

Models in this module:

Enums:
    - MigrationPhase: Migration lifecycle phases
    - TenantMigrationState: Tenant routing states during migration

Configuration:
    - MigrationConfig: Configuration for a tenant migration

Core Models:
    - Migration: Represents a tenant migration operation
    - TenantRouting: Tenant routing configuration
    - PositionMapping: Maps source position to target position
    - SyncLag: Synchronization lag between stores
    - CutoverResult: Result of a cutover operation
    - MigrationStatus: Real-time migration status
    - MigrationResult: Final result of a completed migration
    - MigrationAuditEntry: Audit log entry for migration events

See Also:
    - Task: P1-002-migration-models.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID


class MigrationPhase(Enum):
    """
    Migration lifecycle phases.

    State machine transitions:
        PENDING -> BULK_COPY -> DUAL_WRITE -> CUTOVER -> COMPLETED
                                    |
        Any phase ----------------> ABORTED (operator-initiated)
        Any phase ----------------> FAILED (unrecoverable error)

    Valid transitions:
        - PENDING -> BULK_COPY: Migration starts
        - BULK_COPY -> DUAL_WRITE: Historical copy complete
        - DUAL_WRITE -> CUTOVER: Sync lag below threshold
        - CUTOVER -> COMPLETED: Cutover successful
        - Any -> ABORTED: Operator cancels
        - Any -> FAILED: Unrecoverable error

    Attributes:
        PENDING: Migration created but not started.
        BULK_COPY: Copying historical events from source to target.
        DUAL_WRITE: Real-time sync: new events written to both stores.
        CUTOVER: Brief pause while routing switches to target.
        COMPLETED: Migration finished successfully.
        ABORTED: Migration cancelled by operator.
        FAILED: Migration failed due to unrecoverable error.
    """

    PENDING = "pending"
    """Migration created but not started."""

    BULK_COPY = "bulk_copy"
    """Copying historical events from source to target."""

    DUAL_WRITE = "dual_write"
    """Real-time sync: new events written to both stores."""

    CUTOVER = "cutover"
    """Brief pause while routing switches to target."""

    COMPLETED = "completed"
    """Migration finished successfully."""

    ABORTED = "aborted"
    """Migration cancelled by operator."""

    FAILED = "failed"
    """Migration failed due to unrecoverable error."""

    @property
    def is_terminal(self) -> bool:
        """
        Check if this is a terminal (final) phase.

        Terminal phases are COMPLETED, ABORTED, and FAILED.
        Once a migration reaches a terminal phase, it cannot
        transition to any other phase.

        Returns:
            True if this is a terminal phase.
        """
        return self in (
            MigrationPhase.COMPLETED,
            MigrationPhase.ABORTED,
            MigrationPhase.FAILED,
        )

    @property
    def is_active(self) -> bool:
        """
        Check if migration is actively processing.

        Active phases are BULK_COPY, DUAL_WRITE, and CUTOVER.
        During these phases, the migration system is actively
        working on the migration.

        Returns:
            True if migration is actively processing.
        """
        return self in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
            MigrationPhase.CUTOVER,
        )

    @property
    def allows_writes_to_source(self) -> bool:
        """
        Check if writes to source store are allowed in this phase.

        During CUTOVER phase, writes are temporarily blocked.

        Returns:
            True if writes to source are allowed.
        """
        return self != MigrationPhase.CUTOVER

    @property
    def requires_dual_write(self) -> bool:
        """
        Check if this phase requires dual-write to both stores.

        Returns:
            True if dual-write is required.
        """
        return self == MigrationPhase.DUAL_WRITE

    def can_transition_to(self, target: MigrationPhase) -> bool:
        """
        Check if transition to target phase is valid.

        Args:
            target: The target phase to transition to.

        Returns:
            True if the transition is valid.
        """
        # Terminal phases cannot transition
        if self.is_terminal:
            return False

        # Any phase can go to ABORTED or FAILED
        if target in (MigrationPhase.ABORTED, MigrationPhase.FAILED):
            return True

        # Valid forward transitions
        valid_transitions: dict[MigrationPhase, list[MigrationPhase]] = {
            MigrationPhase.PENDING: [MigrationPhase.BULK_COPY],
            MigrationPhase.BULK_COPY: [MigrationPhase.DUAL_WRITE],
            MigrationPhase.DUAL_WRITE: [MigrationPhase.CUTOVER],
            MigrationPhase.CUTOVER: [
                MigrationPhase.COMPLETED,
                MigrationPhase.DUAL_WRITE,  # Rollback if cutover fails
            ],
        }

        return target in valid_transitions.get(self, [])


class AuditEventType(Enum):
    """
    Types of audit events for migration operations.

    These event types correspond to the CHECK constraint in the
    migration_audit_log table and represent significant migration
    lifecycle events for compliance and debugging purposes.

    Attributes:
        MIGRATION_STARTED: A new migration was initiated.
        PHASE_CHANGED: Migration transitioned between phases.
        MIGRATION_PAUSED: Migration was paused by operator.
        MIGRATION_RESUMED: Migration was resumed after pause.
        MIGRATION_ABORTED: Migration was cancelled by operator.
        MIGRATION_COMPLETED: Migration finished successfully.
        MIGRATION_FAILED: Migration failed due to error.
        ERROR_OCCURRED: A recoverable error was recorded.
        CUTOVER_INITIATED: Cutover process started.
        CUTOVER_COMPLETED: Cutover finished successfully.
        CUTOVER_ROLLED_BACK: Cutover was rolled back.
        VERIFICATION_STARTED: Consistency verification began.
        VERIFICATION_COMPLETED: Consistency verification passed.
        VERIFICATION_FAILED: Consistency verification failed.
        PROGRESS_CHECKPOINT: Periodic progress snapshot.
    """

    MIGRATION_STARTED = "migration_started"
    """A new migration was initiated."""

    PHASE_CHANGED = "phase_changed"
    """Migration transitioned between phases."""

    MIGRATION_PAUSED = "migration_paused"
    """Migration was paused by operator."""

    MIGRATION_RESUMED = "migration_resumed"
    """Migration was resumed after pause."""

    MIGRATION_ABORTED = "migration_aborted"
    """Migration was cancelled by operator."""

    MIGRATION_COMPLETED = "migration_completed"
    """Migration finished successfully."""

    MIGRATION_FAILED = "migration_failed"
    """Migration failed due to error."""

    ERROR_OCCURRED = "error_occurred"
    """A recoverable error was recorded."""

    CUTOVER_INITIATED = "cutover_initiated"
    """Cutover process started."""

    CUTOVER_COMPLETED = "cutover_completed"
    """Cutover finished successfully."""

    CUTOVER_ROLLED_BACK = "cutover_rolled_back"
    """Cutover was rolled back."""

    VERIFICATION_STARTED = "verification_started"
    """Consistency verification began."""

    VERIFICATION_COMPLETED = "verification_completed"
    """Consistency verification passed."""

    VERIFICATION_FAILED = "verification_failed"
    """Consistency verification failed."""

    PROGRESS_CHECKPOINT = "progress_checkpoint"
    """Periodic progress snapshot."""


class TenantMigrationState(Enum):
    """
    Tenant routing states during migration.

    These states determine how the TenantStoreRouter handles
    operations for a specific tenant.

    State transitions:
        NORMAL -> BULK_COPY: Migration starts
        BULK_COPY -> DUAL_WRITE: Bulk copy phase begins dual-write
        DUAL_WRITE -> CUTOVER_PAUSED: Cutover initiated
        CUTOVER_PAUSED -> MIGRATED: Cutover successful
        CUTOVER_PAUSED -> DUAL_WRITE: Cutover rollback
        Any -> NORMAL: Migration aborted/failed

    Attributes:
        NORMAL: Route to configured store (no migration active).
        BULK_COPY: Route reads to source; writes to source only.
        DUAL_WRITE: Route writes through DualWriteInterceptor.
        CUTOVER_PAUSED: Block writes, await cutover completion.
        MIGRATED: Route all operations to target store.
    """

    NORMAL = "normal"
    """Route to configured store (no migration active)."""

    BULK_COPY = "bulk_copy"
    """Route reads to source; writes to source only."""

    DUAL_WRITE = "dual_write"
    """Route writes through DualWriteInterceptor."""

    CUTOVER_PAUSED = "cutover_paused"
    """Block writes, await cutover completion."""

    MIGRATED = "migrated"
    """Route all operations to target store."""

    @property
    def is_migrating(self) -> bool:
        """
        Check if tenant is currently in a migration process.

        Returns:
            True if migration is in progress.
        """
        return self in (
            TenantMigrationState.BULK_COPY,
            TenantMigrationState.DUAL_WRITE,
            TenantMigrationState.CUTOVER_PAUSED,
        )

    @property
    def allows_writes(self) -> bool:
        """
        Check if writes are allowed in this state.

        Writes are blocked during CUTOVER_PAUSED to ensure
        consistency during the cutover operation.

        Returns:
            True if writes are allowed.
        """
        return self != TenantMigrationState.CUTOVER_PAUSED

    @property
    def reads_from_target(self) -> bool:
        """
        Check if reads should come from target store.

        After migration completes (MIGRATED state), all reads
        should come from the target store.

        Returns:
            True if reads should come from target.
        """
        return self == TenantMigrationState.MIGRATED

    def can_transition_to(self, target: TenantMigrationState) -> bool:
        """
        Check if transition to target state is valid.

        Args:
            target: The target state to transition to.

        Returns:
            True if the transition is valid.
        """
        # Any state can go back to NORMAL (migration cleanup)
        if target == TenantMigrationState.NORMAL:
            return True

        valid_transitions: dict[TenantMigrationState, list[TenantMigrationState]] = {
            TenantMigrationState.NORMAL: [TenantMigrationState.BULK_COPY],
            TenantMigrationState.BULK_COPY: [TenantMigrationState.DUAL_WRITE],
            TenantMigrationState.DUAL_WRITE: [TenantMigrationState.CUTOVER_PAUSED],
            TenantMigrationState.CUTOVER_PAUSED: [
                TenantMigrationState.MIGRATED,
                TenantMigrationState.DUAL_WRITE,  # Rollback
            ],
            TenantMigrationState.MIGRATED: [],  # Terminal state
        }

        return target in valid_transitions.get(self, [])


@dataclass(frozen=True)
class MigrationConfig:
    """
    Configuration for a tenant migration.

    Controls batch sizes, timeouts, and thresholds for the migration process.
    This class is immutable (frozen) to prevent accidental modification
    during migration.

    Attributes:
        batch_size: Events per batch during bulk copy (default 1000).
        max_bulk_copy_rate: Max events/second during bulk copy (default 10000).
        dual_write_timeout_minutes: Max time in dual-write phase (default 30).
        cutover_max_lag_events: Max lag allowed before cutover (default 100).
        cutover_timeout_ms: Hard timeout for cutover operation (default 500).
        position_mapping_enabled: Whether to record position mappings (default True).
        verify_consistency: Run consistency verification after migration (default True).
        migrate_subscriptions: Migrate subscription checkpoints (default True).

    Example:
        >>> config = MigrationConfig(
        ...     batch_size=500,
        ...     cutover_max_lag_events=50,
        ... )
        >>> config.batch_size
        500
    """

    batch_size: int = 1000
    max_bulk_copy_rate: int = 10000
    dual_write_timeout_minutes: int = 30
    cutover_max_lag_events: int = 100
    cutover_timeout_ms: int = 500
    position_mapping_enabled: bool = True
    verify_consistency: bool = True
    migrate_subscriptions: bool = True

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {self.batch_size}")

        if self.max_bulk_copy_rate < 1:
            raise ValueError(f"max_bulk_copy_rate must be >= 1, got {self.max_bulk_copy_rate}")

        if self.dual_write_timeout_minutes < 1:
            raise ValueError(
                f"dual_write_timeout_minutes must be >= 1, got {self.dual_write_timeout_minutes}"
            )

        if self.cutover_max_lag_events < 0:
            raise ValueError(
                f"cutover_max_lag_events must be >= 0, got {self.cutover_max_lag_events}"
            )

        if self.cutover_timeout_ms < 100:
            raise ValueError(f"cutover_timeout_ms must be >= 100, got {self.cutover_timeout_ms}")

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON storage.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "batch_size": self.batch_size,
            "max_bulk_copy_rate": self.max_bulk_copy_rate,
            "dual_write_timeout_minutes": self.dual_write_timeout_minutes,
            "cutover_max_lag_events": self.cutover_max_lag_events,
            "cutover_timeout_ms": self.cutover_timeout_ms,
            "position_mapping_enabled": self.position_mapping_enabled,
            "verify_consistency": self.verify_consistency,
            "migrate_subscriptions": self.migrate_subscriptions,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MigrationConfig:
        """
        Create from dictionary.

        Args:
            data: Dictionary containing configuration values.

        Returns:
            MigrationConfig instance.
        """
        return cls(
            batch_size=data.get("batch_size", 1000),
            max_bulk_copy_rate=data.get("max_bulk_copy_rate", 10000),
            dual_write_timeout_minutes=data.get("dual_write_timeout_minutes", 30),
            cutover_max_lag_events=data.get("cutover_max_lag_events", 100),
            cutover_timeout_ms=data.get("cutover_timeout_ms", 500),
            position_mapping_enabled=data.get("position_mapping_enabled", True),
            verify_consistency=data.get("verify_consistency", True),
            migrate_subscriptions=data.get("migrate_subscriptions", True),
        )


@dataclass
class Migration:
    """
    Represents a tenant migration.

    Tracks the state and progress of migrating a tenant's events
    from a source store to a target store.

    This is a mutable dataclass because migration state changes
    throughout the migration lifecycle.

    Attributes:
        id: Unique migration identifier.
        tenant_id: Tenant being migrated.
        source_store_id: Source event store identifier.
        target_store_id: Target event store identifier.
        phase: Current migration phase.
        events_total: Total events to migrate.
        events_copied: Events copied so far.
        last_source_position: Last processed position in source.
        last_target_position: Last written position in target.
        started_at: When migration started.
        bulk_copy_started_at: When bulk copy phase started.
        bulk_copy_completed_at: When bulk copy phase completed.
        dual_write_started_at: When dual-write phase started.
        cutover_started_at: When cutover phase started.
        completed_at: When migration completed.
        config: Migration configuration.
        error_count: Number of errors encountered.
        last_error: Last error message.
        last_error_at: When last error occurred.
        is_paused: Whether migration is paused.
        paused_at: When migration was paused.
        pause_reason: Reason for pause.
        created_at: When migration was created.
        updated_at: When migration was last updated.
        created_by: Who created the migration.
    """

    id: UUID
    tenant_id: UUID
    source_store_id: str
    target_store_id: str
    phase: MigrationPhase = MigrationPhase.PENDING
    events_total: int = 0
    events_copied: int = 0
    last_source_position: int = 0
    last_target_position: int = 0
    started_at: datetime | None = None
    bulk_copy_started_at: datetime | None = None
    bulk_copy_completed_at: datetime | None = None
    dual_write_started_at: datetime | None = None
    cutover_started_at: datetime | None = None
    completed_at: datetime | None = None
    config: MigrationConfig = field(default_factory=MigrationConfig)
    error_count: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None
    is_paused: bool = False
    paused_at: datetime | None = None
    pause_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    created_by: str | None = None

    @property
    def progress_percent(self) -> float:
        """
        Calculate progress percentage (0-100).

        Returns:
            Progress as a percentage.
        """
        if self.events_total == 0:
            return 0.0
        return min(100.0, (self.events_copied / self.events_total) * 100)

    @property
    def is_active(self) -> bool:
        """
        Check if migration is active (not terminal).

        Returns:
            True if migration is still active.
        """
        return not self.phase.is_terminal

    @property
    def is_terminal(self) -> bool:
        """
        Check if migration has reached a terminal state.

        Returns:
            True if migration has completed, aborted, or failed.
        """
        return self.phase.is_terminal

    @property
    def events_remaining(self) -> int:
        """
        Calculate events remaining to copy.

        Returns:
            Number of events remaining.
        """
        return max(0, self.events_total - self.events_copied)

    @property
    def duration(self) -> timedelta | None:
        """
        Calculate total migration duration.

        Returns:
            Duration from start to completion or current time.
        """
        if self.started_at is None:
            return None
        end = self.completed_at or datetime.now()
        return end - self.started_at

    @property
    def bulk_copy_duration(self) -> timedelta | None:
        """
        Calculate bulk copy phase duration.

        Returns:
            Duration of bulk copy phase.
        """
        if self.bulk_copy_started_at is None:
            return None
        end = self.bulk_copy_completed_at or datetime.now()
        return end - self.bulk_copy_started_at

    @property
    def current_phase_started_at(self) -> datetime | None:
        """
        Get the start time of the current phase.

        Returns:
            Start time of the current phase.
        """
        phase_start_times = {
            MigrationPhase.BULK_COPY: self.bulk_copy_started_at,
            MigrationPhase.DUAL_WRITE: self.dual_write_started_at,
            MigrationPhase.CUTOVER: self.cutover_started_at,
            MigrationPhase.COMPLETED: self.completed_at,
        }
        return phase_start_times.get(self.phase, self.started_at)

    def can_transition_to(self, target_phase: MigrationPhase) -> bool:
        """
        Check if transition to target phase is valid.

        Args:
            target_phase: The target phase.

        Returns:
            True if the transition is valid.
        """
        return self.phase.can_transition_to(target_phase)


@dataclass
class TenantRouting:
    """
    Tenant routing configuration.

    Determines which store handles operations for a tenant
    and tracks migration state for routing decisions.

    This is a mutable dataclass because routing state changes
    during migration.

    Attributes:
        tenant_id: Tenant identifier.
        store_id: Primary store identifier.
        migration_state: Current migration state.
        active_migration_id: ID of active migration, if any.
        target_store_id: Target store during migration.
        created_at: When routing was created.
        updated_at: When routing was last updated.
    """

    tenant_id: UUID
    store_id: str
    migration_state: TenantMigrationState = TenantMigrationState.NORMAL
    active_migration_id: UUID | None = None
    target_store_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_migrating(self) -> bool:
        """
        Check if tenant is currently migrating.

        Returns:
            True if migration is in progress.
        """
        return self.migration_state.is_migrating

    @property
    def effective_store_id(self) -> str:
        """
        Get the effective store ID for reads.

        After migration completes (MIGRATED state), reads
        should come from the target store.

        Returns:
            The store ID to use for reads.
        """
        if self.migration_state == TenantMigrationState.MIGRATED:
            return self.target_store_id or self.store_id
        return self.store_id

    def can_transition_to(self, target_state: TenantMigrationState) -> bool:
        """
        Check if transition to target state is valid.

        Args:
            target_state: The target state.

        Returns:
            True if the transition is valid.
        """
        return self.migration_state.can_transition_to(target_state)


@dataclass(frozen=True)
class PositionMapping:
    """
    Maps source position to target position.

    Used for checkpoint translation during subscription migration.
    This class is immutable because position mappings should not
    change once created.

    Attributes:
        migration_id: Migration this mapping belongs to.
        source_position: Position in source store.
        target_position: Corresponding position in target store.
        event_id: Event ID at this position.
        mapped_at: When mapping was created.
    """

    migration_id: UUID
    source_position: int
    target_position: int
    event_id: UUID
    mapped_at: datetime


@dataclass(frozen=True)
class SyncLag:
    """
    Synchronization lag between source and target stores.

    Tracks how far behind the target store is during dual-write phase.
    This class is immutable because it represents a point-in-time
    measurement.

    Attributes:
        events: Number of events behind.
        source_position: Current source store position.
        target_position: Current target store position.
        timestamp: When this lag was measured.
    """

    events: int
    """Number of events behind."""

    source_position: int
    """Current source store position."""

    target_position: int
    """Current target store position."""

    timestamp: datetime
    """When this lag was measured."""

    @property
    def is_converged(self) -> bool:
        """
        Check if stores are fully synchronized.

        Returns:
            True if lag is zero.
        """
        return self.events == 0

    @property
    def lag_ms(self) -> float:
        """
        Estimate lag in milliseconds (assuming ~1ms per event).

        This is a rough estimate based on typical event processing time.

        Returns:
            Estimated lag in milliseconds.
        """
        return float(self.events)

    def is_within_threshold(self, max_lag: int) -> bool:
        """
        Check if lag is within acceptable threshold for cutover.

        Args:
            max_lag: Maximum acceptable lag in events.

        Returns:
            True if lag is within threshold.
        """
        return self.events <= max_lag


@dataclass(frozen=True)
class CutoverResult:
    """
    Result of a cutover operation.

    Contains details about whether cutover succeeded and timing.
    This class is immutable because it represents a completed operation.

    Attributes:
        success: Whether cutover succeeded.
        duration_ms: How long cutover took in milliseconds.
        events_synced: Events synced during cutover pause.
        error_message: Error message if failed.
        rolled_back: Whether rollback was performed.
    """

    success: bool
    duration_ms: float
    events_synced: int = 0
    error_message: str | None = None
    rolled_back: bool = False

    @property
    def within_timeout(self) -> bool:
        """
        Check if cutover completed within typical SLA.

        The standard SLA is sub-100ms cutover pause.

        Returns:
            True if cutover was within 100ms.
        """
        return self.duration_ms < 100.0


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


@dataclass(frozen=True)
class MigrationAuditEntry:
    """
    Audit log entry for migration events.

    Used for compliance and debugging. This class is immutable
    because audit entries should never be modified.

    Attributes:
        id: Unique audit entry identifier (None for new entries).
        migration_id: Migration this entry belongs to.
        event_type: Type of audit event.
        old_phase: Previous phase (for phase changes).
        new_phase: New phase (for phase changes).
        details: Additional event details.
        operator: Who triggered the event.
        occurred_at: When the event occurred.
    """

    id: int | None
    migration_id: UUID
    event_type: AuditEventType
    old_phase: MigrationPhase | None
    new_phase: MigrationPhase | None
    details: dict[str, Any] | None
    operator: str | None
    occurred_at: datetime

    @classmethod
    def phase_change(
        cls,
        migration_id: UUID,
        old_phase: MigrationPhase,
        new_phase: MigrationPhase,
        occurred_at: datetime,
        operator: str | None = None,
        details: dict[str, Any] | None = None,
        id: int | None = None,
    ) -> MigrationAuditEntry:
        """
        Create an audit entry for a phase change.

        Args:
            migration_id: Migration ID.
            old_phase: Previous phase.
            new_phase: New phase.
            occurred_at: When change occurred.
            operator: Who triggered the change.
            details: Additional details.
            id: Audit entry ID (optional, set by database).

        Returns:
            MigrationAuditEntry instance.
        """
        return cls(
            id=id,
            migration_id=migration_id,
            event_type=AuditEventType.PHASE_CHANGED,
            old_phase=old_phase,
            new_phase=new_phase,
            details=details,
            operator=operator,
            occurred_at=occurred_at,
        )

    @classmethod
    def migration_started(
        cls,
        migration_id: UUID,
        occurred_at: datetime,
        operator: str | None = None,
        details: dict[str, Any] | None = None,
        id: int | None = None,
    ) -> MigrationAuditEntry:
        """
        Create an audit entry for migration start.

        Args:
            migration_id: Migration ID.
            occurred_at: When migration started.
            operator: Who triggered the migration.
            details: Additional details (config, etc.).
            id: Audit entry ID (optional, set by database).

        Returns:
            MigrationAuditEntry instance.
        """
        return cls(
            id=id,
            migration_id=migration_id,
            event_type=AuditEventType.MIGRATION_STARTED,
            old_phase=None,
            new_phase=MigrationPhase.PENDING,
            details=details,
            operator=operator,
            occurred_at=occurred_at,
        )

    @classmethod
    def error_occurred(
        cls,
        migration_id: UUID,
        occurred_at: datetime,
        error_message: str,
        error_type: str | None = None,
        operator: str | None = None,
        id: int | None = None,
    ) -> MigrationAuditEntry:
        """
        Create an audit entry for an error occurrence.

        Args:
            migration_id: Migration ID.
            occurred_at: When error occurred.
            error_message: The error message.
            error_type: Classification of the error.
            operator: Who was operating when error occurred.
            id: Audit entry ID (optional, set by database).

        Returns:
            MigrationAuditEntry instance.
        """
        return cls(
            id=id,
            migration_id=migration_id,
            event_type=AuditEventType.ERROR_OCCURRED,
            old_phase=None,
            new_phase=None,
            details={
                "error_message": error_message,
                "error_type": error_type,
            },
            operator=operator,
            occurred_at=occurred_at,
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation suitable for JSON.
        """
        return {
            "id": self.id,
            "migration_id": str(self.migration_id),
            "event_type": self.event_type.value,
            "old_phase": self.old_phase.value if self.old_phase else None,
            "new_phase": self.new_phase.value if self.new_phase else None,
            "details": self.details,
            "operator": self.operator,
            "occurred_at": self.occurred_at.isoformat(),
        }
