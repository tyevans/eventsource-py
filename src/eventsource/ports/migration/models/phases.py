"""
Migration lifecycle and tenant routing phase state machines.

This module defines the phase enums and state machines governing
tenant migrations and tenant store routing.
"""

from __future__ import annotations

from enum import Enum


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


__all__ = [
    "MigrationPhase",
    "TenantMigrationState",
]
