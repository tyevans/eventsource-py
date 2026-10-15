"""Migration lifecycle and tenant routing phase state machines."""

from __future__ import annotations

from enum import Enum


class MigrationPhase(Enum):
    """
    Migration lifecycle phases.

    State machine transitions:
        PENDING -> BULK_COPY -> DUAL_WRITE -> CUTOVER -> COMPLETED
        Any non-terminal phase -> ABORTED (operator-initiated) or FAILED (unrecoverable error)
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
        """Check if this is a terminal (final) phase (COMPLETED, ABORTED, or FAILED)."""
        return self in (
            MigrationPhase.COMPLETED,
            MigrationPhase.ABORTED,
            MigrationPhase.FAILED,
        )

    @property
    def is_active(self) -> bool:
        """Check if migration is actively processing (BULK_COPY, DUAL_WRITE, or CUTOVER)."""
        return self in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
            MigrationPhase.CUTOVER,
        )

    @property
    def allows_writes_to_source(self) -> bool:
        """Check if writes to source store are allowed in this phase (blocked in CUTOVER)."""
        return self != MigrationPhase.CUTOVER

    @property
    def requires_dual_write(self) -> bool:
        """Check if this phase requires dual-write to both stores."""
        return self == MigrationPhase.DUAL_WRITE

    def can_transition_to(self, target: MigrationPhase) -> bool:
        """Check if transition to target phase is valid."""
        if self.is_terminal:
            return False
        if target in (MigrationPhase.ABORTED, MigrationPhase.FAILED):
            return True

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

    State transitions:
        NORMAL -> BULK_COPY -> DUAL_WRITE -> CUTOVER_PAUSED -> MIGRATED
        CUTOVER_PAUSED -> DUAL_WRITE (rollback)
        Any -> NORMAL (aborted/failed cleanup)
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
        """Check if tenant is currently in a migration process."""
        return self in (
            TenantMigrationState.BULK_COPY,
            TenantMigrationState.DUAL_WRITE,
            TenantMigrationState.CUTOVER_PAUSED,
        )

    @property
    def allows_writes(self) -> bool:
        """Check if writes are allowed in this state (blocked during CUTOVER_PAUSED)."""
        return self != TenantMigrationState.CUTOVER_PAUSED

    @property
    def reads_from_target(self) -> bool:
        """Check if reads should come from target store (True when MIGRATED)."""
        return self == TenantMigrationState.MIGRATED

    def can_transition_to(self, target: TenantMigrationState) -> bool:
        """Check if transition to target state is valid."""
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
