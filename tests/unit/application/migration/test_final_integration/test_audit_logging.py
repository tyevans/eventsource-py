"""Audit logging tests throughout migration lifecycle."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)

if TYPE_CHECKING:
    from tests.unit.application.migration.test_final_integration._mocks import (
        InMemoryAuditLogRepository,
    )


class TestAuditLoggingThroughoutMigration:
    """Tests for audit logging throughout the migration process."""

    @pytest.mark.asyncio
    async def test_audit_log_records_all_phase_transitions(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records all phase transitions."""
        phases = [
            (None, MigrationPhase.PENDING),
            (MigrationPhase.PENDING, MigrationPhase.BULK_COPY),
            (MigrationPhase.BULK_COPY, MigrationPhase.DUAL_WRITE),
            (MigrationPhase.DUAL_WRITE, MigrationPhase.CUTOVER),
            (MigrationPhase.CUTOVER, MigrationPhase.COMPLETED),
        ]

        for old_phase, new_phase in phases:
            await audit_log_repo.record(
                MigrationAuditEntry.phase_change(
                    migration_id=migration_id,
                    old_phase=old_phase,
                    new_phase=new_phase,
                    occurred_at=datetime.now(UTC),
                    operator="system",
                )
            )

        # Verify all phase transitions were recorded
        entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.PHASE_CHANGED],
        )

        assert len(entries) == 5

        # Verify order is correct (ascending by occurred_at)
        for i, (old_phase, new_phase) in enumerate(phases):
            assert entries[i].old_phase == old_phase
            assert entries[i].new_phase == new_phase

    @pytest.mark.asyncio
    async def test_audit_log_records_operator_actions(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records operator actions (pause, resume, abort)."""
        # Record pause action
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.MIGRATION_PAUSED,
                old_phase=None,
                new_phase=None,
                details={"reason": "Maintenance window"},
                operator="admin@example.com",
                occurred_at=datetime.now(UTC),
            )
        )

        # Record resume action
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.MIGRATION_RESUMED,
                old_phase=None,
                new_phase=None,
                details={"resumed_after_seconds": 300},
                operator="admin@example.com",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify entries
        entries = await audit_log_repo.get_by_migration(migration_id)
        assert len(entries) == 2

        pause_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.MIGRATION_PAUSED],
        )
        assert len(pause_entries) == 1
        assert pause_entries[0].operator == "admin@example.com"
        assert pause_entries[0].details["reason"] == "Maintenance window"

        resume_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.MIGRATION_RESUMED],
        )
        assert len(resume_entries) == 1

    @pytest.mark.asyncio
    async def test_audit_log_records_errors(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records errors during migration."""
        await audit_log_repo.record(
            MigrationAuditEntry.error_occurred(
                migration_id=migration_id,
                occurred_at=datetime.now(UTC),
                error_message="Connection timeout to target store",
                error_type="NetworkError",
                operator="system",
            )
        )

        # Verify error was recorded
        error_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.ERROR_OCCURRED],
        )

        assert len(error_entries) == 1
        assert "Connection timeout" in error_entries[0].details["error_message"]
        assert error_entries[0].details["error_type"] == "NetworkError"

    @pytest.mark.asyncio
    async def test_audit_log_records_cutover_events(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records cutover-specific events."""
        # Record cutover initiated
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.CUTOVER_INITIATED,
                old_phase=MigrationPhase.DUAL_WRITE,
                new_phase=MigrationPhase.CUTOVER,
                details={"sync_lag": 0},
                operator="admin@example.com",
                occurred_at=datetime.now(UTC),
            )
        )

        # Record cutover completed
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.CUTOVER_COMPLETED,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.COMPLETED,
                details={"duration_ms": 45.5},
                operator="system",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify cutover events
        entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[
                AuditEventType.CUTOVER_INITIATED,
                AuditEventType.CUTOVER_COMPLETED,
            ],
        )

        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_audit_log_get_latest_entry(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test getting the latest audit entry."""
        # Record multiple entries
        for i in range(5):
            await audit_log_repo.record(
                MigrationAuditEntry(
                    id=None,
                    migration_id=migration_id,
                    event_type=AuditEventType.PROGRESS_CHECKPOINT,
                    old_phase=None,
                    new_phase=None,
                    details={"checkpoint": i},
                    operator="system",
                    occurred_at=datetime.now(UTC),
                )
            )

        latest = await audit_log_repo.get_latest(migration_id)
        assert latest is not None
        assert latest.details["checkpoint"] == 4

        # Get latest of specific type
        await audit_log_repo.record(
            MigrationAuditEntry.error_occurred(
                migration_id=migration_id,
                occurred_at=datetime.now(UTC),
                error_message="Test error",
                error_type="TestError",
            )
        )

        latest_error = await audit_log_repo.get_latest(
            migration_id,
            event_type=AuditEventType.ERROR_OCCURRED,
        )
        assert latest_error is not None
        assert latest_error.event_type == AuditEventType.ERROR_OCCURRED


__all__ = [  # pragma: allowlist secret
    "TestAuditLoggingThroughoutMigration",
]
