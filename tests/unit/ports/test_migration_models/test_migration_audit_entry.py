"""Tests for MigrationAuditEntry dataclass."""

from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)


class TestMigrationAuditEntry:
    """Tests for MigrationAuditEntry dataclass."""

    def test_creation(self) -> None:
        """Test creating a MigrationAuditEntry."""
        migration_id = uuid4()
        occurred_at = datetime.now()

        entry = MigrationAuditEntry(
            id=1,
            migration_id=migration_id,
            event_type=AuditEventType.PHASE_CHANGED,
            old_phase=MigrationPhase.PENDING,
            new_phase=MigrationPhase.BULK_COPY,
            details={"reason": "Manual start"},
            operator="admin@example.com",
            occurred_at=occurred_at,
        )

        assert entry.id == 1
        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY

    def test_phase_change_factory(self) -> None:
        """Test phase_change factory method."""
        migration_id = uuid4()
        occurred_at = datetime.now()

        entry = MigrationAuditEntry.phase_change(
            migration_id=migration_id,
            old_phase=MigrationPhase.BULK_COPY,
            new_phase=MigrationPhase.DUAL_WRITE,
            occurred_at=occurred_at,
            operator="system",
            id=1,
        )

        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.BULK_COPY
        assert entry.new_phase == MigrationPhase.DUAL_WRITE
        assert entry.operator == "system"

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        migration_id = uuid4()
        occurred_at = datetime(2024, 1, 1, 10, 0, 0)

        entry = MigrationAuditEntry(
            id=1,
            migration_id=migration_id,
            event_type=AuditEventType.PHASE_CHANGED,
            old_phase=MigrationPhase.PENDING,
            new_phase=MigrationPhase.BULK_COPY,
            details={"key": "value"},
            operator="admin",
            occurred_at=occurred_at,
        )

        data = entry.to_dict()
        assert data["id"] == 1
        assert data["migration_id"] == str(migration_id)
        assert data["event_type"] == "phase_changed"
        assert data["old_phase"] == "pending"
        assert data["new_phase"] == "bulk_copy"
        assert data["details"] == {"key": "value"}
        assert data["operator"] == "admin"
        assert data["occurred_at"] == "2024-01-01T10:00:00"

    def test_to_dict_with_none_phases(self) -> None:
        """Test to_dict with None phases."""
        entry = MigrationAuditEntry(
            id=1,
            migration_id=uuid4(),
            event_type=AuditEventType.MIGRATION_STARTED,
            old_phase=None,
            new_phase=None,
            details=None,
            operator=None,
            occurred_at=datetime.now(),
        )

        data = entry.to_dict()
        assert data["old_phase"] is None
        assert data["new_phase"] is None
        assert data["details"] is None
        assert data["operator"] is None

    def test_is_frozen(self) -> None:
        """Test that MigrationAuditEntry is frozen (immutable)."""
        entry = MigrationAuditEntry(
            id=1,
            migration_id=uuid4(),
            event_type=AuditEventType.ERROR_OCCURRED,
            old_phase=None,
            new_phase=None,
            details=None,
            operator=None,
            occurred_at=datetime.now(),
        )
        with pytest.raises(AttributeError):
            entry.id = 2  # type: ignore[misc]
