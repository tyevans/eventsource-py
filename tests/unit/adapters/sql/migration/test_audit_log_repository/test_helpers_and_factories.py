"""Tests for helper methods, entry factory methods, and AuditEventType enum."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.audit_log import (
    PostgreSQLMigrationAuditLogRepository,
)
from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)


class TestPostgreSQLMigrationAuditLogRepositoryHelpers:
    """Tests for PostgreSQLMigrationAuditLogRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    def test_row_to_entry_with_all_fields(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting a complete row to entry."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            1,  # id
            migration_id,  # migration_id
            "phase_changed",  # event_type
            "pending",  # old_phase
            "bulk_copy",  # new_phase
            json.dumps({"key": "value"}),  # details (JSON string)
            "admin@example.com",  # operator
            now,  # occurred_at
        )

        entry = repo._row_to_entry(row)

        assert entry.id == 1
        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY
        assert entry.details == {"key": "value"}
        assert entry.operator == "admin@example.com"
        assert entry.occurred_at == now

    def test_row_to_entry_with_dict_details(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting row with details already as dict."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            1,
            migration_id,
            "error_occurred",
            None,
            None,
            {"error": "test"},  # dict instead of JSON string
            "system",
            now,
        )

        entry = repo._row_to_entry(row)

        assert entry.details == {"error": "test"}

    def test_row_to_entry_with_null_phases(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting row with null phases."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            1,
            migration_id,
            "error_occurred",
            None,  # old_phase
            None,  # new_phase
            None,  # details
            "system",
            now,
        )

        entry = repo._row_to_entry(row)

        assert entry.old_phase is None
        assert entry.new_phase is None
        assert entry.details is None

    def test_row_to_entry_with_all_event_types(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting rows with different event types."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        event_types = [
            "migration_started",
            "phase_changed",
            "migration_paused",
            "migration_resumed",
            "migration_aborted",
            "migration_completed",
            "migration_failed",
            "error_occurred",
            "cutover_initiated",
            "cutover_completed",
            "cutover_rolled_back",
            "verification_started",
            "verification_completed",
            "verification_failed",
            "progress_checkpoint",
        ]

        for event_type in event_types:
            row = (1, migration_id, event_type, None, None, None, "system", now)
            entry = repo._row_to_entry(row)
            assert entry.event_type.value == event_type


class TestMigrationAuditEntryFactoryMethods:
    """Tests for MigrationAuditEntry factory methods."""

    def test_phase_change_factory(self) -> None:
        """Test phase_change factory method."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry.phase_change(
            migration_id=migration_id,
            old_phase=MigrationPhase.PENDING,
            new_phase=MigrationPhase.BULK_COPY,
            occurred_at=now,
            operator="admin",
            details={"reason": "Manual start"},
        )

        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY
        assert entry.operator == "admin"
        assert entry.details == {"reason": "Manual start"}
        assert entry.id is None  # ID not set until persisted

    def test_migration_started_factory(self) -> None:
        """Test migration_started factory method."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry.migration_started(
            migration_id=migration_id,
            occurred_at=now,
            operator="admin@example.com",
            details={"config": {"batch_size": 1000}},
        )

        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.MIGRATION_STARTED
        assert entry.old_phase is None
        assert entry.new_phase == MigrationPhase.PENDING
        assert entry.operator == "admin@example.com"
        assert entry.details == {"config": {"batch_size": 1000}}

    def test_error_occurred_factory(self) -> None:
        """Test error_occurred factory method."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry.error_occurred(
            migration_id=migration_id,
            occurred_at=now,
            error_message="Connection timeout",
            error_type="NetworkError",
            operator="system",
        )

        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.ERROR_OCCURRED
        assert entry.old_phase is None
        assert entry.new_phase is None
        assert entry.details == {
            "error_message": "Connection timeout",
            "error_type": "NetworkError",
        }

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry(
            id=42,
            migration_id=migration_id,
            event_type=AuditEventType.PHASE_CHANGED,
            old_phase=MigrationPhase.BULK_COPY,
            new_phase=MigrationPhase.DUAL_WRITE,
            details={"key": "value"},
            operator="admin",
            occurred_at=now,
        )

        data = entry.to_dict()

        assert data["id"] == 42
        assert data["migration_id"] == str(migration_id)
        assert data["event_type"] == "phase_changed"
        assert data["old_phase"] == "bulk_copy"
        assert data["new_phase"] == "dual_write"
        assert data["details"] == {"key": "value"}
        assert data["operator"] == "admin"
        assert data["occurred_at"] == now.isoformat()

    def test_to_dict_with_null_phases(self) -> None:
        """Test to_dict with null phases."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry(
            id=1,
            migration_id=migration_id,
            event_type=AuditEventType.ERROR_OCCURRED,
            old_phase=None,
            new_phase=None,
            details=None,
            operator=None,
            occurred_at=now,
        )

        data = entry.to_dict()

        assert data["old_phase"] is None
        assert data["new_phase"] is None
        assert data["details"] is None
        assert data["operator"] is None


class TestAuditEventTypeEnum:
    """Tests for AuditEventType enum."""

    def test_all_event_types_match_db_constraint(self) -> None:
        """Test all enum values match the database CHECK constraint."""
        # These are the values from the migration_audit_log table CHECK constraint
        db_event_types = {
            "migration_started",
            "phase_changed",
            "migration_paused",
            "migration_resumed",
            "migration_aborted",
            "migration_completed",
            "migration_failed",
            "error_occurred",
            "cutover_initiated",
            "cutover_completed",
            "cutover_rolled_back",
            "verification_started",
            "verification_completed",
            "verification_failed",
            "progress_checkpoint",
        }

        enum_values = {et.value for et in AuditEventType}

        assert enum_values == db_event_types

    def test_event_type_values(self) -> None:
        """Test specific event type values."""
        assert AuditEventType.MIGRATION_STARTED.value == "migration_started"
        assert AuditEventType.PHASE_CHANGED.value == "phase_changed"
        assert AuditEventType.MIGRATION_PAUSED.value == "migration_paused"
        assert AuditEventType.MIGRATION_RESUMED.value == "migration_resumed"
        assert AuditEventType.MIGRATION_ABORTED.value == "migration_aborted"
        assert AuditEventType.MIGRATION_COMPLETED.value == "migration_completed"
        assert AuditEventType.MIGRATION_FAILED.value == "migration_failed"
        assert AuditEventType.ERROR_OCCURRED.value == "error_occurred"
        assert AuditEventType.CUTOVER_INITIATED.value == "cutover_initiated"
        assert AuditEventType.CUTOVER_COMPLETED.value == "cutover_completed"
        assert AuditEventType.CUTOVER_ROLLED_BACK.value == "cutover_rolled_back"
        assert AuditEventType.VERIFICATION_STARTED.value == "verification_started"
        assert AuditEventType.VERIFICATION_COMPLETED.value == "verification_completed"
        assert AuditEventType.VERIFICATION_FAILED.value == "verification_failed"
        assert AuditEventType.PROGRESS_CHECKPOINT.value == "progress_checkpoint"
