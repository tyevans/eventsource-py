"""Tests for helper methods, entry factory methods, and AuditEventType enum."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.migration.audit_log import PostgreSQLMigrationAuditLogRepository
from eventsource.ports.migration.models import AuditEventType, MigrationAuditEntry, MigrationPhase

ALL_EVENT_TYPES = [
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


class TestPostgreSQLMigrationAuditLogRepositoryHelpers:
    """Tests for PostgreSQLMigrationAuditLogRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLMigrationAuditLogRepository:
        return PostgreSQLMigrationAuditLogRepository(MagicMock(), enable_tracing=False)

    def test_row_to_entry_with_all_fields(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        row = (
            1,
            migration_id,
            "phase_changed",
            "pending",
            "bulk_copy",
            json_dumps({"key": "value"}),
            "admin@example.com",
            now,
        )
        entry = repo._row_to_entry(row)
        assert entry.id == 1 and entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY
        assert entry.details == {"key": "value"} and entry.operator == "admin@example.com"
        assert entry.occurred_at == now

    def test_row_to_entry_with_dict_details(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        row = (
            1,
            uuid4(),
            "error_occurred",
            None,
            None,
            {"error": "test"},
            "system",
            datetime.now(UTC),
        )
        assert repo._row_to_entry(row).details == {"error": "test"}

    def test_row_to_entry_with_null_phases(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        row = (1, uuid4(), "error_occurred", None, None, None, "system", datetime.now(UTC))
        entry = repo._row_to_entry(row)
        assert (entry.old_phase, entry.new_phase, entry.details) == (None, None, None)

    def test_row_to_entry_with_all_event_types(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        for event_type in ALL_EVENT_TYPES:
            row = (1, migration_id, event_type, None, None, None, "system", now)
            assert repo._row_to_entry(row).event_type.value == event_type


class TestMigrationAuditEntryFactoryMethods:
    """Tests for MigrationAuditEntry factory methods."""

    def test_phase_change_factory(self) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        entry = MigrationAuditEntry.phase_change(
            migration_id,
            MigrationPhase.PENDING,
            MigrationPhase.BULK_COPY,
            now,
            operator="admin",
            details={"reason": "Manual start"},
        )
        assert entry.migration_id == migration_id and entry.id is None
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY
        assert entry.operator == "admin" and entry.details == {"reason": "Manual start"}

    def test_migration_started_factory(self) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        entry = MigrationAuditEntry.migration_started(
            migration_id,
            now,
            operator="admin@example.com",
            details={"config": {"batch_size": 1000}},
        )
        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.MIGRATION_STARTED
        assert entry.old_phase is None and entry.new_phase == MigrationPhase.PENDING
        assert entry.operator == "admin@example.com"
        assert entry.details == {"config": {"batch_size": 1000}}

    def test_error_occurred_factory(self) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        entry = MigrationAuditEntry.error_occurred(
            migration_id,
            now,
            error_message="Connection timeout",
            error_type="NetworkError",
            operator="system",
        )
        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.ERROR_OCCURRED
        assert entry.old_phase is None and entry.new_phase is None
        assert entry.details == {
            "error_message": "Connection timeout",
            "error_type": "NetworkError",
        }

    def test_to_dict(self) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        entry = MigrationAuditEntry(
            42,
            migration_id,
            AuditEventType.PHASE_CHANGED,
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
            {"key": "value"},
            "admin",
            now,
        )
        data = entry.to_dict()
        assert data["id"] == 42 and data["migration_id"] == str(migration_id)
        assert data["event_type"] == "phase_changed"
        assert data["old_phase"] == "bulk_copy" and data["new_phase"] == "dual_write"
        assert data["details"] == {"key": "value"} and data["operator"] == "admin"
        assert data["occurred_at"] == now.isoformat()

    def test_to_dict_with_null_phases(self) -> None:
        entry = MigrationAuditEntry(
            1, uuid4(), AuditEventType.ERROR_OCCURRED, None, None, None, None, datetime.now(UTC)
        )
        data = entry.to_dict()
        for k in ("old_phase", "new_phase", "details", "operator"):
            assert data[k] is None


class TestAuditEventTypeEnum:
    """Tests for AuditEventType enum."""

    def test_all_event_types_match_db_constraint(self) -> None:
        assert {et.value for et in AuditEventType} == set(ALL_EVENT_TYPES)

    def test_event_type_values(self) -> None:
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
