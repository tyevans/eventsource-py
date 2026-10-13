"""Tests for MigrationResult dataclass."""

from datetime import datetime
from uuid import uuid4

from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
    MigrationResult,
)


class TestMigrationResult:
    """Tests for MigrationResult dataclass."""

    def test_successful_result(self) -> None:
        """Test creating a successful migration result."""
        migration_id = uuid4()
        result = MigrationResult(
            migration_id=migration_id,
            success=True,
            duration_seconds=3600.0,
            events_migrated=100000,
            final_phase=MigrationPhase.COMPLETED,
            consistency_verified=True,
            subscriptions_migrated=5,
        )
        assert result.migration_id == migration_id
        assert result.success is True
        assert result.duration_seconds == 3600.0
        assert result.events_migrated == 100000
        assert result.final_phase == MigrationPhase.COMPLETED
        assert result.error_message is None

    def test_failed_result(self) -> None:
        """Test creating a failed migration result."""
        result = MigrationResult(
            migration_id=uuid4(),
            success=False,
            duration_seconds=1800.0,
            events_migrated=50000,
            final_phase=MigrationPhase.FAILED,
            error_message="Consistency check failed",
        )
        assert result.success is False
        assert result.error_message == "Consistency check failed"

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        migration_id = uuid4()
        result = MigrationResult(
            migration_id=migration_id,
            success=True,
            duration_seconds=3600.0,
            events_migrated=100000,
            final_phase=MigrationPhase.COMPLETED,
        )

        data = result.to_dict()
        assert data["migration_id"] == str(migration_id)
        assert data["success"] is True
        assert data["duration_seconds"] == 3600.0
        assert data["events_migrated"] == 100000
        assert data["final_phase"] == "completed"

    def test_from_migration_completed(self) -> None:
        """Test from_migration for completed migration."""
        started = datetime(2024, 1, 1, 10, 0, 0)
        completed = datetime(2024, 1, 1, 11, 0, 0)
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.COMPLETED,
            events_copied=100000,
            started_at=started,
            completed_at=completed,
        )

        result = MigrationResult.from_migration(migration)

        assert result.migration_id == migration.id
        assert result.success is True
        assert result.duration_seconds == 3600.0
        assert result.events_migrated == 100000
        assert result.final_phase == MigrationPhase.COMPLETED

    def test_from_migration_failed(self) -> None:
        """Test from_migration for failed migration."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.FAILED,
            last_error="Connection lost",
        )

        result = MigrationResult.from_migration(migration)

        assert result.success is False
        assert result.error_message == "Connection lost"
        assert result.final_phase == MigrationPhase.FAILED
