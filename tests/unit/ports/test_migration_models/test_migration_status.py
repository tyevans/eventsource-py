"""Tests for MigrationStatus dataclass."""

from datetime import datetime
from uuid import uuid4

from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
    MigrationStatus,
    SyncLag,
)


class TestMigrationStatus:
    """Tests for MigrationStatus dataclass."""

    def test_creation(self) -> None:
        """Test creating a MigrationStatus."""
        migration_id = uuid4()
        tenant_id = uuid4()
        started_at = datetime.now()

        status = MigrationStatus(
            migration_id=migration_id,
            tenant_id=tenant_id,
            phase=MigrationPhase.BULK_COPY,
            progress_percent=50.0,
            events_total=1000,
            events_copied=500,
            events_remaining=500,
            sync_lag_events=10,
            sync_lag_ms=10.0,
            error_count=0,
            started_at=started_at,
            phase_started_at=started_at,
            estimated_completion=None,
            current_rate_events_per_sec=100.0,
            is_paused=False,
        )
        assert status.migration_id == migration_id
        assert status.tenant_id == tenant_id
        assert status.phase == MigrationPhase.BULK_COPY
        assert status.progress_percent == 50.0

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        migration_id = uuid4()
        tenant_id = uuid4()
        started_at = datetime(2024, 1, 1, 10, 0, 0)

        status = MigrationStatus(
            migration_id=migration_id,
            tenant_id=tenant_id,
            phase=MigrationPhase.DUAL_WRITE,
            progress_percent=75.5,
            events_total=1000,
            events_copied=755,
            events_remaining=245,
            sync_lag_events=20,
            sync_lag_ms=20.0,
            error_count=2,
            started_at=started_at,
            phase_started_at=started_at,
            estimated_completion=None,
            current_rate_events_per_sec=50.0,
            is_paused=False,
        )

        data = status.to_dict()
        assert data["migration_id"] == str(migration_id)
        assert data["tenant_id"] == str(tenant_id)
        assert data["phase"] == "dual_write"
        assert data["progress_percent"] == 75.5
        assert data["events_total"] == 1000
        assert data["started_at"] == "2024-01-01T10:00:00"
        assert data["estimated_completion"] is None

    def test_from_migration(self) -> None:
        """Test from_migration factory method."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            events_total=1000,
            events_copied=500,
            error_count=1,
            started_at=datetime.now(),
            bulk_copy_started_at=datetime.now(),
        )

        sync_lag = SyncLag(
            events=25,
            source_position=500,
            target_position=475,
            timestamp=datetime.now(),
        )

        status = MigrationStatus.from_migration(
            migration,
            sync_lag=sync_lag,
            rate_events_per_sec=100.0,
        )

        assert status.migration_id == migration.id
        assert status.tenant_id == migration.tenant_id
        assert status.phase == MigrationPhase.BULK_COPY
        assert status.progress_percent == 50.0
        assert status.events_remaining == 500
        assert status.sync_lag_events == 25
        assert status.sync_lag_ms == 25.0
        assert status.error_count == 1
        assert status.current_rate_events_per_sec == 100.0

    def test_from_migration_no_sync_lag(self) -> None:
        """Test from_migration with no sync lag."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
        )

        status = MigrationStatus.from_migration(migration)

        assert status.sync_lag_events == 0
        assert status.sync_lag_ms == 0.0
