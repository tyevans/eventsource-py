"""Tests for Migration dataclass."""

from datetime import datetime, timedelta
from uuid import uuid4

from eventsource.ports.migration.models import Migration, MigrationPhase


class TestMigration:
    """Tests for Migration dataclass."""

    def test_creation_with_required_fields(self) -> None:
        """Test creating a Migration with required fields."""
        migration_id = uuid4()
        tenant_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="shared",
            target_store_id="dedicated",
        )
        assert migration.id == migration_id
        assert migration.tenant_id == tenant_id
        assert migration.source_store_id == "shared"
        assert migration.target_store_id == "dedicated"
        assert migration.phase == MigrationPhase.PENDING
        assert migration.events_total == 0
        assert migration.events_copied == 0

    def test_progress_percent_zero_total(self) -> None:
        """Test progress_percent with zero total events."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            events_total=0,
            events_copied=0,
        )
        assert migration.progress_percent == 0.0

    def test_progress_percent_calculation(self) -> None:
        """Test progress_percent calculation."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            events_total=100,
            events_copied=50,
        )
        assert migration.progress_percent == 50.0

    def test_progress_percent_capped_at_100(self) -> None:
        """Test progress_percent is capped at 100."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            events_total=100,
            events_copied=150,  # More than total
        )
        assert migration.progress_percent == 100.0

    def test_is_active_for_pending(self) -> None:
        """Test is_active returns True for PENDING phase."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.PENDING,
        )
        assert migration.is_active is True
        assert migration.is_terminal is False

    def test_is_terminal_for_completed(self) -> None:
        """Test is_terminal returns True for COMPLETED phase."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.COMPLETED,
        )
        assert migration.is_terminal is True
        assert migration.is_active is False

    def test_events_remaining(self) -> None:
        """Test events_remaining calculation."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            events_total=100,
            events_copied=30,
        )
        assert migration.events_remaining == 70

    def test_events_remaining_never_negative(self) -> None:
        """Test events_remaining is never negative."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            events_total=100,
            events_copied=150,
        )
        assert migration.events_remaining == 0

    def test_duration_not_started(self) -> None:
        """Test duration returns None when not started."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
        )
        assert migration.duration is None

    def test_duration_in_progress(self) -> None:
        """Test duration calculation when in progress."""
        started = datetime.now() - timedelta(hours=1)
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            started_at=started,
        )
        duration = migration.duration
        assert duration is not None
        assert duration.total_seconds() >= 3600  # At least 1 hour

    def test_duration_completed(self) -> None:
        """Test duration calculation when completed."""
        started = datetime(2024, 1, 1, 10, 0, 0)
        completed = datetime(2024, 1, 1, 11, 30, 0)
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            started_at=started,
            completed_at=completed,
        )
        duration = migration.duration
        assert duration is not None
        assert duration.total_seconds() == 5400  # 1.5 hours

    def test_bulk_copy_duration(self) -> None:
        """Test bulk_copy_duration calculation."""
        started = datetime(2024, 1, 1, 10, 0, 0)
        completed = datetime(2024, 1, 1, 10, 30, 0)
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            bulk_copy_started_at=started,
            bulk_copy_completed_at=completed,
        )
        duration = migration.bulk_copy_duration
        assert duration is not None
        assert duration.total_seconds() == 1800  # 30 minutes

    def test_current_phase_started_at(self) -> None:
        """Test current_phase_started_at property."""
        started = datetime(2024, 1, 1, 10, 0, 0)
        bulk_copy_started = datetime(2024, 1, 1, 10, 5, 0)
        dual_write_started = datetime(2024, 1, 1, 10, 30, 0)

        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.DUAL_WRITE,
            started_at=started,
            bulk_copy_started_at=bulk_copy_started,
            dual_write_started_at=dual_write_started,
        )
        assert migration.current_phase_started_at == dual_write_started

    def test_can_transition_to_delegates_to_phase(self) -> None:
        """Test can_transition_to delegates to phase."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.PENDING,
        )
        assert migration.can_transition_to(MigrationPhase.BULK_COPY) is True
        assert migration.can_transition_to(MigrationPhase.DUAL_WRITE) is False
