"""
Unit tests for migration data models.

Tests cover:
- MigrationPhase enum properties and transitions
- TenantMigrationState enum properties and transitions
- MigrationConfig validation and serialization
- Migration dataclass properties and methods
- TenantRouting dataclass properties
- PositionMapping dataclass
- SyncLag dataclass properties
- CutoverResult dataclass properties
- MigrationStatus serialization and factory methods
- MigrationResult serialization and factory methods
- MigrationAuditEntry serialization and factory methods
"""

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from eventsource.migration.models import (
    AuditEventType,
    CutoverResult,
    Migration,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    MigrationResult,
    MigrationStatus,
    PositionMapping,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)


class TestMigrationPhase:
    """Tests for MigrationPhase enum."""

    def test_all_phases_have_values(self) -> None:
        """Test that all phases have string values."""
        assert MigrationPhase.PENDING.value == "pending"
        assert MigrationPhase.BULK_COPY.value == "bulk_copy"
        assert MigrationPhase.DUAL_WRITE.value == "dual_write"
        assert MigrationPhase.CUTOVER.value == "cutover"
        assert MigrationPhase.COMPLETED.value == "completed"
        assert MigrationPhase.ABORTED.value == "aborted"
        assert MigrationPhase.FAILED.value == "failed"

    def test_is_terminal_for_terminal_phases(self) -> None:
        """Test is_terminal property returns True for terminal phases."""
        assert MigrationPhase.COMPLETED.is_terminal is True
        assert MigrationPhase.ABORTED.is_terminal is True
        assert MigrationPhase.FAILED.is_terminal is True

    def test_is_terminal_for_non_terminal_phases(self) -> None:
        """Test is_terminal property returns False for non-terminal phases."""
        assert MigrationPhase.PENDING.is_terminal is False
        assert MigrationPhase.BULK_COPY.is_terminal is False
        assert MigrationPhase.DUAL_WRITE.is_terminal is False
        assert MigrationPhase.CUTOVER.is_terminal is False

    def test_is_active_for_active_phases(self) -> None:
        """Test is_active property returns True for active phases."""
        assert MigrationPhase.BULK_COPY.is_active is True
        assert MigrationPhase.DUAL_WRITE.is_active is True
        assert MigrationPhase.CUTOVER.is_active is True

    def test_is_active_for_inactive_phases(self) -> None:
        """Test is_active property returns False for inactive phases."""
        assert MigrationPhase.PENDING.is_active is False
        assert MigrationPhase.COMPLETED.is_active is False
        assert MigrationPhase.ABORTED.is_active is False
        assert MigrationPhase.FAILED.is_active is False

    def test_allows_writes_to_source(self) -> None:
        """Test allows_writes_to_source property."""
        # CUTOVER blocks writes
        assert MigrationPhase.CUTOVER.allows_writes_to_source is False
        # All other phases allow writes
        assert MigrationPhase.PENDING.allows_writes_to_source is True
        assert MigrationPhase.BULK_COPY.allows_writes_to_source is True
        assert MigrationPhase.DUAL_WRITE.allows_writes_to_source is True
        assert MigrationPhase.COMPLETED.allows_writes_to_source is True

    def test_requires_dual_write(self) -> None:
        """Test requires_dual_write property."""
        assert MigrationPhase.DUAL_WRITE.requires_dual_write is True
        assert MigrationPhase.PENDING.requires_dual_write is False
        assert MigrationPhase.BULK_COPY.requires_dual_write is False
        assert MigrationPhase.CUTOVER.requires_dual_write is False
        assert MigrationPhase.COMPLETED.requires_dual_write is False

    def test_valid_forward_transitions(self) -> None:
        """Test valid forward transitions in state machine."""
        assert MigrationPhase.PENDING.can_transition_to(MigrationPhase.BULK_COPY)
        assert MigrationPhase.BULK_COPY.can_transition_to(MigrationPhase.DUAL_WRITE)
        assert MigrationPhase.DUAL_WRITE.can_transition_to(MigrationPhase.CUTOVER)
        assert MigrationPhase.CUTOVER.can_transition_to(MigrationPhase.COMPLETED)

    def test_cutover_can_rollback_to_dual_write(self) -> None:
        """Test cutover can rollback to dual_write on failure."""
        assert MigrationPhase.CUTOVER.can_transition_to(MigrationPhase.DUAL_WRITE)

    def test_any_phase_can_transition_to_aborted(self) -> None:
        """Test any non-terminal phase can transition to ABORTED."""
        assert MigrationPhase.PENDING.can_transition_to(MigrationPhase.ABORTED)
        assert MigrationPhase.BULK_COPY.can_transition_to(MigrationPhase.ABORTED)
        assert MigrationPhase.DUAL_WRITE.can_transition_to(MigrationPhase.ABORTED)
        assert MigrationPhase.CUTOVER.can_transition_to(MigrationPhase.ABORTED)

    def test_any_phase_can_transition_to_failed(self) -> None:
        """Test any non-terminal phase can transition to FAILED."""
        assert MigrationPhase.PENDING.can_transition_to(MigrationPhase.FAILED)
        assert MigrationPhase.BULK_COPY.can_transition_to(MigrationPhase.FAILED)
        assert MigrationPhase.DUAL_WRITE.can_transition_to(MigrationPhase.FAILED)
        assert MigrationPhase.CUTOVER.can_transition_to(MigrationPhase.FAILED)

    def test_terminal_phases_cannot_transition(self) -> None:
        """Test terminal phases cannot transition to any other phase."""
        for target in MigrationPhase:
            assert MigrationPhase.COMPLETED.can_transition_to(target) is False
            assert MigrationPhase.ABORTED.can_transition_to(target) is False
            assert MigrationPhase.FAILED.can_transition_to(target) is False

    def test_invalid_forward_transitions(self) -> None:
        """Test invalid forward transitions are rejected."""
        # Cannot skip phases
        assert MigrationPhase.PENDING.can_transition_to(MigrationPhase.DUAL_WRITE) is False
        assert MigrationPhase.PENDING.can_transition_to(MigrationPhase.CUTOVER) is False
        assert MigrationPhase.BULK_COPY.can_transition_to(MigrationPhase.CUTOVER) is False
        # Cannot go backwards (except CUTOVER -> DUAL_WRITE rollback)
        assert MigrationPhase.DUAL_WRITE.can_transition_to(MigrationPhase.BULK_COPY) is False
        assert MigrationPhase.BULK_COPY.can_transition_to(MigrationPhase.PENDING) is False


class TestTenantMigrationState:
    """Tests for TenantMigrationState enum."""

    def test_all_states_have_values(self) -> None:
        """Test that all states have string values."""
        assert TenantMigrationState.NORMAL.value == "normal"
        assert TenantMigrationState.BULK_COPY.value == "bulk_copy"
        assert TenantMigrationState.DUAL_WRITE.value == "dual_write"
        assert TenantMigrationState.CUTOVER_PAUSED.value == "cutover_paused"
        assert TenantMigrationState.MIGRATED.value == "migrated"

    def test_is_migrating_for_migrating_states(self) -> None:
        """Test is_migrating property returns True for migrating states."""
        assert TenantMigrationState.BULK_COPY.is_migrating is True
        assert TenantMigrationState.DUAL_WRITE.is_migrating is True
        assert TenantMigrationState.CUTOVER_PAUSED.is_migrating is True

    def test_is_migrating_for_non_migrating_states(self) -> None:
        """Test is_migrating property returns False for non-migrating states."""
        assert TenantMigrationState.NORMAL.is_migrating is False
        assert TenantMigrationState.MIGRATED.is_migrating is False

    def test_allows_writes(self) -> None:
        """Test allows_writes property."""
        # CUTOVER_PAUSED blocks writes
        assert TenantMigrationState.CUTOVER_PAUSED.allows_writes is False
        # All other states allow writes
        assert TenantMigrationState.NORMAL.allows_writes is True
        assert TenantMigrationState.BULK_COPY.allows_writes is True
        assert TenantMigrationState.DUAL_WRITE.allows_writes is True
        assert TenantMigrationState.MIGRATED.allows_writes is True

    def test_reads_from_target(self) -> None:
        """Test reads_from_target property."""
        assert TenantMigrationState.MIGRATED.reads_from_target is True
        assert TenantMigrationState.NORMAL.reads_from_target is False
        assert TenantMigrationState.BULK_COPY.reads_from_target is False
        assert TenantMigrationState.DUAL_WRITE.reads_from_target is False
        assert TenantMigrationState.CUTOVER_PAUSED.reads_from_target is False

    def test_valid_forward_transitions(self) -> None:
        """Test valid forward transitions."""
        assert TenantMigrationState.NORMAL.can_transition_to(TenantMigrationState.BULK_COPY)
        assert TenantMigrationState.BULK_COPY.can_transition_to(TenantMigrationState.DUAL_WRITE)
        assert TenantMigrationState.DUAL_WRITE.can_transition_to(
            TenantMigrationState.CUTOVER_PAUSED
        )
        assert TenantMigrationState.CUTOVER_PAUSED.can_transition_to(TenantMigrationState.MIGRATED)

    def test_cutover_paused_can_rollback(self) -> None:
        """Test CUTOVER_PAUSED can rollback to DUAL_WRITE."""
        assert TenantMigrationState.CUTOVER_PAUSED.can_transition_to(
            TenantMigrationState.DUAL_WRITE
        )

    def test_any_state_can_transition_to_normal(self) -> None:
        """Test any state can transition to NORMAL (cleanup)."""
        assert TenantMigrationState.BULK_COPY.can_transition_to(TenantMigrationState.NORMAL)
        assert TenantMigrationState.DUAL_WRITE.can_transition_to(TenantMigrationState.NORMAL)
        assert TenantMigrationState.CUTOVER_PAUSED.can_transition_to(TenantMigrationState.NORMAL)
        assert TenantMigrationState.MIGRATED.can_transition_to(TenantMigrationState.NORMAL)

    def test_migrated_is_terminal(self) -> None:
        """Test MIGRATED cannot transition to other states except NORMAL."""
        # Only NORMAL is allowed (via base cleanup path)
        assert TenantMigrationState.MIGRATED.can_transition_to(TenantMigrationState.NORMAL)
        # All others should fail
        assert (
            TenantMigrationState.MIGRATED.can_transition_to(TenantMigrationState.BULK_COPY) is False
        )
        assert (
            TenantMigrationState.MIGRATED.can_transition_to(TenantMigrationState.DUAL_WRITE)
            is False
        )


class TestMigrationConfig:
    """Tests for MigrationConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = MigrationConfig()
        assert config.batch_size == 1000
        assert config.max_bulk_copy_rate == 10000
        assert config.dual_write_timeout_minutes == 30
        assert config.cutover_max_lag_events == 100
        assert config.cutover_timeout_ms == 500
        assert config.position_mapping_enabled is True
        assert config.verify_consistency is True
        assert config.migrate_subscriptions is True

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = MigrationConfig(
            batch_size=500,
            max_bulk_copy_rate=5000,
            dual_write_timeout_minutes=60,
            cutover_max_lag_events=50,
            cutover_timeout_ms=200,
            position_mapping_enabled=False,
            verify_consistency=False,
            migrate_subscriptions=False,
        )
        assert config.batch_size == 500
        assert config.max_bulk_copy_rate == 5000
        assert config.dual_write_timeout_minutes == 60
        assert config.cutover_max_lag_events == 50
        assert config.cutover_timeout_ms == 200
        assert config.position_mapping_enabled is False
        assert config.verify_consistency is False
        assert config.migrate_subscriptions is False

    def test_is_frozen(self) -> None:
        """Test that MigrationConfig is frozen (immutable)."""
        config = MigrationConfig()
        with pytest.raises(AttributeError):
            config.batch_size = 500  # type: ignore[misc]

    def test_validation_batch_size(self) -> None:
        """Test batch_size validation."""
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            MigrationConfig(batch_size=0)
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            MigrationConfig(batch_size=-1)

    def test_validation_max_bulk_copy_rate(self) -> None:
        """Test max_bulk_copy_rate validation."""
        with pytest.raises(ValueError, match="max_bulk_copy_rate must be >= 1"):
            MigrationConfig(max_bulk_copy_rate=0)

    def test_validation_dual_write_timeout_minutes(self) -> None:
        """Test dual_write_timeout_minutes validation."""
        with pytest.raises(ValueError, match="dual_write_timeout_minutes must be >= 1"):
            MigrationConfig(dual_write_timeout_minutes=0)

    def test_validation_cutover_max_lag_events(self) -> None:
        """Test cutover_max_lag_events validation."""
        with pytest.raises(ValueError, match="cutover_max_lag_events must be >= 0"):
            MigrationConfig(cutover_max_lag_events=-1)
        # Zero is valid
        config = MigrationConfig(cutover_max_lag_events=0)
        assert config.cutover_max_lag_events == 0

    def test_validation_cutover_timeout_ms(self) -> None:
        """Test cutover_timeout_ms validation."""
        with pytest.raises(ValueError, match="cutover_timeout_ms must be >= 100"):
            MigrationConfig(cutover_timeout_ms=99)
        # Exactly 100 is valid
        config = MigrationConfig(cutover_timeout_ms=100)
        assert config.cutover_timeout_ms == 100

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        config = MigrationConfig(batch_size=500, verify_consistency=False)
        data = config.to_dict()
        assert data["batch_size"] == 500
        assert data["max_bulk_copy_rate"] == 10000
        assert data["verify_consistency"] is False
        assert "position_mapping_enabled" in data

    def test_from_dict(self) -> None:
        """Test from_dict deserialization."""
        data = {
            "batch_size": 500,
            "cutover_max_lag_events": 50,
            "verify_consistency": False,
        }
        config = MigrationConfig.from_dict(data)
        assert config.batch_size == 500
        assert config.cutover_max_lag_events == 50
        assert config.verify_consistency is False
        # Defaults are applied for missing keys
        assert config.max_bulk_copy_rate == 10000

    def test_from_dict_empty(self) -> None:
        """Test from_dict with empty dictionary uses defaults."""
        config = MigrationConfig.from_dict({})
        assert config.batch_size == 1000
        assert config.max_bulk_copy_rate == 10000

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """Test to_dict/from_dict roundtrip."""
        original = MigrationConfig(
            batch_size=500,
            max_bulk_copy_rate=8000,
            dual_write_timeout_minutes=45,
        )
        data = original.to_dict()
        restored = MigrationConfig.from_dict(data)
        assert original == restored


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


class TestTenantRouting:
    """Tests for TenantRouting dataclass."""

    def test_creation(self) -> None:
        """Test creating a TenantRouting."""
        tenant_id = uuid4()
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="shared",
        )
        assert routing.tenant_id == tenant_id
        assert routing.store_id == "shared"
        assert routing.migration_state == TenantMigrationState.NORMAL
        assert routing.active_migration_id is None

    def test_is_migrating(self) -> None:
        """Test is_migrating property."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        assert routing.is_migrating is True

        routing.migration_state = TenantMigrationState.NORMAL
        assert routing.is_migrating is False

    def test_effective_store_id_normal(self) -> None:
        """Test effective_store_id in NORMAL state."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.NORMAL,
        )
        assert routing.effective_store_id == "shared"

    def test_effective_store_id_migrated(self) -> None:
        """Test effective_store_id after migration."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            target_store_id="dedicated",
            migration_state=TenantMigrationState.MIGRATED,
        )
        assert routing.effective_store_id == "dedicated"

    def test_effective_store_id_migrated_no_target(self) -> None:
        """Test effective_store_id falls back to store_id if no target."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.MIGRATED,
        )
        assert routing.effective_store_id == "shared"

    def test_can_transition_to(self) -> None:
        """Test can_transition_to delegates to migration_state."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.NORMAL,
        )
        assert routing.can_transition_to(TenantMigrationState.BULK_COPY) is True
        assert routing.can_transition_to(TenantMigrationState.MIGRATED) is False


class TestPositionMapping:
    """Tests for PositionMapping dataclass."""

    def test_creation(self) -> None:
        """Test creating a PositionMapping."""
        migration_id = uuid4()
        event_id = uuid4()
        mapped_at = datetime.now()
        mapping = PositionMapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=event_id,
            mapped_at=mapped_at,
        )
        assert mapping.migration_id == migration_id
        assert mapping.source_position == 100
        assert mapping.target_position == 50
        assert mapping.event_id == event_id
        assert mapping.mapped_at == mapped_at

    def test_is_frozen(self) -> None:
        """Test that PositionMapping is frozen (immutable)."""
        mapping = PositionMapping(
            migration_id=uuid4(),
            source_position=100,
            target_position=50,
            event_id=uuid4(),
            mapped_at=datetime.now(),
        )
        with pytest.raises(AttributeError):
            mapping.source_position = 200  # type: ignore[misc]


class TestSyncLag:
    """Tests for SyncLag dataclass."""

    def test_creation(self) -> None:
        """Test creating a SyncLag."""
        timestamp = datetime.now()
        lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=timestamp,
        )
        assert lag.events == 50
        assert lag.source_position == 1000
        assert lag.target_position == 950
        assert lag.timestamp == timestamp

    def test_is_converged_true(self) -> None:
        """Test is_converged returns True when lag is zero."""
        lag = SyncLag(
            events=0,
            source_position=1000,
            target_position=1000,
            timestamp=datetime.now(),
        )
        assert lag.is_converged is True

    def test_is_converged_false(self) -> None:
        """Test is_converged returns False when lag is non-zero."""
        lag = SyncLag(
            events=10,
            source_position=1000,
            target_position=990,
            timestamp=datetime.now(),
        )
        assert lag.is_converged is False

    def test_lag_ms(self) -> None:
        """Test lag_ms property."""
        lag = SyncLag(
            events=100,
            source_position=1000,
            target_position=900,
            timestamp=datetime.now(),
        )
        assert lag.lag_ms == 100.0

    def test_is_within_threshold_true(self) -> None:
        """Test is_within_threshold returns True when lag is below threshold."""
        lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=datetime.now(),
        )
        assert lag.is_within_threshold(100) is True
        assert lag.is_within_threshold(50) is True

    def test_is_within_threshold_false(self) -> None:
        """Test is_within_threshold returns False when lag exceeds threshold."""
        lag = SyncLag(
            events=100,
            source_position=1000,
            target_position=900,
            timestamp=datetime.now(),
        )
        assert lag.is_within_threshold(50) is False
        assert lag.is_within_threshold(99) is False

    def test_is_frozen(self) -> None:
        """Test that SyncLag is frozen (immutable)."""
        lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=datetime.now(),
        )
        with pytest.raises(AttributeError):
            lag.events = 100  # type: ignore[misc]


class TestCutoverResult:
    """Tests for CutoverResult dataclass."""

    def test_successful_cutover(self) -> None:
        """Test creating a successful cutover result."""
        result = CutoverResult(
            success=True,
            duration_ms=50.5,
            events_synced=10,
        )
        assert result.success is True
        assert result.duration_ms == 50.5
        assert result.events_synced == 10
        assert result.error_message is None
        assert result.rolled_back is False

    def test_failed_cutover(self) -> None:
        """Test creating a failed cutover result."""
        result = CutoverResult(
            success=False,
            duration_ms=150.0,
            error_message="Timeout exceeded",
            rolled_back=True,
        )
        assert result.success is False
        assert result.error_message == "Timeout exceeded"
        assert result.rolled_back is True

    def test_within_timeout_true(self) -> None:
        """Test within_timeout returns True for fast cutover."""
        result = CutoverResult(
            success=True,
            duration_ms=50.0,
        )
        assert result.within_timeout is True

    def test_within_timeout_false(self) -> None:
        """Test within_timeout returns False for slow cutover."""
        result = CutoverResult(
            success=False,
            duration_ms=150.0,
        )
        assert result.within_timeout is False

    def test_within_timeout_boundary(self) -> None:
        """Test within_timeout at exactly 100ms boundary."""
        # Exactly 100ms should be False (< 100, not <=)
        result = CutoverResult(success=True, duration_ms=100.0)
        assert result.within_timeout is False

        result = CutoverResult(success=True, duration_ms=99.9)
        assert result.within_timeout is True


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
