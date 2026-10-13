"""Tests for TenantMigrationState enum."""

from eventsource.ports.migration.models import TenantMigrationState


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
