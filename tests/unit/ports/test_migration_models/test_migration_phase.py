"""Tests for MigrationPhase enum."""

from eventsource.ports.migration.models import MigrationPhase


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
