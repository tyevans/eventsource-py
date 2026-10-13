"""
Unit tests for migration lifecycle exceptions.

Tests cover:
- MigrationError base exception
- MigrationNotFoundError
- MigrationAlreadyExistsError
- MigrationStateError
- InvalidPhaseTransitionError
- CutoverError
- CutoverTimeoutError
- CutoverLagError
"""

from uuid import uuid4

from eventsource.application.migration.exceptions import (
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.ports.migration.models import MigrationPhase


class TestMigrationError:
    """Tests for MigrationError base exception."""

    def test_basic_creation(self) -> None:
        """Test creating a basic MigrationError."""
        error = MigrationError("Test error message")
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.migration_id is None
        assert error.tenant_id is None
        assert error.recoverable is False
        assert error.is_recoverable is False
        assert error.suggested_action is None

    def test_with_migration_id(self) -> None:
        """Test creating error with migration_id."""
        migration_id = uuid4()
        error = MigrationError("Error", migration_id=migration_id)
        assert error.migration_id == migration_id
        assert f"migration_id={migration_id}" in str(error)

    def test_with_tenant_id(self) -> None:
        """Test creating error with tenant_id."""
        tenant_id = uuid4()
        error = MigrationError("Error", tenant_id=tenant_id)
        assert error.tenant_id == tenant_id
        assert f"tenant_id={tenant_id}" in str(error)

    def test_with_recoverable(self) -> None:
        """Test creating recoverable error."""
        error = MigrationError("Error", recoverable=True)
        assert error.recoverable is True
        assert error.is_recoverable is True
        assert "(recoverable)" in str(error)

    def test_with_suggested_action(self) -> None:
        """Test creating error with suggested action."""
        error = MigrationError(
            "Error",
            recoverable=True,
            suggested_action="Retry the operation",
        )
        assert error.suggested_action == "Retry the operation"

    def test_full_str_representation(self) -> None:
        """Test full string representation with all attributes."""
        migration_id = uuid4()
        tenant_id = uuid4()
        error = MigrationError(
            "Something went wrong",
            migration_id=migration_id,
            tenant_id=tenant_id,
            recoverable=True,
        )
        error_str = str(error)
        assert "Something went wrong" in error_str
        assert f"migration_id={migration_id}" in error_str
        assert f"tenant_id={tenant_id}" in error_str
        assert "(recoverable)" in error_str


class TestMigrationNotFoundError:
    """Tests for MigrationNotFoundError."""

    def test_creation(self) -> None:
        """Test creating a MigrationNotFoundError."""
        migration_id = uuid4()
        error = MigrationNotFoundError(migration_id)

        assert error.migration_id == migration_id
        assert f"Migration not found: {migration_id}" in str(error)
        assert error.recoverable is False

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that MigrationNotFoundError is a MigrationError."""
        error = MigrationNotFoundError(uuid4())
        assert isinstance(error, MigrationError)


class TestMigrationAlreadyExistsError:
    """Tests for MigrationAlreadyExistsError."""

    def test_creation(self) -> None:
        """Test creating a MigrationAlreadyExistsError."""
        tenant_id = uuid4()
        existing_migration_id = uuid4()
        error = MigrationAlreadyExistsError(tenant_id, existing_migration_id)

        assert error.tenant_id == tenant_id
        assert error.existing_migration_id == existing_migration_id
        assert error.migration_id == existing_migration_id
        assert f"tenant {tenant_id}" in str(error)
        assert str(existing_migration_id) in str(error)
        assert error.recoverable is False
        assert error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that MigrationAlreadyExistsError is a MigrationError."""
        error = MigrationAlreadyExistsError(uuid4(), uuid4())
        assert isinstance(error, MigrationError)


class TestMigrationStateError:
    """Tests for MigrationStateError."""

    def test_creation(self) -> None:
        """Test creating a MigrationStateError."""
        migration_id = uuid4()
        error = MigrationStateError(
            "Cannot cutover from pending",
            migration_id=migration_id,
            current_phase=MigrationPhase.PENDING,
            expected_phases=[MigrationPhase.DUAL_WRITE],
            operation="cutover",
        )

        assert error.message == "Cannot cutover from pending"
        assert error.migration_id == migration_id
        assert error.current_phase == MigrationPhase.PENDING
        assert error.expected_phases == [MigrationPhase.DUAL_WRITE]
        assert error.operation == "cutover"
        assert error.recoverable is False

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that MigrationStateError is a MigrationError."""
        error = MigrationStateError(
            "Invalid state",
            migration_id=uuid4(),
            current_phase=MigrationPhase.PENDING,
        )
        assert isinstance(error, MigrationError)


class TestInvalidPhaseTransitionError:
    """Tests for InvalidPhaseTransitionError."""

    def test_creation(self) -> None:
        """Test creating an InvalidPhaseTransitionError."""
        migration_id = uuid4()
        error = InvalidPhaseTransitionError(
            migration_id,
            current_phase=MigrationPhase.PENDING,
            target_phase=MigrationPhase.CUTOVER,
        )

        assert error.migration_id == migration_id
        assert error.current_phase == MigrationPhase.PENDING
        assert error.target_phase == MigrationPhase.CUTOVER
        assert "pending -> cutover" in str(error)
        assert error.operation == "phase_transition"

    def test_is_subclass_of_migration_state_error(self) -> None:
        """Test that InvalidPhaseTransitionError is a MigrationStateError."""
        error = InvalidPhaseTransitionError(
            uuid4(),
            current_phase=MigrationPhase.PENDING,
            target_phase=MigrationPhase.CUTOVER,
        )
        assert isinstance(error, MigrationStateError)
        assert isinstance(error, MigrationError)


class TestCutoverError:
    """Tests for CutoverError."""

    def test_creation(self) -> None:
        """Test creating a CutoverError."""
        migration_id = uuid4()
        error = CutoverError(
            "Cutover failed",
            migration_id=migration_id,
            rollback_performed=True,
            reason="sync_lag_too_high",
        )

        assert error.message == "Cutover failed"
        assert error.migration_id == migration_id
        assert error.rollback_performed is True
        assert error.reason == "sync_lag_too_high"
        assert error.recoverable is True  # CutoverErrors are recoverable
        assert error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that CutoverError is a MigrationError."""
        error = CutoverError("Failed", migration_id=uuid4())
        assert isinstance(error, MigrationError)


class TestCutoverTimeoutError:
    """Tests for CutoverTimeoutError."""

    def test_creation(self) -> None:
        """Test creating a CutoverTimeoutError."""
        migration_id = uuid4()
        error = CutoverTimeoutError(
            migration_id=migration_id,
            elapsed_ms=150.5,
            timeout_ms=100.0,
        )

        assert error.migration_id == migration_id
        assert error.elapsed_ms == 150.5
        assert error.timeout_ms == 100.0
        assert "150.50ms" in str(error)
        assert "100.00ms" in str(error)
        assert error.rollback_performed is True
        assert error.reason == "timeout"

    def test_is_subclass_of_cutover_error(self) -> None:
        """Test that CutoverTimeoutError is a CutoverError."""
        error = CutoverTimeoutError(uuid4(), 150.0, 100.0)
        assert isinstance(error, CutoverError)
        assert isinstance(error, MigrationError)


class TestCutoverLagError:
    """Tests for CutoverLagError."""

    def test_creation(self) -> None:
        """Test creating a CutoverLagError."""
        migration_id = uuid4()
        error = CutoverLagError(
            migration_id=migration_id,
            current_lag=500,
            max_lag=100,
        )

        assert error.migration_id == migration_id
        assert error.current_lag == 500
        assert error.max_lag == 100
        assert "500 events" in str(error)
        assert "max: 100" in str(error)
        assert error.rollback_performed is False
        assert error.reason == "lag_too_high"

    def test_is_subclass_of_cutover_error(self) -> None:
        """Test that CutoverLagError is a CutoverError."""
        error = CutoverLagError(uuid4(), 500, 100)
        assert isinstance(error, CutoverError)
        assert isinstance(error, MigrationError)
