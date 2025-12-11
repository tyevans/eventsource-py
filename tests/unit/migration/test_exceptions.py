"""
Unit tests for migration exceptions.

Tests cover:
- MigrationError base exception
- All exception subclasses
- Exception attributes and string representations
"""

from uuid import uuid4

import pytest

from eventsource.migration.exceptions import (
    BulkCopyError,
    ConsistencyError,
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
    DualWriteError,
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
    PositionMappingError,
    RoutingError,
)
from eventsource.migration.models import MigrationPhase


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


class TestConsistencyError:
    """Tests for ConsistencyError."""

    def test_creation_basic(self) -> None:
        """Test creating a basic ConsistencyError."""
        migration_id = uuid4()
        error = ConsistencyError(
            "Event counts don't match",
            migration_id=migration_id,
        )

        assert error.message == "Event counts don't match"
        assert error.migration_id == migration_id
        assert error.recoverable is False
        assert error.suggested_action is not None

    def test_creation_with_counts(self) -> None:
        """Test creating ConsistencyError with counts."""
        migration_id = uuid4()
        error = ConsistencyError(
            "Mismatch detected",
            migration_id=migration_id,
            source_count=1000,
            target_count=999,
        )

        assert error.source_count == 1000
        assert error.target_count == 999

    def test_creation_with_stream_id(self) -> None:
        """Test creating ConsistencyError with stream_id."""
        error = ConsistencyError(
            "Stream mismatch",
            migration_id=uuid4(),
            stream_id="user-123",
        )

        assert error.stream_id == "user-123"

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that ConsistencyError is a MigrationError."""
        error = ConsistencyError("Mismatch", migration_id=uuid4())
        assert isinstance(error, MigrationError)


class TestBulkCopyError:
    """Tests for BulkCopyError."""

    def test_creation(self) -> None:
        """Test creating a BulkCopyError."""
        migration_id = uuid4()
        error = BulkCopyError(
            migration_id=migration_id,
            last_position=50000,
            error="Connection timeout",
        )

        assert error.migration_id == migration_id
        assert error.last_position == 50000
        assert error.original_error == "Connection timeout"
        assert "position 50000" in str(error)
        assert "Connection timeout" in str(error)
        assert error.recoverable is True
        assert error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that BulkCopyError is a MigrationError."""
        error = BulkCopyError(uuid4(), 0, "error")
        assert isinstance(error, MigrationError)


class TestDualWriteError:
    """Tests for DualWriteError."""

    def test_creation(self) -> None:
        """Test creating a DualWriteError."""
        migration_id = uuid4()
        error = DualWriteError(
            migration_id=migration_id,
            target_error="Target database unavailable",
        )

        assert error.migration_id == migration_id
        assert error.target_error == "Target database unavailable"
        assert "Target store write failed" in str(error)
        assert "Target database unavailable" in str(error)
        assert error.recoverable is True
        assert error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that DualWriteError is a MigrationError."""
        error = DualWriteError(uuid4(), "error")
        assert isinstance(error, MigrationError)


class TestPositionMappingError:
    """Tests for PositionMappingError."""

    def test_creation_basic(self) -> None:
        """Test creating a basic PositionMappingError."""
        migration_id = uuid4()
        error = PositionMappingError(
            "Position not found in mapping",
            migration_id=migration_id,
        )

        assert error.message == "Position not found in mapping"
        assert error.migration_id == migration_id
        assert error.recoverable is False

    def test_creation_with_position(self) -> None:
        """Test creating PositionMappingError with source position."""
        migration_id = uuid4()
        error = PositionMappingError(
            "Cannot map position",
            migration_id=migration_id,
            source_position=12345,
            reason="Position out of range",
        )

        assert error.source_position == 12345
        assert error.reason == "Position out of range"

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that PositionMappingError is a MigrationError."""
        error = PositionMappingError("error", migration_id=uuid4())
        assert isinstance(error, MigrationError)


class TestRoutingError:
    """Tests for RoutingError."""

    def test_creation_basic(self) -> None:
        """Test creating a basic RoutingError."""
        tenant_id = uuid4()
        error = RoutingError(
            "No routing configured for tenant",
            tenant_id=tenant_id,
        )

        assert error.message == "No routing configured for tenant"
        assert error.tenant_id == tenant_id
        assert error.recoverable is False

    def test_creation_with_reason(self) -> None:
        """Test creating RoutingError with reason."""
        tenant_id = uuid4()
        error = RoutingError(
            "Routing lookup failed",
            tenant_id=tenant_id,
            reason="Database connection error",
        )

        assert error.reason == "Database connection error"

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that RoutingError is a MigrationError."""
        error = RoutingError("error", tenant_id=uuid4())
        assert isinstance(error, MigrationError)

    def test_migration_id_is_none(self) -> None:
        """Test that RoutingError has None migration_id."""
        error = RoutingError("error", tenant_id=uuid4())
        # RoutingError does not set migration_id
        assert error.migration_id is None


class TestExceptionHierarchy:
    """Tests for exception hierarchy and catching."""

    def test_catch_all_migration_errors(self) -> None:
        """Test that all errors can be caught with MigrationError."""
        errors = [
            MigrationNotFoundError(uuid4()),
            MigrationAlreadyExistsError(uuid4(), uuid4()),
            MigrationStateError("msg", uuid4(), MigrationPhase.PENDING),
            InvalidPhaseTransitionError(uuid4(), MigrationPhase.PENDING, MigrationPhase.CUTOVER),
            CutoverError("msg", uuid4()),
            CutoverTimeoutError(uuid4(), 150.0, 100.0),
            CutoverLagError(uuid4(), 500, 100),
            ConsistencyError("msg", uuid4()),
            BulkCopyError(uuid4(), 0, "error"),
            DualWriteError(uuid4(), "error"),
            PositionMappingError("msg", uuid4()),
            RoutingError("msg", uuid4()),
        ]

        for error in errors:
            with pytest.raises(MigrationError):
                raise error

    def test_catch_cutover_errors_specifically(self) -> None:
        """Test catching only CutoverError and subclasses."""
        cutover_errors = [
            CutoverError("msg", uuid4()),
            CutoverTimeoutError(uuid4(), 150.0, 100.0),
            CutoverLagError(uuid4(), 500, 100),
        ]

        for error in cutover_errors:
            with pytest.raises(CutoverError):
                raise error

        # Non-cutover errors should not be caught
        non_cutover = BulkCopyError(uuid4(), 0, "error")
        with pytest.raises(BulkCopyError):
            raise non_cutover
        # Verify it's not a CutoverError
        assert not isinstance(non_cutover, CutoverError)
