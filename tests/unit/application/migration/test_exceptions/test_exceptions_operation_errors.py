"""Unit tests for migration operation, consistency, and classification exceptions."""

from uuid import uuid4

import pytest

from eventsource.application.migration.error_classification import (
    ErrorRecoverability,
    ErrorSeverity,
)
from eventsource.application.migration.exceptions import (
    BulkCopyError,
    CircuitBreakerOpenError,
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
)
from eventsource.ports.migration.models import MigrationPhase
from eventsource.ports.positions import Position


class TestConsistencyError:
    """Tests for ConsistencyError."""

    def test_creation_basic(self) -> None:
        """Test creating a basic ConsistencyError."""
        migration_id = uuid4()
        error = ConsistencyError("Event counts don't match", migration_id=migration_id)
        assert error.message == "Event counts don't match"
        assert error.migration_id == migration_id
        assert error.recoverable is False
        assert error.suggested_action is not None

    def test_creation_with_counts(self) -> None:
        """Test creating ConsistencyError with counts."""
        error = ConsistencyError(
            "Mismatch detected", migration_id=uuid4(), source_count=1000, target_count=999
        )
        assert error.source_count == 1000
        assert error.target_count == 999

    def test_creation_with_stream_id(self) -> None:
        """Test creating ConsistencyError with stream_id."""
        error = ConsistencyError("Stream mismatch", migration_id=uuid4(), stream_id="user-123")
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
        last_position = Position(store_id="source", key=(50000,))
        error = BulkCopyError(
            migration_id=migration_id, last_position=last_position, error="Connection timeout"
        )
        assert error.migration_id == migration_id
        assert error.last_position == last_position
        assert error.original_error == "Connection timeout"
        assert f"position {last_position.to_str()}" in str(error)
        assert "Connection timeout" in str(error)
        assert error.recoverable is True
        assert error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that BulkCopyError is a MigrationError."""
        error = BulkCopyError(uuid4(), None, "error")
        assert isinstance(error, MigrationError)


class TestDualWriteError:
    """Tests for DualWriteError."""

    def test_creation(self) -> None:
        """Test creating a DualWriteError."""
        migration_id = uuid4()
        error = DualWriteError(
            migration_id=migration_id, target_error="Target database unavailable"
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
        error = PositionMappingError("Position not found in mapping", migration_id=migration_id)
        assert error.message == "Position not found in mapping"
        assert error.migration_id == migration_id
        assert error.recoverable is False

    def test_creation_with_position(self) -> None:
        """Test creating PositionMappingError with source position."""
        source_position = Position(store_id="src", key=(12345,))
        error = PositionMappingError(
            "Cannot map position",
            migration_id=uuid4(),
            source_position=source_position,
            reason="Position out of range",
        )
        assert error.source_position == source_position
        assert error.reason == "Position out of range"

    def test_is_subclass_of_migration_error(self) -> None:
        """Test that PositionMappingError is a MigrationError."""
        error = PositionMappingError("error", migration_id=uuid4())
        assert isinstance(error, MigrationError)


class TestCircuitBreakerOpenError:
    """Tests for CircuitBreakerOpenError exception."""

    def test_classification(self) -> None:
        """Test CircuitBreakerOpenError classification."""
        error = CircuitBreakerOpenError("test_op", time_until_retry=30.0)
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "CIRCUIT_BREAKER_OPEN"

    def test_attributes(self) -> None:
        """Test CircuitBreakerOpenError attributes."""
        error = CircuitBreakerOpenError("my_operation", time_until_retry=15.5, migration_id=uuid4())
        assert error.operation_name == "my_operation"
        assert error.time_until_retry == 15.5
        assert "my_operation" in str(error)
        assert "15.5s" in str(error)


class TestExceptionClassification:
    """Tests for exception classification integration."""

    def test_migration_error_classification(self) -> None:
        """Test base MigrationError classification."""
        error = MigrationError("Test error")
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "MIGRATION_ERROR"

    def test_migration_not_found_classification(self) -> None:
        """Test MigrationNotFoundError classification."""
        error = MigrationNotFoundError(uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "MIGRATION_NOT_FOUND"

    def test_migration_already_exists_classification(self) -> None:
        """Test MigrationAlreadyExistsError classification."""
        error = MigrationAlreadyExistsError(uuid4(), uuid4())
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "MIGRATION_ALREADY_EXISTS"

    def test_migration_state_error_classification(self) -> None:
        """Test MigrationStateError classification."""
        error = MigrationStateError("Invalid state", uuid4(), MigrationPhase.PENDING)
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "MIGRATION_STATE_ERROR"

    def test_invalid_phase_transition_classification(self) -> None:
        """Test InvalidPhaseTransitionError classification."""
        error = InvalidPhaseTransitionError(uuid4(), MigrationPhase.PENDING, MigrationPhase.CUTOVER)
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "INVALID_PHASE_TRANSITION"

    def test_cutover_error_classification(self) -> None:
        """Test CutoverError classification."""
        error = CutoverError("Cutover failed", migration_id=uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "CUTOVER_ERROR"
        assert error.retry_config is not None

    def test_cutover_timeout_classification(self) -> None:
        """Test CutoverTimeoutError classification."""
        error = CutoverTimeoutError(uuid4(), elapsed_ms=150.0, timeout_ms=100.0)
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "CUTOVER_TIMEOUT"
        assert error.retry_config is not None

    def test_cutover_lag_classification(self) -> None:
        """Test CutoverLagError classification."""
        error = CutoverLagError(uuid4(), current_lag=500, max_lag=100)
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "CUTOVER_LAG_TOO_HIGH"
        assert error.retry_config is not None

    def test_consistency_error_classification(self) -> None:
        """Test ConsistencyError classification."""
        error = ConsistencyError("Data mismatch", migration_id=uuid4())
        assert error.severity == ErrorSeverity.CRITICAL
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "CONSISTENCY_ERROR"

    def test_bulk_copy_error_classification(self) -> None:
        """Test BulkCopyError classification."""
        error = BulkCopyError(
            uuid4(), last_position=Position(store_id="source", key=(1000,)), error="Connection lost"
        )
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "BULK_COPY_ERROR"
        assert error.retry_config is not None

    def test_dual_write_error_classification(self) -> None:
        """Test DualWriteError classification."""
        error = DualWriteError(uuid4(), target_error="Write failed")
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "DUAL_WRITE_ERROR"
        assert error.retry_config is not None

    def test_position_mapping_error_classification(self) -> None:
        """Test PositionMappingError classification."""
        error = PositionMappingError("Position not found", migration_id=uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "POSITION_MAPPING_ERROR"

    def test_error_to_dict(self) -> None:
        """Test exception to_dict includes classification."""
        error = CutoverTimeoutError(uuid4(), elapsed_ms=150.0, timeout_ms=100.0)
        data = error.to_dict()
        assert "message" in data
        assert "classification" in data
        assert data["classification"]["error_code"] == "CUTOVER_TIMEOUT"
        assert data["classification"]["severity"] == "error"


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
            BulkCopyError(uuid4(), None, "error"),
            DualWriteError(uuid4(), "error"),
            PositionMappingError("msg", uuid4()),
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

        non_cutover = BulkCopyError(uuid4(), None, "error")
        with pytest.raises(BulkCopyError):
            raise non_cutover
        assert not isinstance(non_cutover, CutoverError)
