"""Unit tests for migration operation, consistency, and classification exceptions."""

from uuid import uuid4

import pytest

import eventsource.application.migration.exceptions as mig_exc
from eventsource.application.migration.error_classification import (
    ErrorRecoverability as Rec,
)
from eventsource.application.migration.error_classification import (
    ErrorSeverity as Sev,
)
from eventsource.ports.migration.models import MigrationPhase
from eventsource.ports.positions import Position


class TestConsistencyError:
    def test_creation_basic(self) -> None:
        mid = uuid4()
        error = mig_exc.ConsistencyError("Event counts don't match", migration_id=mid)
        assert error.message == "Event counts don't match" and error.migration_id == mid
        assert error.recoverable is False and error.suggested_action is not None

    def test_creation_with_counts(self) -> None:
        err = mig_exc.ConsistencyError("Mismatch", uuid4(), source_count=1000, target_count=999)
        assert err.source_count == 1000 and err.target_count == 999

    def test_creation_with_stream_id(self) -> None:
        err = mig_exc.ConsistencyError("Stream mismatch", uuid4(), stream_id="user-123")
        assert err.stream_id == "user-123"

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(mig_exc.ConsistencyError("Mismatch", uuid4()), mig_exc.MigrationError)


class TestBulkCopyError:
    def test_creation(self) -> None:
        mid, pos = uuid4(), Position(store_id="source", key=(50000,))
        err = mig_exc.BulkCopyError(migration_id=mid, last_position=pos, error="Connection timeout")
        assert err.migration_id == mid and err.last_position == pos
        assert err.original_error == "Connection timeout"
        assert f"position {pos.to_str()}" in str(err) and "Connection timeout" in str(err)
        assert err.recoverable is True and err.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(mig_exc.BulkCopyError(uuid4(), None, "error"), mig_exc.MigrationError)


class TestDualWriteError:
    def test_creation(self) -> None:
        mid = uuid4()
        err = mig_exc.DualWriteError(migration_id=mid, target_error="Target database unavailable")
        assert err.migration_id == mid and err.target_error == "Target database unavailable"
        assert "Target store write failed" in str(err) and "Target database unavailable" in str(err)
        assert err.recoverable is True and err.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(mig_exc.DualWriteError(uuid4(), "error"), mig_exc.MigrationError)


class TestPositionMappingError:
    def test_creation_basic(self) -> None:
        mid = uuid4()
        err = mig_exc.PositionMappingError("Position not found in mapping", migration_id=mid)
        assert err.message == "Position not found in mapping"
        assert err.migration_id == mid and err.recoverable is False

    def test_creation_with_position(self) -> None:
        pos = Position(store_id="src", key=(12345,))
        err = mig_exc.PositionMappingError(
            "Cannot map position", uuid4(), source_position=pos, reason="Position out of range"
        )
        assert err.source_position == pos and err.reason == "Position out of range"

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(mig_exc.PositionMappingError("error", uuid4()), mig_exc.MigrationError)


class TestCircuitBreakerOpenError:
    def test_classification(self) -> None:
        err = mig_exc.CircuitBreakerOpenError("test_op", time_until_retry=30.0)
        assert (err.severity, err.error_code) == (Sev.WARNING, "CIRCUIT_BREAKER_OPEN")
        assert err.recoverability_type == Rec.TRANSIENT

    def test_attributes(self) -> None:
        err = mig_exc.CircuitBreakerOpenError("my_op", time_until_retry=15.5, migration_id=uuid4())
        assert err.operation_name == "my_op" and err.time_until_retry == 15.5
        assert "my_op" in str(err) and "15.5s" in str(err)


def _assert_cls(err: mig_exc.MigrationError, sev: Sev, rec: Rec, code: str) -> None:
    assert (err.severity, err.recoverability_type, err.error_code) == (sev, rec, code)


class TestExceptionClassification:
    def test_migration_error_classification(self) -> None:
        _assert_cls(mig_exc.MigrationError("Test error"), Sev.ERROR, Rec.FATAL, "MIGRATION_ERROR")

    def test_migration_not_found_classification(self) -> None:
        err = mig_exc.MigrationNotFoundError(uuid4())
        _assert_cls(err, Sev.ERROR, Rec.FATAL, "MIGRATION_NOT_FOUND")

    def test_migration_already_exists_classification(self) -> None:
        err = mig_exc.MigrationAlreadyExistsError(uuid4(), uuid4())
        _assert_cls(err, Sev.WARNING, Rec.RECOVERABLE, "MIGRATION_ALREADY_EXISTS")

    def test_migration_state_error_classification(self) -> None:
        err = mig_exc.MigrationStateError("Invalid state", uuid4(), MigrationPhase.PENDING)
        _assert_cls(err, Sev.ERROR, Rec.FATAL, "MIGRATION_STATE_ERROR")

    def test_invalid_phase_transition_classification(self) -> None:
        err = mig_exc.InvalidPhaseTransitionError(
            uuid4(), MigrationPhase.PENDING, MigrationPhase.CUTOVER
        )
        _assert_cls(err, Sev.ERROR, Rec.FATAL, "INVALID_PHASE_TRANSITION")

    def test_cutover_error_classification(self) -> None:
        err = mig_exc.CutoverError("Cutover failed", migration_id=uuid4())
        _assert_cls(err, Sev.ERROR, Rec.RECOVERABLE, "CUTOVER_ERROR")
        assert err.retry_config is not None

    def test_cutover_timeout_classification(self) -> None:
        err = mig_exc.CutoverTimeoutError(uuid4(), elapsed_ms=150.0, timeout_ms=100.0)
        _assert_cls(err, Sev.ERROR, Rec.TRANSIENT, "CUTOVER_TIMEOUT")
        assert err.retry_config is not None

    def test_cutover_lag_classification(self) -> None:
        err = mig_exc.CutoverLagError(uuid4(), current_lag=500, max_lag=100)
        _assert_cls(err, Sev.WARNING, Rec.TRANSIENT, "CUTOVER_LAG_TOO_HIGH")
        assert err.retry_config is not None

    def test_consistency_error_classification(self) -> None:
        err = mig_exc.ConsistencyError("Data mismatch", migration_id=uuid4())
        _assert_cls(err, Sev.CRITICAL, Rec.RECOVERABLE, "CONSISTENCY_ERROR")

    def test_bulk_copy_error_classification(self) -> None:
        pos = Position(store_id="source", key=(1000,))
        err = mig_exc.BulkCopyError(uuid4(), last_position=pos, error="Connection lost")
        _assert_cls(err, Sev.ERROR, Rec.TRANSIENT, "BULK_COPY_ERROR")
        assert err.retry_config is not None

    def test_dual_write_error_classification(self) -> None:
        err = mig_exc.DualWriteError(uuid4(), target_error="Write failed")
        _assert_cls(err, Sev.WARNING, Rec.TRANSIENT, "DUAL_WRITE_ERROR")
        assert err.retry_config is not None

    def test_position_mapping_error_classification(self) -> None:
        err = mig_exc.PositionMappingError("Position not found", migration_id=uuid4())
        _assert_cls(err, Sev.ERROR, Rec.RECOVERABLE, "POSITION_MAPPING_ERROR")

    def test_error_to_dict(self) -> None:
        data = mig_exc.CutoverTimeoutError(uuid4(), elapsed_ms=150.0, timeout_ms=100.0).to_dict()
        assert "message" in data and "classification" in data
        assert data["classification"]["error_code"] == "CUTOVER_TIMEOUT"
        assert data["classification"]["severity"] == "error"


class TestExceptionHierarchy:
    def test_catch_all_migration_errors(self) -> None:
        uid, p1, p2 = uuid4(), MigrationPhase.PENDING, MigrationPhase.CUTOVER
        for error in [
            mig_exc.MigrationNotFoundError(uid),
            mig_exc.MigrationAlreadyExistsError(uid, uid),
            mig_exc.MigrationStateError("msg", uid, p1),
            mig_exc.InvalidPhaseTransitionError(uid, p1, p2),
            mig_exc.CutoverError("msg", uid),
            mig_exc.CutoverTimeoutError(uid, 150.0, 100.0),
            mig_exc.CutoverLagError(uid, 500, 100),
            mig_exc.ConsistencyError("msg", uid),
            mig_exc.BulkCopyError(uid, None, "error"),
            mig_exc.DualWriteError(uid, "error"),
            mig_exc.PositionMappingError("msg", uid),
        ]:
            with pytest.raises(mig_exc.MigrationError):
                raise error

    def test_catch_cutover_errors_specifically(self) -> None:
        uid = uuid4()
        for error in [
            mig_exc.CutoverError("msg", uid),
            mig_exc.CutoverTimeoutError(uid, 150.0, 100.0),
            mig_exc.CutoverLagError(uid, 500, 100),
        ]:
            with pytest.raises(mig_exc.CutoverError):
                raise error
        non_cutover = mig_exc.BulkCopyError(uid, None, "error")
        with pytest.raises(mig_exc.BulkCopyError):
            raise non_cutover
        assert not isinstance(non_cutover, mig_exc.CutoverError)
