"""Unit tests for migration lifecycle exceptions."""

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
    def test_basic_creation(self) -> None:
        error = MigrationError("Test error message")
        assert str(error) == "Test error message" and error.message == "Test error message"
        assert (error.migration_id, error.tenant_id, error.suggested_action) == (None, None, None)
        assert error.recoverable is False and error.is_recoverable is False

    def test_with_migration_id(self) -> None:
        migration_id = uuid4()
        error = MigrationError("Error", migration_id=migration_id)
        assert error.migration_id == migration_id and f"migration_id={migration_id}" in str(error)

    def test_with_tenant_id(self) -> None:
        tenant_id = uuid4()
        error = MigrationError("Error", tenant_id=tenant_id)
        assert error.tenant_id == tenant_id and f"tenant_id={tenant_id}" in str(error)

    def test_with_recoverable(self) -> None:
        error = MigrationError("Error", recoverable=True)
        assert error.recoverable is True and error.is_recoverable is True
        assert "(recoverable)" in str(error)

    def test_with_suggested_action(self) -> None:
        error = MigrationError("Error", recoverable=True, suggested_action="Retry the operation")
        assert error.suggested_action == "Retry the operation"

    def test_full_str_representation(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        error = MigrationError(
            "Something went wrong",
            migration_id=migration_id,
            tenant_id=tenant_id,
            recoverable=True,
        )
        error_str = str(error)
        for part in (
            "Something went wrong",
            f"migration_id={migration_id}",
            f"tenant_id={tenant_id}",
            "(recoverable)",
        ):
            assert part in error_str


class TestMigrationNotFoundError:
    def test_creation(self) -> None:
        migration_id = uuid4()
        error = MigrationNotFoundError(migration_id)
        assert error.migration_id == migration_id
        assert f"Migration not found: {migration_id}" in str(error) and error.recoverable is False

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(MigrationNotFoundError(uuid4()), MigrationError)


class TestMigrationAlreadyExistsError:
    def test_creation(self) -> None:
        tenant_id, existing_migration_id = uuid4(), uuid4()
        error = MigrationAlreadyExistsError(tenant_id, existing_migration_id)
        assert error.tenant_id == tenant_id
        assert error.existing_migration_id == existing_migration_id
        assert error.migration_id == existing_migration_id
        assert f"tenant {tenant_id}" in str(error) and str(existing_migration_id) in str(error)
        assert error.recoverable is False and error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(MigrationAlreadyExistsError(uuid4(), uuid4()), MigrationError)


class TestMigrationStateError:
    def test_creation(self) -> None:
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
        assert error.operation == "cutover" and error.recoverable is False

    def test_is_subclass_of_migration_error(self) -> None:
        error = MigrationStateError(
            "Invalid state", migration_id=uuid4(), current_phase=MigrationPhase.PENDING
        )
        assert isinstance(error, MigrationError)


class TestInvalidPhaseTransitionError:
    def test_creation(self) -> None:
        migration_id = uuid4()
        error = InvalidPhaseTransitionError(
            migration_id,
            current_phase=MigrationPhase.PENDING,
            target_phase=MigrationPhase.CUTOVER,
        )
        assert error.migration_id == migration_id
        assert error.current_phase == MigrationPhase.PENDING
        assert error.target_phase == MigrationPhase.CUTOVER
        assert "pending -> cutover" in str(error) and error.operation == "phase_transition"

    def test_is_subclass_of_migration_state_error(self) -> None:
        error = InvalidPhaseTransitionError(
            uuid4(), current_phase=MigrationPhase.PENDING, target_phase=MigrationPhase.CUTOVER
        )
        assert isinstance(error, MigrationStateError) and isinstance(error, MigrationError)


class TestCutoverError:
    def test_creation(self) -> None:
        migration_id = uuid4()
        error = CutoverError(
            "Cutover failed",
            migration_id=migration_id,
            rollback_performed=True,
            reason="sync_lag_too_high",
        )
        assert error.message == "Cutover failed" and error.migration_id == migration_id
        assert error.rollback_performed is True and error.reason == "sync_lag_too_high"
        assert error.recoverable is True and error.suggested_action is not None

    def test_is_subclass_of_migration_error(self) -> None:
        assert isinstance(CutoverError("Failed", migration_id=uuid4()), MigrationError)


class TestCutoverTimeoutError:
    def test_creation(self) -> None:
        migration_id = uuid4()
        error = CutoverTimeoutError(migration_id=migration_id, elapsed_ms=150.5, timeout_ms=100.0)
        assert (error.migration_id, error.elapsed_ms, error.timeout_ms) == (
            migration_id,
            150.5,
            100.0,
        )
        assert "150.50ms" in str(error) and "100.00ms" in str(error)
        assert error.rollback_performed is True and error.reason == "timeout"

    def test_is_subclass_of_cutover_error(self) -> None:
        error = CutoverTimeoutError(uuid4(), 150.0, 100.0)
        assert isinstance(error, CutoverError) and isinstance(error, MigrationError)


class TestCutoverLagError:
    def test_creation(self) -> None:
        migration_id = uuid4()
        error = CutoverLagError(migration_id=migration_id, current_lag=500, max_lag=100)
        assert (error.migration_id, error.current_lag, error.max_lag) == (migration_id, 500, 100)
        assert "500 events" in str(error) and "max: 100" in str(error)
        assert error.rollback_performed is False and error.reason == "lag_too_high"

    def test_is_subclass_of_cutover_error(self) -> None:
        error = CutoverLagError(uuid4(), 500, 100)
        assert isinstance(error, CutoverError) and isinstance(error, MigrationError)
