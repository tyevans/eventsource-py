"""Error handling and classification tests for migration scenarios."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.migration.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
)
from eventsource.application.migration.error_classification import (
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.application.migration.error_handling import (
    ErrorHandler,
    classify_exception,
)
from eventsource.application.migration.exceptions import (
    BulkCopyError,
    CircuitBreakerOpenError,
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
    DualWriteError,
    MigrationError,
    MigrationNotFoundError,
)
from tests.unit.application.migration.test_final_integration._events import src_pos


class TestErrorHandlingAndClassification:
    """Tests for error handling and classification in real scenarios."""

    def test_error_severity_levels(self) -> None:
        consistency_error = MigrationError(message="Data inconsistency detected", recoverable=False)
        assert consistency_error.severity == ErrorSeverity.ERROR

        cutover_timeout = CutoverTimeoutError(
            migration_id=uuid4(), elapsed_ms=150.0, timeout_ms=100.0
        )
        assert cutover_timeout.severity == ErrorSeverity.ERROR

        cutover_lag = CutoverLagError(migration_id=uuid4(), current_lag=200, max_lag=100)
        assert cutover_lag.severity == ErrorSeverity.WARNING

    def test_error_recoverability_classification(self) -> None:
        dual_write_error = DualWriteError(migration_id=uuid4(), target_error="Connection timeout")
        assert dual_write_error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert dual_write_error.recoverability_type.should_retry is True

        not_found_error = MigrationNotFoundError(uuid4())
        assert not_found_error.recoverability_type == ErrorRecoverability.FATAL
        assert not_found_error.recoverability_type.should_retry is False

    def test_classify_exception_function(self) -> None:
        cutover_error = CutoverError(message="Cutover failed", migration_id=uuid4())
        classification = classify_exception(cutover_error)
        assert classification.error_code == "CUTOVER_ERROR"
        assert classification.category == "cutover"

        generic_error = ValueError("Some value error")
        classification = classify_exception(generic_error)
        assert classification.error_code == "UNKNOWN_ERROR"
        assert classification.severity == ErrorSeverity.ERROR

    def test_error_to_dict_serialization(self) -> None:
        migration_id = uuid4()
        error = BulkCopyError(
            migration_id=migration_id,
            last_position=src_pos(5000),
            error="Connection lost",
        )
        error_dict = error.to_dict()
        assert error_dict["message"] is not None
        assert error_dict["migration_id"] == str(migration_id)
        assert error_dict["error_code"] == "BULK_COPY_ERROR"
        assert error_dict["classification"]["category"] == "bulk_copy"

    @pytest.mark.asyncio
    async def test_error_handler_with_retry(self) -> None:
        handler = ErrorHandler()
        attempts: list[datetime] = []

        async def flaky_operation() -> str:
            attempts.append(datetime.now(UTC))
            if len(attempts) < 3:
                raise DualWriteError(migration_id=uuid4(), target_error="Temporary failure")
            return "success"

        result = await handler.execute_with_retry(
            flaky_operation,
            operation_name="flaky_op",
            retry_config=RetryConfig(max_attempts=5, base_delay_ms=10.0, max_delay_ms=100.0),
        )
        assert result == "success" and len(attempts) == 3

    @pytest.mark.asyncio
    async def test_error_handler_exhausts_retries(self) -> None:
        handler = ErrorHandler()

        async def always_fails() -> None:
            raise DualWriteError(migration_id=uuid4(), target_error="Permanent failure")

        with pytest.raises(DualWriteError):
            await handler.execute_with_retry(
                always_fails,
                operation_name="always_fails",
                retry_config=RetryConfig(max_attempts=3, base_delay_ms=10.0, max_delay_ms=50.0),
            )

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self) -> None:
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=3, timeout_seconds=0.1),
            name="test_circuit",
        )
        migration_id = uuid4()

        for _ in range(3):
            try:
                async with await cb.protect("test_op", migration_id):
                    raise MigrationError("Test failure", migration_id=migration_id)
            except MigrationError:
                pass

        with pytest.raises(CircuitBreakerOpenError):
            await cb.protect("test_op", migration_id)

        await asyncio.sleep(0.15)
        async with await cb.protect("test_op", migration_id):
            pass

    @pytest.mark.asyncio
    async def test_circuit_breaker_reset(self) -> None:
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=2),
            name="test_reset",
        )
        migration_id = uuid4()

        for _ in range(2):
            try:
                async with await cb.protect("test_op", migration_id):
                    raise MigrationError("Failure", migration_id=migration_id)
            except MigrationError:
                pass

        with pytest.raises(CircuitBreakerOpenError):
            await cb.protect("test_op", migration_id)

        cb.reset()
        async with await cb.protect("test_op", migration_id):
            pass


__all__ = [  # pragma: allowlist secret
    "TestErrorHandlingAndClassification",
]
