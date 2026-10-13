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
        """Test error severity classification."""
        # CRITICAL errors
        consistency_error = MigrationError(
            message="Data inconsistency detected",
            recoverable=False,
        )
        assert consistency_error.severity == ErrorSeverity.ERROR

        # Test specific error types have correct severity
        cutover_timeout = CutoverTimeoutError(
            migration_id=uuid4(),
            elapsed_ms=150.0,
            timeout_ms=100.0,
        )
        assert cutover_timeout.severity == ErrorSeverity.ERROR

        cutover_lag = CutoverLagError(
            migration_id=uuid4(),
            current_lag=200,
            max_lag=100,
        )
        assert cutover_lag.severity == ErrorSeverity.WARNING

    def test_error_recoverability_classification(self) -> None:
        """Test error recoverability classification."""
        # TRANSIENT errors should be retried
        dual_write_error = DualWriteError(
            migration_id=uuid4(),
            target_error="Connection timeout",
        )
        assert dual_write_error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert dual_write_error.recoverability_type.should_retry is True

        # FATAL errors should not be retried
        not_found_error = MigrationNotFoundError(uuid4())
        assert not_found_error.recoverability_type == ErrorRecoverability.FATAL
        assert not_found_error.recoverability_type.should_retry is False

    def test_classify_exception_function(self) -> None:
        """Test classify_exception helper function."""
        # Migration errors get their specific classification
        cutover_error = CutoverError(
            message="Cutover failed",
            migration_id=uuid4(),
        )
        classification = classify_exception(cutover_error)
        assert classification.error_code == "CUTOVER_ERROR"
        assert classification.category == "cutover"

        # Non-migration errors get generic classification
        generic_error = ValueError("Some value error")
        classification = classify_exception(generic_error)
        assert classification.error_code == "UNKNOWN_ERROR"
        assert classification.severity == ErrorSeverity.ERROR

    def test_error_to_dict_serialization(self) -> None:
        """Test error serialization to dictionary."""
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
        assert "classification" in error_dict
        assert error_dict["classification"]["category"] == "bulk_copy"

    @pytest.mark.asyncio
    async def test_error_handler_with_retry(self) -> None:
        """Test error handler with automatic retry."""
        handler = ErrorHandler()

        # Track retry attempts
        attempts = []

        async def flaky_operation():
            attempts.append(datetime.now(UTC))
            if len(attempts) < 3:
                raise DualWriteError(
                    migration_id=uuid4(),
                    target_error="Temporary failure",
                )
            return "success"

        # Execute with retry (should succeed on 3rd attempt)
        result = await handler.execute_with_retry(
            flaky_operation,
            operation_name="flaky_op",
            retry_config=RetryConfig(
                max_attempts=5,
                base_delay_ms=10.0,  # Short delay for testing
                max_delay_ms=100.0,
            ),
        )

        assert result == "success"
        assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_error_handler_exhausts_retries(self) -> None:
        """Test error handler exhausts retries and raises."""
        handler = ErrorHandler()

        async def always_fails():
            raise DualWriteError(
                migration_id=uuid4(),
                target_error="Permanent failure",
            )

        with pytest.raises(DualWriteError):
            await handler.execute_with_retry(
                always_fails,
                operation_name="always_fails",
                retry_config=RetryConfig(
                    max_attempts=3,
                    base_delay_ms=10.0,
                    max_delay_ms=50.0,
                ),
            )

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self) -> None:
        """Test circuit breaker opens after threshold failures."""
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                timeout_seconds=0.1,
            ),
            name="test_circuit",
        )

        migration_id = uuid4()

        # Cause failures to open circuit
        for _ in range(3):
            try:
                ctx = await cb.protect("test_op", migration_id)
                async with ctx:
                    raise MigrationError("Test failure", migration_id=migration_id)
            except MigrationError:
                pass

        # Circuit should now be open
        with pytest.raises(CircuitBreakerOpenError):
            await cb.protect("test_op", migration_id)

        # Wait for timeout and verify half-open
        await asyncio.sleep(0.15)

        # Should now be able to try again (half-open)
        ctx = await cb.protect("test_op", migration_id)
        async with ctx:
            pass  # Success

    @pytest.mark.asyncio
    async def test_circuit_breaker_reset(self) -> None:
        """Test circuit breaker can be manually reset."""
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=2),
            name="test_reset",
        )

        migration_id = uuid4()

        # Cause failures
        for _ in range(2):
            try:
                ctx = await cb.protect("test_op", migration_id)
                async with ctx:
                    raise MigrationError("Failure", migration_id=migration_id)
            except MigrationError:
                pass

        # Circuit should be open
        with pytest.raises(CircuitBreakerOpenError):
            await cb.protect("test_op", migration_id)

        # Reset the circuit
        cb.reset()

        # Should be able to use again
        ctx = await cb.protect("test_op", migration_id)
        async with ctx:
            pass  # Success


__all__ = [  # pragma: allowlist secret
    "TestErrorHandlingAndClassification",
]
