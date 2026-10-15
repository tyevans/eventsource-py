"""Unit tests for migration error handling (ErrorHandler, classify_exception)."""

from uuid import uuid4

import pytest

from eventsource.application.migration.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from eventsource.application.migration.error_classification import (
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.application.migration.error_handling import ErrorHandler, classify_exception
from eventsource.application.migration.exceptions import (
    BulkCopyError,
    CircuitBreakerOpenError,
    ConsistencyError,
    DualWriteError,
    MigrationError,
    MigrationNotFoundError,
)


class TestClassifyException:
    def test_classify_migration_error(self) -> None:
        error = BulkCopyError(uuid4(), last_position=None, error="test")
        classification = classify_exception(error)
        assert classification.error_code == "BULK_COPY_ERROR"
        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.TRANSIENT

    def test_classify_generic_exception(self) -> None:
        error = ValueError("Something went wrong")
        classification = classify_exception(error)
        assert classification.error_code == "UNKNOWN_ERROR"
        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.FATAL


class TestErrorHandler:
    @pytest.fixture
    def handler(self) -> ErrorHandler:
        return ErrorHandler()

    @pytest.mark.asyncio
    async def test_successful_operation(self, handler: ErrorHandler) -> None:
        async def operation() -> str:
            return "success"

        result = await handler.execute_with_retry(operation, "test_op")
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retry_transient_error(self, handler: ErrorHandler) -> None:
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise DualWriteError(uuid4(), "Temporary failure")
            return "success"

        config = RetryConfig(max_attempts=5, base_delay_ms=1, jitter_factor=0)
        result = await handler.execute_with_retry(operation, "test_op", retry_config=config)
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_no_retry_fatal_error(self, handler: ErrorHandler) -> None:
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            raise MigrationNotFoundError(uuid4())

        with pytest.raises(MigrationNotFoundError):
            await handler.execute_with_retry(operation, "test_op")
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_exhausted_retries(self, handler: ErrorHandler) -> None:
        async def operation() -> str:
            raise DualWriteError(uuid4(), "Always fails")

        config = RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0)
        with pytest.raises(DualWriteError):
            await handler.execute_with_retry(operation, "test_op", retry_config=config)

    @pytest.mark.asyncio
    async def test_on_retry_callback(self, handler: ErrorHandler) -> None:
        retries: list[tuple[int, Exception, float]] = []
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise DualWriteError(uuid4(), "Temporary")
            return "success"

        config = RetryConfig(max_attempts=5, base_delay_ms=10, jitter_factor=0)
        await handler.execute_with_retry(
            operation,
            "test_op",
            retry_config=config,
            on_retry=lambda attempt, exc, delay_ms: retries.append((attempt, exc, delay_ms)),
        )
        assert len(retries) == 2
        assert (retries[0][0], retries[1][0]) == (0, 1)

    @pytest.mark.asyncio
    async def test_alert_callback(self) -> None:
        alerts: list[MigrationError] = []
        handler = ErrorHandler(alert_callback=alerts.append)

        async def operation() -> str:
            raise ConsistencyError("Critical issue", migration_id=uuid4())

        with pytest.raises(ConsistencyError):
            await handler.execute_with_retry(operation, "test_op")
        assert len(alerts) == 1
        assert alerts[0].severity == ErrorSeverity.CRITICAL

    @pytest.mark.asyncio
    async def test_metrics_callback(self) -> None:
        metrics: list[tuple[MigrationError, bool]] = []
        handler = ErrorHandler(metrics_callback=lambda err, retried: metrics.append((err, retried)))
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise DualWriteError(uuid4(), "Temporary")
            return "success"

        config = RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0)
        await handler.execute_with_retry(operation, "test_op", retry_config=config)
        assert len(metrics) == 1
        assert metrics[0][1] is True

    @pytest.mark.asyncio
    async def test_with_circuit_breaker(self) -> None:
        cb_config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1)
        handler = ErrorHandler(circuit_breaker=CircuitBreaker(config=cb_config, name="test"))

        async def failing_operation() -> str:
            raise MigrationNotFoundError(uuid4())

        for _ in range(2):
            with pytest.raises(MigrationNotFoundError):
                await handler.execute_with_retry(failing_operation, "test_op")

        with pytest.raises(CircuitBreakerOpenError):
            await handler.execute_with_retry(failing_operation, "test_op")

    @pytest.mark.asyncio
    async def test_with_retry_decorator(self, handler: ErrorHandler) -> None:
        call_count = 0

        @handler.with_retry(
            operation_name="decorated_op",
            retry_config=RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0),
        )
        async def decorated_operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise DualWriteError(uuid4(), "Temporary")
            return "decorated_success"

        result = await decorated_operation()
        assert result == "decorated_success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_non_migration_error_not_retried(self, handler: ErrorHandler) -> None:
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Unexpected error")

        with pytest.raises(RuntimeError):
            await handler.execute_with_retry(operation, "test_op")
        assert call_count == 1
