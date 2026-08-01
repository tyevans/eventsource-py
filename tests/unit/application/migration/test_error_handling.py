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
    """Tests for classify_exception function."""

    def test_classify_migration_error(self) -> None:
        """Test classifying a MigrationError."""
        error = BulkCopyError(uuid4(), last_position=None, error="test")
        classification = classify_exception(error)

        assert classification.error_code == "BULK_COPY_ERROR"
        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.TRANSIENT

    def test_classify_generic_exception(self) -> None:
        """Test classifying a non-MigrationError."""
        error = ValueError("Something went wrong")
        classification = classify_exception(error)

        assert classification.error_code == "UNKNOWN_ERROR"
        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.FATAL


class TestErrorHandler:
    """Tests for ErrorHandler with retry logic."""

    @pytest.fixture
    def handler(self) -> ErrorHandler:
        """Create an error handler for testing."""
        return ErrorHandler()

    @pytest.mark.asyncio
    async def test_successful_operation(self, handler: ErrorHandler) -> None:
        """Test successful operation returns result."""

        async def operation() -> str:
            return "success"

        result = await handler.execute_with_retry(operation, "test_op")
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retry_transient_error(self, handler: ErrorHandler) -> None:
        """Test transient error is retried."""
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
        """Test fatal error is not retried."""
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
        """Test error raised after exhausting retries."""

        async def operation() -> str:
            raise DualWriteError(uuid4(), "Always fails")

        config = RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0)
        with pytest.raises(DualWriteError):
            await handler.execute_with_retry(operation, "test_op", retry_config=config)

    @pytest.mark.asyncio
    async def test_on_retry_callback(self, handler: ErrorHandler) -> None:
        """Test on_retry callback is called."""
        retries: list[tuple[int, Exception, float]] = []

        def on_retry(attempt: int, exc: Exception, delay_ms: float) -> None:
            retries.append((attempt, exc, delay_ms))

        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise DualWriteError(uuid4(), "Temporary")
            return "success"

        config = RetryConfig(max_attempts=5, base_delay_ms=10, jitter_factor=0)
        await handler.execute_with_retry(
            operation, "test_op", retry_config=config, on_retry=on_retry
        )

        assert len(retries) == 2
        assert retries[0][0] == 0  # First retry
        assert retries[1][0] == 1  # Second retry

    @pytest.mark.asyncio
    async def test_alert_callback(self) -> None:
        """Test alert callback is invoked for alerting severity."""
        alerts: list[MigrationError] = []

        def alert_callback(error: MigrationError) -> None:
            alerts.append(error)

        handler = ErrorHandler(alert_callback=alert_callback)

        async def operation() -> str:
            raise ConsistencyError("Critical issue", migration_id=uuid4())

        with pytest.raises(ConsistencyError):
            await handler.execute_with_retry(operation, "test_op")

        assert len(alerts) == 1
        assert alerts[0].severity == ErrorSeverity.CRITICAL

    @pytest.mark.asyncio
    async def test_metrics_callback(self) -> None:
        """Test metrics callback is invoked."""
        metrics: list[tuple[MigrationError, bool]] = []

        def metrics_callback(error: MigrationError, retried: bool) -> None:
            metrics.append((error, retried))

        handler = ErrorHandler(metrics_callback=metrics_callback)

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
        assert metrics[0][1] is True  # retried = True for transient

    @pytest.mark.asyncio
    async def test_with_circuit_breaker(self) -> None:
        """Test error handler with circuit breaker."""
        cb_config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1)
        cb = CircuitBreaker(config=cb_config, name="test")
        handler = ErrorHandler(circuit_breaker=cb)

        async def failing_operation() -> str:
            raise MigrationNotFoundError(uuid4())

        # Fail twice to open circuit
        for _ in range(2):
            with pytest.raises(MigrationNotFoundError):
                await handler.execute_with_retry(failing_operation, "test_op")

        # Next call should get circuit breaker error
        with pytest.raises(CircuitBreakerOpenError):
            await handler.execute_with_retry(failing_operation, "test_op")

    @pytest.mark.asyncio
    async def test_with_retry_decorator(self, handler: ErrorHandler) -> None:
        """Test with_retry decorator."""
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
        """Test non-MigrationError exceptions are not retried."""
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Unexpected error")

        with pytest.raises(RuntimeError):
            await handler.execute_with_retry(operation, "test_op")

        assert call_count == 1
