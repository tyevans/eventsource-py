"""Unit tests for retry execution, retryable operations, and integration."""

import time
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryableOperation,
    RetryConfig,
    RetryError,
    RetryStats,
    retry_async,
)


class TestRetryAsync:
    """Tests for retry_async function."""

    @pytest.mark.asyncio
    async def test_success_on_first_try(self):
        """Test successful operation on first try."""
        operation = AsyncMock(return_value="success")

        result = await retry_async(operation)

        assert result == "success"
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_success_after_retry(self):
        """Test successful operation after retry."""
        operation = AsyncMock(side_effect=[ConnectionError("First fail"), "success"])
        config = RetryConfig(max_retries=3, initial_delay=0.01)

        result = await retry_async(operation, config=config)

        assert result == "success"
        assert operation.call_count == 2

    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self):
        """Test RetryError when max retries exhausted."""
        operation = AsyncMock(side_effect=ConnectionError("Always fails"))
        config = RetryConfig(max_retries=3, initial_delay=0.01)

        with pytest.raises(RetryError) as exc_info:
            await retry_async(operation, config=config)

        assert exc_info.value.attempts == 4  # 1 initial + 3 retries
        assert isinstance(exc_info.value.last_error, ConnectionError)

    @pytest.mark.asyncio
    async def test_non_retryable_exception_not_retried(self):
        """Test non-retryable exceptions are raised immediately."""
        operation = AsyncMock(side_effect=ValueError("Not retryable"))
        config = RetryConfig(max_retries=3, initial_delay=0.01)

        with pytest.raises(ValueError) as exc_info:
            await retry_async(operation, config=config)

        assert "Not retryable" in str(exc_info.value)
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_retryable_exceptions(self):
        """Test custom retryable exceptions."""

        class CustomError(Exception):
            pass

        operation = AsyncMock(side_effect=[CustomError("First"), CustomError("Second"), "success"])
        config = RetryConfig(max_retries=3, initial_delay=0.01)

        result = await retry_async(
            operation,
            config=config,
            retryable_exceptions=(CustomError,),
        )

        assert result == "success"
        assert operation.call_count == 3

    @pytest.mark.asyncio
    async def test_zero_retries(self):
        """Test no retries when max_retries=0."""
        operation = AsyncMock(side_effect=ConnectionError("Fails"))
        config = RetryConfig(max_retries=0, initial_delay=0.01)

        with pytest.raises(RetryError) as exc_info:
            await retry_async(operation, config=config)

        assert exc_info.value.attempts == 1
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_delay_between_retries(self):
        """Test delay is applied between retries."""
        operation = AsyncMock(side_effect=[ConnectionError("Fail"), "success"])
        config = RetryConfig(max_retries=1, initial_delay=0.1, jitter=0.0)

        start = time.monotonic()
        await retry_async(operation, config=config)
        elapsed = time.monotonic() - start

        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_default_config_used(self):
        """Test default config is used when not provided."""
        operation = AsyncMock(return_value="success")

        result = await retry_async(operation)

        assert result == "success"


class TestRetryableOperation:
    """Tests for RetryableOperation class."""

    @pytest.mark.asyncio
    async def test_execute_success(self):
        """Test successful execution."""
        retry = RetryableOperation()
        operation = AsyncMock(return_value="result")

        result = await retry.execute(operation, name="test_op")

        assert result == "result"

    @pytest.mark.asyncio
    async def test_execute_with_retry(self):
        """Test execution with retries."""
        config = RetryConfig(max_retries=3, initial_delay=0.01)
        retry = RetryableOperation(config=config)
        operation = AsyncMock(side_effect=[ConnectionError("Fail"), "success"])

        result = await retry.execute(operation, name="test_op")

        assert result == "success"
        assert operation.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_with_circuit_breaker(self):
        """Test execution with circuit breaker."""
        config = RetryConfig(max_retries=1, initial_delay=0.01)
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        retry = RetryableOperation(config=config, circuit_breaker=breaker)

        # First operation opens circuit
        operation1 = AsyncMock(side_effect=ConnectionError("Fail"))
        with pytest.raises(RetryError):
            await retry.execute(operation1, name="op1")

        # Second operation blocked by circuit breaker
        operation2 = AsyncMock(return_value="success")
        with pytest.raises(RetryError):  # Wrapped in retry
            await retry.execute(operation2, name="op2")

    @pytest.mark.asyncio
    async def test_stats_property(self):
        """Test stats property returns RetryStats."""
        retry = RetryableOperation()
        assert isinstance(retry.stats, RetryStats)


class TestRetryIntegration:
    """Integration tests for retry functionality."""

    @pytest.mark.asyncio
    async def test_retry_with_exponential_backoff(self):
        """Test retry with actual exponential backoff timing."""
        config = RetryConfig(
            max_retries=3,
            initial_delay=0.05,
            exponential_base=2.0,
            jitter=0.0,
        )

        call_times = []
        call_count = 0

        async def operation():
            nonlocal call_count
            call_times.append(time.monotonic())
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Fail")
            return "success"

        start = time.monotonic()
        result = await retry_async(operation, config=config)
        total_time = time.monotonic() - start

        assert result == "success"
        # Should have delays of ~0.05s and ~0.1s
        assert total_time >= 0.15

    @pytest.mark.asyncio
    async def test_circuit_breaker_prevents_retry_storm(self):
        """Test circuit breaker prevents retry storm after repeated failures."""
        breaker_config = CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=60.0,
        )
        retry_config = RetryConfig(max_retries=5, initial_delay=0.01)

        breaker = CircuitBreaker(breaker_config)
        retry = RetryableOperation(config=retry_config, circuit_breaker=breaker)

        call_count = 0

        async def failing_operation():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")

        # First execution - should try until circuit opens
        with pytest.raises(RetryError):
            await retry.execute(failing_operation, name="op")

        # Circuit should be open after 2 failures
        assert breaker.is_open
        # Should have stopped retrying once circuit opened
        # (2 failures to open + retries that hit CircuitBreakerOpenError)


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_retry_module(self):
        """Test imports from retry module."""
        from eventsource.application.subscriptions.retry import (
            TRANSIENT_EXCEPTIONS,
            CircuitBreaker,
            CircuitBreakerConfig,
            CircuitBreakerOpenError,
            CircuitState,
            RetryableOperation,
            RetryConfig,
            RetryError,
            RetryStats,
            calculate_backoff,
            is_retryable_exception,
            retry_async,
        )

        assert RetryConfig is not None
        assert RetryStats is not None
        assert RetryError is not None
        assert CircuitBreaker is not None
        assert CircuitBreakerConfig is not None
        assert CircuitBreakerOpenError is not None
        assert CircuitState is not None
        assert RetryableOperation is not None
        assert calculate_backoff is not None
        assert retry_async is not None
        assert is_retryable_exception is not None
        assert TRANSIENT_EXCEPTIONS is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.application.subscriptions import (
            TRANSIENT_EXCEPTIONS,
            CircuitBreaker,
            CircuitBreakerConfig,
            CircuitBreakerOpenError,
            CircuitState,
            RetryableOperation,
            RetryConfig,
            RetryError,
            RetryStats,
            calculate_backoff,
            is_retryable_exception,
            retry_async,
        )

        assert RetryConfig is not None
        assert RetryStats is not None
        assert RetryError is not None
        assert CircuitBreaker is not None
        assert CircuitBreakerConfig is not None
        assert CircuitBreakerOpenError is not None
        assert CircuitState is not None
        assert RetryableOperation is not None
        assert calculate_backoff is not None
        assert retry_async is not None
        assert is_retryable_exception is not None
        assert TRANSIENT_EXCEPTIONS is not None
