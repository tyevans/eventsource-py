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
    @pytest.mark.asyncio
    async def test_success_on_first_try(self):
        operation = AsyncMock(return_value="success")
        result = await retry_async(operation)
        assert result == "success"
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_success_after_retry(self):
        operation = AsyncMock(side_effect=[ConnectionError("First fail"), "success"])
        config = RetryConfig(max_retries=3, initial_delay=0.01)
        result = await retry_async(operation, config=config)
        assert result == "success"
        assert operation.call_count == 2

    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self):
        operation = AsyncMock(side_effect=ConnectionError("Always fails"))
        config = RetryConfig(max_retries=3, initial_delay=0.01)
        with pytest.raises(RetryError) as exc_info:
            await retry_async(operation, config=config)
        assert exc_info.value.attempts == 4
        assert isinstance(exc_info.value.last_error, ConnectionError)

    @pytest.mark.asyncio
    async def test_non_retryable_exception_not_retried(self):
        operation = AsyncMock(side_effect=ValueError("Not retryable"))
        config = RetryConfig(max_retries=3, initial_delay=0.01)
        with pytest.raises(ValueError) as exc_info:
            await retry_async(operation, config=config)
        assert "Not retryable" in str(exc_info.value)
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_retryable_exceptions(self):
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
        operation = AsyncMock(side_effect=ConnectionError("Fails"))
        config = RetryConfig(max_retries=0, initial_delay=0.01)
        with pytest.raises(RetryError) as exc_info:
            await retry_async(operation, config=config)
        assert exc_info.value.attempts == 1
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_delay_between_retries(self):
        operation = AsyncMock(side_effect=[ConnectionError("Fail"), "success"])
        config = RetryConfig(max_retries=1, initial_delay=0.1, jitter=0.0)
        start = time.monotonic()
        await retry_async(operation, config=config)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_default_config_used(self):
        operation = AsyncMock(return_value="success")
        result = await retry_async(operation)
        assert result == "success"


class TestRetryableOperation:
    @pytest.mark.asyncio
    async def test_execute_success(self):
        retry = RetryableOperation()
        operation = AsyncMock(return_value="result")
        result = await retry.execute(operation, name="test_op")
        assert result == "result"

    @pytest.mark.asyncio
    async def test_execute_with_retry(self):
        config = RetryConfig(max_retries=3, initial_delay=0.01)
        retry = RetryableOperation(config=config)
        operation = AsyncMock(side_effect=[ConnectionError("Fail"), "success"])
        result = await retry.execute(operation, name="test_op")
        assert result == "success"
        assert operation.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_with_circuit_breaker(self):
        config = RetryConfig(max_retries=1, initial_delay=0.01)
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        retry = RetryableOperation(config=config, circuit_breaker=breaker)

        operation1 = AsyncMock(side_effect=ConnectionError("Fail"))
        with pytest.raises(RetryError):
            await retry.execute(operation1, name="op1")

        operation2 = AsyncMock(return_value="success")
        with pytest.raises(RetryError):
            await retry.execute(operation2, name="op2")

    @pytest.mark.asyncio
    async def test_stats_property(self):
        retry = RetryableOperation()
        assert isinstance(retry.stats, RetryStats)


class TestRetryIntegration:
    @pytest.mark.asyncio
    async def test_retry_with_exponential_backoff(self):
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
        assert total_time >= 0.15

    @pytest.mark.asyncio
    async def test_circuit_breaker_prevents_retry_storm(self):
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

        with pytest.raises(RetryError):
            await retry.execute(failing_operation, name="op")
        assert breaker.is_open


class TestModuleImports:
    def test_import_from_retry_module(self):
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
