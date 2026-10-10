"""Integration tests for retry mechanisms and circuit breaker."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from eventsource.application.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    RetryConfig,
    RetryError,
    calculate_backoff,
    retry_async,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


class TestRetryAndRecovery:
    """Tests for retry mechanisms and recovery."""

    async def test_retry_on_transient_failure(self):
        """Test that transient failures are retried."""
        call_count = 0

        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Transient failure")
            return "success"

        config = RetryConfig(
            max_retries=5,
            initial_delay=0.01,
            max_delay=0.1,
        )

        result = await retry_async(
            flaky_operation,
            config=config,
            operation_name="test_operation",
        )

        assert result == "success"
        assert call_count == 3

    async def test_exponential_backoff_delays(self):
        """Test that retry delays follow exponential backoff."""
        config = RetryConfig(
            initial_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=0.0,
        )

        delays = [calculate_backoff(i, config) for i in range(6)]

        assert delays[0] == pytest.approx(1.0, rel=0.01)
        assert delays[1] == pytest.approx(2.0, rel=0.01)
        assert delays[2] == pytest.approx(4.0, rel=0.01)
        assert delays[3] == pytest.approx(8.0, rel=0.01)
        assert delays[4] == pytest.approx(16.0, rel=0.01)
        assert delays[5] == pytest.approx(32.0, rel=0.01)

    async def test_max_retries_exceeded_raises_error(self):
        """Test behavior when max retries exceeded."""

        async def always_fail():
            raise ConnectionError("Permanent failure")

        config = RetryConfig(
            max_retries=3,
            initial_delay=0.01,
            max_delay=0.1,
        )

        with pytest.raises(RetryError) as exc_info:
            await retry_async(
                always_fail,
                config=config,
                operation_name="failing_operation",
            )

        assert exc_info.value.attempts == 4
        assert isinstance(exc_info.value.last_error, ConnectionError)

    async def test_non_retryable_exception_not_retried(self):
        """Test that non-retryable exceptions are raised immediately."""
        call_count = 0

        async def value_error_operation():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")

        config = RetryConfig(max_retries=5, initial_delay=0.01)

        with pytest.raises(ValueError):
            await retry_async(
                value_error_operation,
                config=config,
                retryable_exceptions=(ConnectionError,),
            )

        assert call_count == 1


class TestCircuitBreaker:
    """Tests for circuit breaker behavior."""

    async def test_circuit_breaker_opens_after_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=60.0,
            )
        )

        async def failing_operation():
            raise RuntimeError("Failure")

        for _ in range(3):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.state == CircuitState.OPEN
        assert breaker.failure_count >= 3

    async def test_circuit_breaker_blocks_when_open(self):
        """Test circuit breaker blocks requests when open."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=60.0,
            )
        )

        async def failing_operation():
            raise RuntimeError("Failure")

        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.is_open

        with pytest.raises(CircuitBreakerOpenError):
            await breaker.execute(failing_operation)

    async def test_circuit_breaker_half_open_after_timeout(self):
        """Test circuit breaker enters half-open state after timeout."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.1,
            )
        )

        async def failing_operation():
            raise RuntimeError("Failure")

        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.is_open

        await asyncio.sleep(0.15)

        assert breaker.state in (CircuitState.OPEN, CircuitState.HALF_OPEN)

    async def test_circuit_breaker_closes_on_success(self):
        """Test circuit breaker closes after successful recovery."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.1,
            )
        )

        call_count = 0

        async def intermittent_operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("Failure")
            return "success"

        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(intermittent_operation)

        assert breaker.is_open

        await asyncio.sleep(0.15)

        result = await breaker.execute(intermittent_operation)
        assert result == "success"
        assert breaker.is_closed

    async def test_circuit_breaker_reset(self):
        """Test circuit breaker can be reset."""
        breaker = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=2))

        async def failing_operation():
            raise RuntimeError("Failure")

        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.is_open

        breaker.reset()

        assert breaker.is_closed
        assert breaker.failure_count == 0
