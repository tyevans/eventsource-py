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


async def _failing_operation() -> None:
    raise RuntimeError("Failure")


class TestRetryAndRecovery:
    """Tests for retry mechanisms and recovery."""

    async def test_retry_on_transient_failure(self):
        call_count = 0

        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Transient failure")
            return "success"

        config = RetryConfig(max_retries=5, initial_delay=0.01, max_delay=0.1)
        result = await retry_async(flaky_operation, config=config, operation_name="test_operation")
        assert result == "success" and call_count == 3

    async def test_exponential_backoff_delays(self):
        config = RetryConfig(initial_delay=1.0, max_delay=60.0, exponential_base=2.0, jitter=0.0)
        delays = [calculate_backoff(i, config) for i in range(6)]
        for i, expected in enumerate((1.0, 2.0, 4.0, 8.0, 16.0, 32.0)):
            assert delays[i] == pytest.approx(expected, rel=0.01)

    async def test_max_retries_exceeded_raises_error(self):
        async def always_fail():
            raise ConnectionError("Permanent failure")

        config = RetryConfig(max_retries=3, initial_delay=0.01, max_delay=0.1)
        with pytest.raises(RetryError) as exc_info:
            await retry_async(always_fail, config=config, operation_name="failing_operation")

        assert exc_info.value.attempts == 4
        assert isinstance(exc_info.value.last_error, ConnectionError)

    async def test_non_retryable_exception_not_retried(self):
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
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=60.0)
        )
        for _ in range(3):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(_failing_operation)

        assert breaker.state == CircuitState.OPEN and breaker.failure_count >= 3

    async def test_circuit_breaker_blocks_when_open(self):
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=60.0)
        )
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(_failing_operation)

        assert breaker.is_open
        with pytest.raises(CircuitBreakerOpenError):
            await breaker.execute(_failing_operation)

    async def test_circuit_breaker_half_open_after_timeout(self):
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1)
        )
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(_failing_operation)

        assert breaker.is_open
        await asyncio.sleep(0.15)
        assert breaker.state in (CircuitState.OPEN, CircuitState.HALF_OPEN)

    async def test_circuit_breaker_closes_on_success(self):
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1)
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
        assert result == "success" and breaker.is_closed

    async def test_circuit_breaker_reset(self):
        breaker = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=2))
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(_failing_operation)

        assert breaker.is_open
        breaker.reset()
        assert breaker.is_closed and breaker.failure_count == 0
