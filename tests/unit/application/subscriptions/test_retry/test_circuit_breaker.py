"""Unit tests for circuit breaker pattern and state management."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
)


class TestCircuitBreakerConfigCreation:
    def test_default_values(self):
        config = CircuitBreakerConfig()
        assert config.failure_threshold == 5
        assert config.recovery_timeout == 30.0
        assert config.half_open_max_calls == 1

    def test_custom_values(self):
        config = CircuitBreakerConfig(
            failure_threshold=10,
            recovery_timeout=60.0,
            half_open_max_calls=3,
        )
        assert config.failure_threshold == 10
        assert config.recovery_timeout == 60.0
        assert config.half_open_max_calls == 3

    def test_zero_failure_threshold_raises(self):
        with pytest.raises(ValueError) as exc_info:
            CircuitBreakerConfig(failure_threshold=0)
        assert "failure_threshold must be >= 1" in str(exc_info.value)

    def test_zero_recovery_timeout_raises(self):
        with pytest.raises(ValueError) as exc_info:
            CircuitBreakerConfig(recovery_timeout=0)
        assert "recovery_timeout must be positive" in str(exc_info.value)

    def test_zero_half_open_max_calls_raises(self):
        with pytest.raises(ValueError) as exc_info:
            CircuitBreakerConfig(half_open_max_calls=0)
        assert "half_open_max_calls must be >= 1" in str(exc_info.value)


class TestCircuitBreakerCreation:
    def test_default_creation(self):
        breaker = CircuitBreaker()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_custom_config(self):
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(config)
        assert breaker.config.failure_threshold == 3


class TestCircuitBreakerState:
    @pytest.mark.asyncio
    async def test_starts_closed(self):
        breaker = CircuitBreaker()
        assert breaker.is_closed
        assert not breaker.is_open
        assert not breaker.is_half_open

    @pytest.mark.asyncio
    async def test_opens_after_failure_threshold(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        for _ in range(3):
            await breaker.record_failure()
        assert breaker.is_open
        assert breaker.failure_count == 3

    @pytest.mark.asyncio
    async def test_stays_closed_below_threshold(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        await breaker.record_failure()
        await breaker.record_failure()
        assert breaker.is_closed
        assert breaker.failure_count == 2

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        await breaker.record_failure()
        await breaker.record_failure()
        await breaker.record_success()
        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_transitions_to_half_open(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.05))
        await breaker.record_failure()
        assert breaker.is_open
        await asyncio.sleep(0.1)
        can_execute = await breaker._can_execute()
        assert breaker.is_half_open
        assert can_execute

    @pytest.mark.asyncio
    async def test_closes_on_half_open_success(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01))
        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker._can_execute()
        await breaker.record_success()
        assert breaker.is_closed

    @pytest.mark.asyncio
    async def test_reopens_on_half_open_failure(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01))
        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker._can_execute()
        await breaker.record_failure()
        assert breaker.is_open


class TestCircuitBreakerExecution:
    @pytest.mark.asyncio
    async def test_execute_success(self):
        breaker = CircuitBreaker()
        operation = AsyncMock(return_value="result")
        result = await breaker.execute(operation)
        assert result == "result"
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_failure_recorded(self):
        breaker = CircuitBreaker()
        operation = AsyncMock(side_effect=ValueError("Error"))
        with pytest.raises(ValueError):
            await breaker.execute(operation)
        assert breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_execute_blocked_when_open(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=60.0))
        await breaker.record_failure()
        assert breaker.is_open
        operation = AsyncMock(return_value="result")
        with pytest.raises(CircuitBreakerOpenError):
            await breaker.execute(operation)
        operation.assert_not_called()

    @pytest.mark.asyncio
    async def test_half_open_limits_calls(self):
        breaker = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01, half_open_max_calls=1)
        )
        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker._can_execute()
        assert breaker.is_half_open
        can_execute = await breaker._can_execute()
        assert not can_execute


class TestCircuitBreakerReset:
    @pytest.mark.asyncio
    async def test_reset_clears_state(self):
        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
        await breaker.record_failure()
        assert breaker.is_open
        breaker.reset()
        assert breaker.is_closed
        assert breaker.failure_count == 0

    def test_to_dict(self):
        breaker = CircuitBreaker()
        result = breaker.to_dict()
        assert result["state"] == "closed"
        assert result["failure_count"] == 0
        assert result["last_failure_time"] is None
        assert result["half_open_calls"] == 0


class TestCircuitBreakerOpenError:
    def test_create_exception(self):
        error = CircuitBreakerOpenError("Circuit is open", recovery_time=1000.0)
        assert "Circuit is open" in str(error)
        assert error.recovery_time == 1000.0

    def test_is_exception(self):
        error = CircuitBreakerOpenError("Open", recovery_time=0)
        assert isinstance(error, Exception)
