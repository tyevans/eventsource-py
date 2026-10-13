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
    """Tests for CircuitBreakerConfig creation and validation."""

    def test_default_values(self):
        """Test default configuration values."""
        config = CircuitBreakerConfig()
        assert config.failure_threshold == 5
        assert config.recovery_timeout == 30.0
        assert config.half_open_max_calls == 1

    def test_custom_values(self):
        """Test custom configuration values."""
        config = CircuitBreakerConfig(
            failure_threshold=10,
            recovery_timeout=60.0,
            half_open_max_calls=3,
        )
        assert config.failure_threshold == 10
        assert config.recovery_timeout == 60.0
        assert config.half_open_max_calls == 3

    def test_zero_failure_threshold_raises(self):
        """Test failure_threshold < 1 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            CircuitBreakerConfig(failure_threshold=0)
        assert "failure_threshold must be >= 1" in str(exc_info.value)

    def test_zero_recovery_timeout_raises(self):
        """Test recovery_timeout <= 0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            CircuitBreakerConfig(recovery_timeout=0)
        assert "recovery_timeout must be positive" in str(exc_info.value)

    def test_zero_half_open_max_calls_raises(self):
        """Test half_open_max_calls < 1 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            CircuitBreakerConfig(half_open_max_calls=0)
        assert "half_open_max_calls must be >= 1" in str(exc_info.value)


class TestCircuitBreakerCreation:
    """Tests for CircuitBreaker creation."""

    def test_default_creation(self):
        """Test circuit breaker with default config."""
        breaker = CircuitBreaker()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_custom_config(self):
        """Test circuit breaker with custom config."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(config)
        assert breaker.config.failure_threshold == 3


class TestCircuitBreakerState:
    """Tests for CircuitBreaker state transitions."""

    @pytest.mark.asyncio
    async def test_starts_closed(self):
        """Test circuit breaker starts in CLOSED state."""
        breaker = CircuitBreaker()
        assert breaker.is_closed
        assert not breaker.is_open
        assert not breaker.is_half_open

    @pytest.mark.asyncio
    async def test_opens_after_failure_threshold(self):
        """Test circuit opens after reaching failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(config)

        for _ in range(3):
            await breaker.record_failure()

        assert breaker.is_open
        assert breaker.failure_count == 3

    @pytest.mark.asyncio
    async def test_stays_closed_below_threshold(self):
        """Test circuit stays closed below failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        await breaker.record_failure()

        assert breaker.is_closed
        assert breaker.failure_count == 2

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        """Test success resets failure count when closed."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        await breaker.record_failure()
        await breaker.record_success()

        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_transitions_to_half_open(self):
        """Test circuit transitions to half-open after recovery timeout."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.05,
        )
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        assert breaker.is_open

        await asyncio.sleep(0.1)
        # The state check happens in _can_execute
        can_execute = await breaker._can_execute()

        assert breaker.is_half_open
        assert can_execute

    @pytest.mark.asyncio
    async def test_closes_on_half_open_success(self):
        """Test circuit closes after successful half-open request."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.01,
        )
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker._can_execute()  # Transition to half-open
        await breaker.record_success()

        assert breaker.is_closed

    @pytest.mark.asyncio
    async def test_reopens_on_half_open_failure(self):
        """Test circuit reopens after failed half-open request."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.01,
        )
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        await asyncio.sleep(0.02)
        await breaker._can_execute()  # Transition to half-open
        await breaker.record_failure()

        assert breaker.is_open


class TestCircuitBreakerExecution:
    """Tests for CircuitBreaker execute method."""

    @pytest.mark.asyncio
    async def test_execute_success(self):
        """Test successful execution through circuit breaker."""
        breaker = CircuitBreaker()
        operation = AsyncMock(return_value="result")

        result = await breaker.execute(operation)

        assert result == "result"
        operation.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_failure_recorded(self):
        """Test failure is recorded on exception."""
        breaker = CircuitBreaker()
        operation = AsyncMock(side_effect=ValueError("Error"))

        with pytest.raises(ValueError):
            await breaker.execute(operation)

        assert breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_execute_blocked_when_open(self):
        """Test execution blocked when circuit is open."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=60.0,
        )
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        assert breaker.is_open

        operation = AsyncMock(return_value="result")

        with pytest.raises(CircuitBreakerOpenError):
            await breaker.execute(operation)

        operation.assert_not_called()

    @pytest.mark.asyncio
    async def test_half_open_limits_calls(self):
        """Test half-open state limits concurrent calls."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.01,
            half_open_max_calls=1,
        )
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        await asyncio.sleep(0.02)

        # First call should be allowed
        await breaker._can_execute()
        assert breaker.is_half_open

        # Second call should be blocked
        can_execute = await breaker._can_execute()
        assert not can_execute


class TestCircuitBreakerReset:
    """Tests for CircuitBreaker reset method."""

    @pytest.mark.asyncio
    async def test_reset_clears_state(self):
        """Test reset clears all state."""
        config = CircuitBreakerConfig(failure_threshold=1)
        breaker = CircuitBreaker(config)

        await breaker.record_failure()
        assert breaker.is_open

        breaker.reset()

        assert breaker.is_closed
        assert breaker.failure_count == 0

    def test_to_dict(self):
        """Test to_dict method."""
        breaker = CircuitBreaker()
        result = breaker.to_dict()

        assert result["state"] == "closed"
        assert result["failure_count"] == 0
        assert result["last_failure_time"] is None
        assert result["half_open_calls"] == 0


class TestCircuitBreakerOpenError:
    """Tests for CircuitBreakerOpenError exception."""

    def test_create_exception(self):
        """Test creating CircuitBreakerOpenError exception."""
        error = CircuitBreakerOpenError("Circuit is open", recovery_time=1000.0)

        assert "Circuit is open" in str(error)
        assert error.recovery_time == 1000.0

    def test_is_exception(self):
        """Test CircuitBreakerOpenError is an Exception."""
        error = CircuitBreakerOpenError("Open", recovery_time=0)
        assert isinstance(error, Exception)
