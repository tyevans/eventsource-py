"""
Unit tests for retry utilities and circuit breaker pattern.

Tests for:
- RetryConfig validation
- RetryStats tracking
- RetryError exception
- calculate_backoff function
- retry_async function
- CircuitBreaker class
- CircuitBreakerOpenError exception
- RetryableOperation class
"""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from eventsource.subscriptions.retry import (
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

# RetryConfig Tests


class TestRetryConfigCreation:
    """Tests for RetryConfig creation and validation."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_retries == 5
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter == 0.1

    def test_custom_values(self):
        """Test custom configuration values."""
        config = RetryConfig(
            max_retries=3,
            initial_delay=0.5,
            max_delay=30.0,
            exponential_base=3.0,
            jitter=0.2,
        )
        assert config.max_retries == 3
        assert config.initial_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 3.0
        assert config.jitter == 0.2

    def test_zero_retries_allowed(self):
        """Test max_retries=0 is valid (no retries)."""
        config = RetryConfig(max_retries=0)
        assert config.max_retries == 0

    def test_negative_max_retries_raises(self):
        """Test negative max_retries raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(max_retries=-1)
        assert "max_retries must be >= 0" in str(exc_info.value)

    def test_zero_initial_delay_raises(self):
        """Test zero initial_delay raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(initial_delay=0)
        assert "initial_delay must be positive" in str(exc_info.value)

    def test_negative_initial_delay_raises(self):
        """Test negative initial_delay raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(initial_delay=-1.0)
        assert "initial_delay must be positive" in str(exc_info.value)

    def test_zero_max_delay_raises(self):
        """Test zero max_delay raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(max_delay=0)
        assert "max_delay must be positive" in str(exc_info.value)

    def test_max_delay_less_than_initial_raises(self):
        """Test max_delay < initial_delay raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(initial_delay=10.0, max_delay=5.0)
        assert "max_delay" in str(exc_info.value) and "initial_delay" in str(exc_info.value)

    def test_exponential_base_too_low_raises(self):
        """Test exponential_base <= 1.0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(exponential_base=1.0)
        assert "exponential_base must be > 1.0" in str(exc_info.value)

    def test_jitter_negative_raises(self):
        """Test negative jitter raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(jitter=-0.1)
        assert "jitter must be between 0.0 and 1.0" in str(exc_info.value)

    def test_jitter_too_high_raises(self):
        """Test jitter > 1.0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(jitter=1.1)
        assert "jitter must be between 0.0 and 1.0" in str(exc_info.value)

    def test_jitter_zero_allowed(self):
        """Test jitter=0 is valid (no jitter)."""
        config = RetryConfig(jitter=0.0)
        assert config.jitter == 0.0

    def test_jitter_one_allowed(self):
        """Test jitter=1.0 is valid (maximum jitter)."""
        config = RetryConfig(jitter=1.0)
        assert config.jitter == 1.0


# RetryStats Tests


class TestRetryStats:
    """Tests for RetryStats dataclass."""

    def test_default_values(self):
        """Test default stats values."""
        stats = RetryStats()
        assert stats.attempts == 0
        assert stats.successes == 0
        assert stats.failures == 0
        assert stats.total_delay_seconds == 0.0
        assert stats.last_error is None

    def test_to_dict(self):
        """Test to_dict conversion."""
        stats = RetryStats(
            attempts=5,
            successes=1,
            failures=4,
            total_delay_seconds=15.5,
            last_error="Connection refused",
        )
        result = stats.to_dict()

        assert result["attempts"] == 5
        assert result["successes"] == 1
        assert result["failures"] == 4
        assert result["total_delay_seconds"] == 15.5
        assert result["last_error"] == "Connection refused"

    def test_to_dict_with_none_error(self):
        """Test to_dict with None error."""
        stats = RetryStats()
        result = stats.to_dict()
        assert result["last_error"] is None


# RetryError Tests


class TestRetryError:
    """Tests for RetryError exception."""

    def test_create_error(self):
        """Test creating RetryError."""
        original = ValueError("Original error")
        error = RetryError("Failed after 5 attempts", attempts=5, last_error=original)

        assert "Failed after 5 attempts" in str(error)
        assert error.attempts == 5
        assert error.last_error is original

    def test_error_is_exception(self):
        """Test RetryError is an Exception."""
        original = ConnectionError("Connection refused")
        error = RetryError("Failed", attempts=3, last_error=original)

        assert isinstance(error, Exception)


# calculate_backoff Tests


class TestCalculateBackoff:
    """Tests for calculate_backoff function."""

    def test_first_attempt(self):
        """Test backoff for first attempt (attempt=0)."""
        config = RetryConfig(initial_delay=1.0, jitter=0.0)
        delay = calculate_backoff(0, config)
        assert delay == 1.0

    def test_exponential_growth(self):
        """Test exponential growth of backoff."""
        config = RetryConfig(
            initial_delay=1.0,
            exponential_base=2.0,
            max_delay=100.0,
            jitter=0.0,
        )

        assert calculate_backoff(0, config) == 1.0
        assert calculate_backoff(1, config) == 2.0
        assert calculate_backoff(2, config) == 4.0
        assert calculate_backoff(3, config) == 8.0
        assert calculate_backoff(4, config) == 16.0

    def test_max_delay_cap(self):
        """Test backoff is capped at max_delay."""
        config = RetryConfig(
            initial_delay=1.0,
            max_delay=10.0,
            jitter=0.0,
        )

        # After 4 attempts, delay would be 16s but capped at 10s
        assert calculate_backoff(4, config) == 10.0
        assert calculate_backoff(10, config) == 10.0

    def test_jitter_adds_variation(self):
        """Test jitter adds random variation to delay."""
        config = RetryConfig(
            initial_delay=10.0,
            jitter=0.5,  # 50% jitter
            max_delay=100.0,
        )

        # With 50% jitter, delay should be between 5s and 15s
        delays = [calculate_backoff(0, config) for _ in range(100)]

        assert min(delays) >= 5.0
        assert max(delays) <= 15.0
        # Check there's actual variation
        assert len({round(d, 2) for d in delays}) > 1

    def test_jitter_zero_no_variation(self):
        """Test jitter=0 produces consistent delays."""
        config = RetryConfig(initial_delay=5.0, jitter=0.0)

        delays = [calculate_backoff(0, config) for _ in range(10)]
        assert all(d == 5.0 for d in delays)

    def test_delay_never_negative(self):
        """Test delay is never negative."""
        config = RetryConfig(
            initial_delay=0.1,
            jitter=1.0,  # Maximum jitter
        )

        for _ in range(100):
            delay = calculate_backoff(0, config)
            assert delay >= 0


# is_retryable_exception Tests


class TestIsRetryableException:
    """Tests for is_retryable_exception function."""

    def test_connection_error_is_retryable(self):
        """Test ConnectionError is retryable."""
        assert is_retryable_exception(ConnectionError("Connection refused"))

    def test_timeout_error_is_retryable(self):
        """Test TimeoutError is retryable."""
        assert is_retryable_exception(TimeoutError("Timed out"))

    def test_asyncio_timeout_is_retryable(self):
        """Test asyncio.TimeoutError is retryable."""
        assert is_retryable_exception(TimeoutError())

    def test_os_error_is_retryable(self):
        """Test OSError is retryable."""
        assert is_retryable_exception(OSError("Network unreachable"))

    def test_value_error_not_retryable(self):
        """Test ValueError is not retryable by default."""
        assert not is_retryable_exception(ValueError("Invalid value"))

    def test_custom_retryable_exceptions(self):
        """Test custom retryable exceptions."""

        class CustomError(Exception):
            pass

        assert is_retryable_exception(
            CustomError("Custom"),
            retryable_exceptions=(CustomError,),
        )
        assert not is_retryable_exception(
            ValueError("Value"),
            retryable_exceptions=(CustomError,),
        )


# retry_async Tests


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


# CircuitBreakerConfig Tests


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


# CircuitBreaker Tests


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


# CircuitBreakerOpenError Tests


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


# RetryableOperation Tests


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


# TRANSIENT_EXCEPTIONS Tests


class TestTransientExceptions:
    """Tests for TRANSIENT_EXCEPTIONS constant."""

    def test_includes_connection_error(self):
        """Test ConnectionError is in transient exceptions."""
        assert ConnectionError in TRANSIENT_EXCEPTIONS

    def test_includes_timeout_error(self):
        """Test TimeoutError is in transient exceptions."""
        assert TimeoutError in TRANSIENT_EXCEPTIONS

    def test_includes_asyncio_timeout(self):
        """Test asyncio.TimeoutError is in transient exceptions."""
        assert asyncio.TimeoutError in TRANSIENT_EXCEPTIONS

    def test_includes_os_error(self):
        """Test OSError is in transient exceptions."""
        assert OSError in TRANSIENT_EXCEPTIONS


# Integration Tests


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


# Module Import Tests


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_retry_module(self):
        """Test imports from retry module."""
        from eventsource.subscriptions.retry import (
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
        from eventsource.subscriptions import (
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
