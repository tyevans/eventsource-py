"""Unit tests for the migration circuit breaker (CircuitBreaker, CircuitBreakerConfig, CircuitState)."""

import asyncio

import pytest

from eventsource.application.migration.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)
from eventsource.application.migration.exceptions import CircuitBreakerOpenError


class TestCircuitBreaker:
    """Tests for CircuitBreaker implementation."""

    @pytest.fixture
    def circuit_breaker(self) -> CircuitBreaker:
        """Create a circuit breaker with low thresholds for testing."""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout_seconds=0.1,  # Very short for testing
        )
        return CircuitBreaker(config=config, name="test")

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit starts in closed state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_successful_operation(self, circuit_breaker: CircuitBreaker) -> None:
        """Test successful operation keeps circuit closed."""
        ctx = await circuit_breaker.protect("test_op")
        async with ctx:
            pass  # Success

        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_failure_increments_count(self, circuit_breaker: CircuitBreaker) -> None:
        """Test failure increments failure count."""
        ctx = await circuit_breaker.protect("test_op")
        with pytest.raises(ValueError):
            async with ctx:
                raise ValueError("Test error")

        assert circuit_breaker.failure_count == 1
        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_circuit_opens_after_threshold(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit opens after failure threshold."""
        # Cause enough failures to open circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_operations(self, circuit_breaker: CircuitBreaker) -> None:
        """Test open circuit rejects new operations."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        # Next operation should be rejected
        with pytest.raises(CircuitBreakerOpenError) as exc_info:
            await circuit_breaker.protect("test_op")

        assert "Circuit breaker open" in str(exc_info.value)
        assert exc_info.value.time_until_retry >= 0

    @pytest.mark.asyncio
    async def test_circuit_transitions_to_half_open(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit transitions to half-open after timeout."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Should allow operation in half-open state
        ctx = await circuit_breaker.protect("test_op")
        assert circuit_breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_closes_on_success(self, circuit_breaker: CircuitBreaker) -> None:
        """Test half-open circuit closes after successful operations."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Success in half-open state
        for _ in range(2):  # success_threshold
            ctx = await circuit_breaker.protect("test_op")
            async with ctx:
                pass

        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_reopens_on_failure(self, circuit_breaker: CircuitBreaker) -> None:
        """Test half-open circuit reopens on failure."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Failure in half-open state
        ctx = await circuit_breaker.protect("test_op")
        with pytest.raises(ValueError):
            async with ctx:
                raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_reset(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit can be manually reset."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

        # Reset
        circuit_breaker.reset()

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_excluded_exceptions_dont_trip_circuit(self) -> None:
        """Test excluded exceptions don't trip the circuit."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            excluded_exceptions=(ValueError,),
        )
        cb = CircuitBreaker(config=config, name="test")

        # Excluded exception shouldn't count
        for _ in range(5):
            ctx = await cb.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Excluded")

        assert cb.state == CircuitState.CLOSED

        # Non-excluded exception should count
        for _ in range(2):
            ctx = await cb.protect("test_op")
            with pytest.raises(RuntimeError):
                async with ctx:
                    raise RuntimeError("Not excluded")

        assert cb.state == CircuitState.OPEN
