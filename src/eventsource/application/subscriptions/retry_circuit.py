"""Circuit breaker implementation for preventing cascading failures."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from eventsource.application.subscriptions.retry_types import (
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
)

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """
    Circuit breaker for preventing cascading failures.

    The circuit breaker pattern prevents repeated calls to a failing
    service, giving it time to recover while avoiding resource exhaustion.

    States:
        CLOSED: Normal operation, requests flow through
        OPEN: Too many failures, requests are blocked
        HALF_OPEN: Testing if service recovered

    Attributes:
        config: Circuit breaker configuration
        state: Current circuit state

    Example:
        >>> breaker = CircuitBreaker()
        >>> async with breaker:
        ...     await risky_operation()
    """

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        """
        Initialize the circuit breaker.

        Args:
            config: Circuit breaker configuration
        """
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get current failure count."""
        return self._failure_count

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (allowing requests)."""
        return self._state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking requests)."""
        return self._state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        """Check if circuit is half-open (testing recovery)."""
        return self._state == CircuitState.HALF_OPEN

    async def _check_state(self) -> None:
        """Check and possibly update circuit state based on timeout."""
        async with self._lock:
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                elapsed = time.monotonic() - self._last_failure_time
                if elapsed >= self.config.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info(
                        "Circuit breaker entering half-open state",
                        extra={"elapsed_seconds": elapsed},
                    )

    async def _can_execute(self) -> bool:
        """Check if a request can be executed."""
        await self._check_state()

        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls < self.config.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False

            # State is OPEN
            return False

    async def record_success(self) -> None:
        """Record a successful operation."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                # Service recovered, close the circuit
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._half_open_calls = 0
                logger.info("Circuit breaker closed after successful recovery")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    async def record_failure(self) -> None:
        """Record a failed operation."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                # Recovery attempt failed, reopen circuit
                self._state = CircuitState.OPEN
                logger.warning(
                    "Circuit breaker reopened after failed recovery attempt",
                    extra={"failure_count": self._failure_count},
                )
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.warning(
                        "Circuit breaker opened due to failure threshold",
                        extra={
                            "failure_count": self._failure_count,
                            "threshold": self.config.failure_threshold,
                        },
                    )

    async def execute[T](
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str = "operation",
    ) -> T:
        """
        Execute an operation through the circuit breaker.

        Args:
            operation: Async function to execute
            operation_name: Name for logging

        Returns:
            Result of the operation

        Raises:
            CircuitBreakerOpenError: If circuit is open
            Exception: If operation fails
        """
        if not await self._can_execute():
            recovery_time = (
                self._last_failure_time + self.config.recovery_timeout
                if self._last_failure_time
                else time.monotonic() + self.config.recovery_timeout
            )
            raise CircuitBreakerOpenError(
                f"Circuit breaker is open for {operation_name}",
                recovery_time=recovery_time,
            )

        try:
            result = await operation()
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0
        logger.info("Circuit breaker reset to closed state")

    def to_dict(self) -> dict[str, Any]:
        """
        Convert circuit breaker state to dictionary.

        Returns:
            Dictionary representation of state
        """
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "last_failure_time": self._last_failure_time,
            "half_open_calls": self._half_open_calls,
        }


__all__ = ["CircuitBreaker"]
