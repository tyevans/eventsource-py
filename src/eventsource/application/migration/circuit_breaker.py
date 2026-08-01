"""
Circuit breaker for migration operations.

Trips after a configured number of consecutive failures and rejects calls
while open, preventing a struggling backend from being hammered during a
migration. `CircuitBreakerOpenError` lives with the rest of the migration
exception taxonomy in `exceptions.py` (ADR 0044); this module imports it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any
from uuid import UUID

from eventsource.application.migration.exceptions import CircuitBreakerOpenError

logger = logging.getLogger(__name__)

__all__ = [
    "CircuitState",
    "CircuitBreakerConfig",
    "CircuitBreaker",
    "CircuitBreakerContext",
]


class CircuitState(Enum):
    """
    Circuit breaker state.

    Attributes:
        CLOSED: Circuit is closed, operations proceed normally.
        OPEN: Circuit is open, operations are rejected immediately.
        HALF_OPEN: Circuit is testing if operations can succeed again.
    """

    CLOSED = "closed"
    """Circuit is closed, operations proceed normally."""

    OPEN = "open"
    """Circuit is open, operations are rejected immediately."""

    HALF_OPEN = "half_open"
    """Circuit is testing if operations can succeed again."""


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for circuit breaker behavior.

    Attributes:
        failure_threshold: Number of failures before opening circuit.
        success_threshold: Number of successes in half-open state to close.
        timeout_seconds: Seconds before trying half-open state.
        excluded_exceptions: Exception types that don't trip the circuit.
    """

    failure_threshold: int = 5
    """Number of failures before opening circuit."""

    success_threshold: int = 2
    """Number of successes in half-open state to close circuit."""

    timeout_seconds: float = 30.0
    """Seconds before trying half-open state."""

    excluded_exceptions: tuple[type[Exception], ...] = ()
    """Exception types that don't trip the circuit."""


class CircuitBreaker:
    """
    Circuit breaker implementation for migration operations.

    Tracks failures and opens the circuit when a threshold is reached,
    preventing cascading failures during system instability.

    Usage:
        >>> cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=5))
        >>> async with cb.protect("my_operation"):
        ...     await risky_operation()

    Attributes:
        config: Circuit breaker configuration.
        state: Current circuit state.
        failure_count: Number of consecutive failures.
        success_count: Number of consecutive successes in half-open state.
        last_failure_time: Timestamp of last failure.
    """

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        name: str = "default",
    ) -> None:
        """
        Initialize the circuit breaker.

        Args:
            config: Circuit breaker configuration (uses defaults if None).
            name: Name for logging and identification.
        """
        self.config = config or CircuitBreakerConfig()
        self.name = name
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get the current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get the current failure count."""
        return self._failure_count

    async def _check_state(self) -> None:
        """Check and potentially update the circuit state."""
        if self._state == CircuitState.OPEN and self._last_failure_time is not None:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.config.timeout_seconds:
                logger.info(
                    "Circuit breaker '%s' transitioning to half-open after %.1fs",
                    self.name,
                    elapsed,
                )
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0

    async def _record_success(self) -> None:
        """Record a successful operation."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    logger.info(
                        "Circuit breaker '%s' closing after %d successes",
                        self.name,
                        self._success_count,
                    )
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def _record_failure(self, exc: Exception) -> None:
        """Record a failed operation."""
        # Check if this exception should trip the circuit
        if isinstance(exc, self.config.excluded_exceptions):
            return

        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(
                    "Circuit breaker '%s' opening from half-open after failure",
                    self.name,
                )
                self._state = CircuitState.OPEN
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.config.failure_threshold
            ):
                logger.warning(
                    "Circuit breaker '%s' opening after %d failures",
                    self.name,
                    self._failure_count,
                )
                self._state = CircuitState.OPEN

    def get_time_until_retry(self) -> float:
        """Get seconds until the circuit will try half-open."""
        if self._state != CircuitState.OPEN or self._last_failure_time is None:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self.config.timeout_seconds - elapsed)

    async def protect(
        self,
        operation_name: str,
        migration_id: UUID | None = None,
    ) -> CircuitBreakerContext:
        """
        Create a context manager for protecting an operation.

        Args:
            operation_name: Name of the operation for logging.
            migration_id: Optional migration ID for error context.

        Returns:
            Context manager that protects the operation.

        Raises:
            CircuitBreakerOpenError: If the circuit is open.
        """
        await self._check_state()

        if self._state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(
                operation_name=operation_name,
                time_until_retry=self.get_time_until_retry(),
                migration_id=migration_id,
            )

        return CircuitBreakerContext(
            circuit_breaker=self,
            operation_name=operation_name,
        )

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None


class CircuitBreakerContext:
    """Context manager for circuit breaker protected operations."""

    def __init__(
        self,
        circuit_breaker: CircuitBreaker,
        operation_name: str,
    ) -> None:
        self._cb = circuit_breaker
        self._operation_name = operation_name

    async def __aenter__(self) -> CircuitBreakerContext:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if exc_val is None:
            await self._cb._record_success()
        elif isinstance(exc_val, Exception):
            await self._cb._record_failure(exc_val)
        return False  # Don't suppress exceptions
