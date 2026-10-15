"""Circuit breaker for migration operations (see ADR-0044)."""

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

__all__ = ["CircuitState", "CircuitBreakerConfig", "CircuitBreaker", "CircuitBreakerContext"]


class CircuitState(Enum):
    """Circuit breaker state (CLOSED, OPEN, or HALF_OPEN)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    excluded_exceptions: tuple[type[Exception], ...] = ()


class CircuitBreaker:
    """Circuit breaker tracking failures and opening when threshold is reached."""

    def __init__(self, config: CircuitBreakerConfig | None = None, name: str = "default") -> None:
        self.config = config or CircuitBreakerConfig()
        self.name = name
        self._state = CircuitState.CLOSED
        self._failure_count = self._success_count = 0
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
                logger.info("Circuit breaker '%s' transitioning to half-open after %.1fs", self.name, elapsed)  # fmt: skip
                self._state, self._success_count = CircuitState.HALF_OPEN, 0

    async def _record_success(self) -> None:
        """Record a successful operation."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    logger.info("Circuit breaker '%s' closing after %d successes", self.name, self._success_count)  # fmt: skip
                    self._state, self._failure_count = CircuitState.CLOSED, 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def _record_failure(self, exc: Exception) -> None:
        """Record a failed operation."""
        if isinstance(exc, self.config.excluded_exceptions):
            return
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN:
                logger.warning("Circuit breaker '%s' opening from half-open after failure", self.name)  # fmt: skip
                self._state = CircuitState.OPEN
            elif self._state == CircuitState.CLOSED and self._failure_count >= self.config.failure_threshold:  # fmt: skip
                logger.warning("Circuit breaker '%s' opening after %d failures", self.name, self._failure_count)  # fmt: skip
                self._state = CircuitState.OPEN

    def get_time_until_retry(self) -> float:
        """Get seconds until the circuit will try half-open."""
        if self._state != CircuitState.OPEN or self._last_failure_time is None:
            return 0.0
        return max(0.0, self.config.timeout_seconds - (time.monotonic() - self._last_failure_time))

    async def protect(self, operation_name: str, migration_id: UUID | None = None) -> CircuitBreakerContext:  # fmt: skip
        """Create a context manager for protecting an operation, or raise CircuitBreakerOpenError."""
        await self._check_state()
        if self._state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(
                operation_name=operation_name,
                time_until_retry=self.get_time_until_retry(),
                migration_id=migration_id,
            )
        return CircuitBreakerContext(circuit_breaker=self, operation_name=operation_name)

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = self._success_count = 0
        self._last_failure_time = None


class CircuitBreakerContext:
    """Context manager for circuit breaker protected operations."""

    def __init__(self, circuit_breaker: CircuitBreaker, operation_name: str) -> None:
        self._cb, self._operation_name = circuit_breaker, operation_name

    async def __aenter__(self) -> CircuitBreakerContext:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> bool:
        if exc_val is None:
            await self._cb._record_success()
        elif isinstance(exc_val, Exception):
            await self._cb._record_failure(exc_val)
        return False
