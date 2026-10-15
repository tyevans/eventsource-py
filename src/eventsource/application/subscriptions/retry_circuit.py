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
    """Circuit breaker for preventing cascading failures (CLOSED, OPEN, HALF_OPEN)."""

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        """Initialize the circuit breaker."""
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    @property
    def is_closed(self) -> bool:
        return self._state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self._state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        return self._state == CircuitState.HALF_OPEN

    async def _check_state(self) -> None:
        async with self._lock:
            if self._state == CircuitState.OPEN and self._last_failure_time is not None:
                elapsed = time.monotonic() - self._last_failure_time
                if elapsed >= self.config.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    extra = {"elapsed_seconds": elapsed}
                    logger.info("Circuit breaker entering half-open state", extra=extra)

    async def _can_execute(self) -> bool:
        await self._check_state()
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            if (
                self._state == CircuitState.HALF_OPEN
                and self._half_open_calls < self.config.half_open_max_calls
            ):
                self._half_open_calls += 1
                return True
            return False

    async def record_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._half_open_calls = 0
                logger.info("Circuit breaker closed after successful recovery")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def record_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                extra = {"failure_count": self._failure_count}
                logger.warning(
                    "Circuit breaker reopened after failed recovery attempt", extra=extra
                )
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.config.failure_threshold
            ):
                self._state = CircuitState.OPEN
                extra = {
                    "failure_count": self._failure_count,
                    "threshold": self.config.failure_threshold,
                }
                logger.warning("Circuit breaker opened due to failure threshold", extra=extra)

    async def execute[T](
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str = "operation",
    ) -> T:
        """Execute an operation through the circuit breaker, raising CircuitBreakerOpenError if open."""
        if not await self._can_execute():
            base_time = self._last_failure_time or time.monotonic()
            raise CircuitBreakerOpenError(
                f"Circuit breaker is open for {operation_name}",
                recovery_time=base_time + self.config.recovery_timeout,
            )
        try:
            result = await operation()
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise

    def reset(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0
        logger.info("Circuit breaker reset to closed state")

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "last_failure_time": self._last_failure_time,
            "half_open_calls": self._half_open_calls,
        }


__all__ = ["CircuitBreaker"]
