"""Async retry execution and retryable operation context."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.retry_types import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreakerOpenError,
    RetryConfig,
    RetryError,
    RetryStats,
    calculate_backoff,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.retry_circuit import CircuitBreaker

logger = logging.getLogger(__name__)


async def retry_async[T](
    operation: Callable[[], Awaitable[T]],
    config: RetryConfig | None = None,
    retryable_exceptions: tuple[type[Exception], ...] = TRANSIENT_EXCEPTIONS,
    operation_name: str = "operation",
) -> T:
    """
    Retry an async operation with exponential backoff and jitter on transient failures.

    Raises RetryError if all retries are exhausted, or re-raises non-retryable exceptions.
    """
    config = config or RetryConfig()
    stats = RetryStats()
    last_error: Exception | None = None

    for attempt in range(config.max_retries + 1):
        stats.attempts += 1
        try:
            result = await operation()
            stats.successes += 1
            if attempt > 0:
                logger.info(
                    f"Operation {operation_name} succeeded after retry",
                    extra={
                        "operation": operation_name,
                        "attempt": attempt + 1,
                        "total_attempts": stats.attempts,
                    },
                )
            return result
        except retryable_exceptions as e:
            last_error = e
            stats.failures += 1
            stats.last_error = str(e)
            if attempt < config.max_retries:
                delay = calculate_backoff(attempt, config)
                stats.total_delay_seconds += delay
                logger.warning(
                    f"Retrying {operation_name} after failure",
                    extra={
                        "operation": operation_name,
                        "attempt": attempt + 1,
                        "max_retries": config.max_retries,
                        "delay_seconds": delay,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"All retries exhausted for {operation_name}",
                    extra={
                        "operation": operation_name,
                        "attempts": stats.attempts,
                        "total_delay_seconds": stats.total_delay_seconds,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
        except Exception:
            logger.error(
                f"Non-retryable error in {operation_name}",
                extra={"operation": operation_name, "attempt": attempt + 1},
                exc_info=True,
            )
            raise

    assert last_error is not None
    raise RetryError(
        f"Failed after {stats.attempts} attempts: {last_error}",
        attempts=stats.attempts,
        last_error=last_error,
    )


@dataclass
class RetryableOperation:
    """Context for operations that should be retried on failure with optional circuit breaker."""

    config: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreaker | None = None
    _stats: RetryStats = field(default_factory=RetryStats, init=False, repr=False)

    async def execute[T](
        self,
        operation: Callable[[], Awaitable[T]],
        name: str = "operation",
        retryable_exceptions: tuple[type[Exception], ...] = TRANSIENT_EXCEPTIONS,
    ) -> T:
        """Execute an operation with retry logic and optional circuit breaker protection."""
        if self.circuit_breaker:
            cb = self.circuit_breaker

            async def protected_operation() -> T:
                return await cb.execute(operation, name)

            return await retry_async(
                operation=protected_operation,
                config=self.config,
                retryable_exceptions=retryable_exceptions + (CircuitBreakerOpenError,),
                operation_name=name,
            )
        return await retry_async(
            operation=operation,
            config=self.config,
            retryable_exceptions=retryable_exceptions,
            operation_name=name,
        )

    @property
    def stats(self) -> RetryStats:
        """Get retry statistics."""
        return self._stats


__all__ = ["RetryableOperation", "retry_async"]
