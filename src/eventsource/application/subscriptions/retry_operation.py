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
    Retry an async operation with exponential backoff.

    Executes the operation and retries on transient failures using
    exponential backoff with jitter.

    Args:
        operation: Async function to retry
        config: Retry configuration (uses defaults if None)
        retryable_exceptions: Exception types to retry on
        operation_name: Name for logging purposes

    Returns:
        Result of successful operation

    Raises:
        RetryError: If all retries exhausted
        Exception: Non-retryable exceptions are raised immediately

    Example:
        >>> async def fetch_data():
        ...     return await http_client.get("/data")
        >>> data = await retry_async(fetch_data, operation_name="fetch_data")
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
            # Non-retryable exception, raise immediately
            logger.error(
                f"Non-retryable error in {operation_name}",
                extra={
                    "operation": operation_name,
                    "attempt": attempt + 1,
                },
                exc_info=True,
            )
            raise

    # last_error is guaranteed to be set if we reach here (loop only exits after failure)
    assert last_error is not None
    raise RetryError(
        f"Failed after {stats.attempts} attempts: {last_error}",
        attempts=stats.attempts,
        last_error=last_error,
    )


@dataclass
class RetryableOperation:
    """
    Context for operations that should be retried on failure.

    Combines retry logic with optional circuit breaker protection.

    Attributes:
        config: Retry configuration
        circuit_breaker: Optional circuit breaker for protection

    Example:
        >>> retry = RetryableOperation(RetryConfig(max_retries=3))
        >>> result = await retry.execute(
        ...     lambda: event_store.read_all(),
        ...     name="read_events",
        ... )
    """

    config: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreaker | None = None
    _stats: RetryStats = field(default_factory=RetryStats, init=False, repr=False)

    async def execute[T](
        self,
        operation: Callable[[], Awaitable[T]],
        name: str = "operation",
        retryable_exceptions: tuple[type[Exception], ...] = TRANSIENT_EXCEPTIONS,
    ) -> T:
        """
        Execute an operation with retry logic.

        Args:
            operation: Async function to execute
            name: Operation name for logging
            retryable_exceptions: Exception types to retry

        Returns:
            Result of successful operation

        Raises:
            RetryError: If all retries exhausted
            CircuitBreakerOpenError: If circuit breaker is open
        """
        if self.circuit_breaker:
            # Wrap operation with circuit breaker
            cb = self.circuit_breaker  # Local ref for closure

            async def protected_operation() -> T:
                return await cb.execute(operation, name)

            return await retry_async(
                operation=protected_operation,
                config=self.config,
                retryable_exceptions=retryable_exceptions + (CircuitBreakerOpenError,),
                operation_name=name,
            )
        else:
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


__all__ = [
    "RetryableOperation",
    "retry_async",
]
