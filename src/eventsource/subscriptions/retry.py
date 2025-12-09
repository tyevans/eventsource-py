"""
Retry utilities for handling transient failures.

Provides exponential backoff with jitter for resilient operations,
and a circuit breaker pattern for preventing repeated failures.

This module provides:
- RetryConfig: Configuration for retry behavior
- RetryStats: Statistics for retry operations
- RetryError: Exception raised when all retries are exhausted
- CircuitBreaker: Circuit breaker for preventing cascading failures
- CircuitBreakerOpenError: Exception raised when circuit breaker is open
- calculate_backoff: Calculate delay with exponential backoff and jitter
- retry_async: Retry an async operation with exponential backoff
- RetryableOperation: Context for retryable operations
"""

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


# Common transient exceptions that should be retried
TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    OSError,  # Includes network errors
)


class CircuitState(Enum):
    """
    State of the circuit breaker.

    Attributes:
        CLOSED: Normal operation, requests are allowed through
        OPEN: Failure threshold exceeded, requests are blocked
        HALF_OPEN: Testing if service has recovered
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.

    Controls how retries are performed with exponential backoff.

    Attributes:
        max_retries: Maximum number of retry attempts (0 = no retries)
        initial_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay in seconds between retries
        exponential_base: Base for exponential backoff calculation
        jitter: Fraction of delay to add as random jitter (0-1)

    Example:
        >>> config = RetryConfig(
        ...     max_retries=5,
        ...     initial_delay=1.0,
        ...     max_delay=60.0,
        ... )
    """

    max_retries: int = 5
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.max_retries < 0:
            raise ValueError(
                f"max_retries must be >= 0, got {self.max_retries}. Use 0 for no retries."
            )

        if self.initial_delay <= 0:
            raise ValueError(f"initial_delay must be positive, got {self.initial_delay}.")

        if self.max_delay <= 0:
            raise ValueError(f"max_delay must be positive, got {self.max_delay}.")

        if self.max_delay < self.initial_delay:
            raise ValueError(
                f"max_delay ({self.max_delay}) must be >= initial_delay ({self.initial_delay})."
            )

        if self.exponential_base <= 1.0:
            raise ValueError(f"exponential_base must be > 1.0, got {self.exponential_base}.")

        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError(f"jitter must be between 0.0 and 1.0, got {self.jitter}.")


@dataclass
class RetryStats:
    """
    Statistics for retry operations.

    Tracks retry attempts and outcomes for monitoring.

    Attributes:
        attempts: Total number of attempts (including initial)
        successes: Number of successful attempts
        failures: Number of failed attempts
        total_delay_seconds: Total time spent in delays
        last_error: String representation of the last error
    """

    attempts: int = 0
    successes: int = 0
    failures: int = 0
    total_delay_seconds: float = 0.0
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """
        Convert stats to dictionary for serialization.

        Returns:
            Dictionary representation of stats
        """
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "total_delay_seconds": self.total_delay_seconds,
            "last_error": self.last_error,
        }


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for circuit breaker behavior.

    Attributes:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Seconds to wait before attempting recovery
        half_open_max_calls: Max calls allowed in half-open state
    """

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {self.failure_threshold}.")

        if self.recovery_timeout <= 0:
            raise ValueError(f"recovery_timeout must be positive, got {self.recovery_timeout}.")

        if self.half_open_max_calls < 1:
            raise ValueError(f"half_open_max_calls must be >= 1, got {self.half_open_max_calls}.")


class RetryError(Exception):
    """
    Raised when all retry attempts fail.

    Attributes:
        message: Error message
        attempts: Number of attempts made
        last_error: The last exception that was raised
    """

    def __init__(self, message: str, attempts: int, last_error: Exception) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


class CircuitBreakerOpenError(Exception):
    """
    Raised when the circuit breaker is open and blocking requests.

    Attributes:
        message: Error message
        recovery_time: Time when circuit may attempt recovery
    """

    def __init__(self, message: str, recovery_time: float) -> None:
        super().__init__(message)
        self.recovery_time = recovery_time


def calculate_backoff(
    attempt: int,
    config: RetryConfig,
) -> float:
    """
    Calculate backoff delay with exponential growth and jitter.

    Uses exponential backoff with random jitter to prevent
    thundering herd problems when multiple clients retry simultaneously.

    Args:
        attempt: Current attempt number (0-based)
        config: Retry configuration

    Returns:
        Delay in seconds

    Example:
        >>> config = RetryConfig(initial_delay=1.0, max_delay=60.0)
        >>> delay = calculate_backoff(0, config)  # ~1s
        >>> delay = calculate_backoff(3, config)  # ~8s
    """
    # Exponential backoff: initial * base^attempt
    delay = config.initial_delay * (config.exponential_base**attempt)

    # Cap at max delay
    delay = min(delay, config.max_delay)

    # Add jitter (random variation to prevent thundering herd)
    jitter_range = delay * config.jitter
    delay += random.uniform(-jitter_range, jitter_range)  # nosec B311 - not crypto

    # Ensure non-negative
    return max(0, delay)


def is_retryable_exception(
    exception: Exception,
    retryable_exceptions: tuple[type[Exception], ...] = TRANSIENT_EXCEPTIONS,
) -> bool:
    """
    Check if an exception is retryable.

    Args:
        exception: The exception to check
        retryable_exceptions: Tuple of exception types to retry

    Returns:
        True if the exception should be retried
    """
    return isinstance(exception, retryable_exceptions)


async def retry_async(
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

    async def execute(
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

    async def execute(
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
    # Configuration
    "RetryConfig",
    "RetryStats",
    "CircuitBreakerConfig",
    # Exceptions
    "RetryError",
    "CircuitBreakerOpenError",
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitState",
    # Functions
    "calculate_backoff",
    "retry_async",
    "is_retryable_exception",
    # Classes
    "RetryableOperation",
    # Constants
    "TRANSIENT_EXCEPTIONS",
]
