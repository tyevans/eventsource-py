"""Configuration, exceptions, stats, and backoff calculation for retries and circuit breaker."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any

from eventsource.domain.exceptions import EventSourceError

TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    OSError,
)


class CircuitState(Enum):
    """State of the circuit breaker (CLOSED, OPEN, HALF_OPEN)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Configuration for exponential backoff retry behavior."""

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
    """Statistics for retry operations."""

    attempts: int = 0
    successes: int = 0
    failures: int = 0
    total_delay_seconds: float = 0.0
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to dictionary for serialization."""
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "total_delay_seconds": self.total_delay_seconds,
            "last_error": self.last_error,
        }


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

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


class RetryError(EventSourceError):
    """Raised when all retry attempts fail."""

    def __init__(self, message: str, attempts: int, last_error: Exception) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


class CircuitBreakerOpenError(EventSourceError):
    """Raised when the circuit breaker is open and blocking requests."""

    def __init__(self, message: str, recovery_time: float) -> None:
        super().__init__(message)
        self.recovery_time = recovery_time


def calculate_backoff(attempt: int, config: RetryConfig) -> float:
    """Calculate backoff delay with exponential growth and jitter."""
    delay = min(config.initial_delay * (config.exponential_base**attempt), config.max_delay)
    jitter_range = delay * config.jitter
    delay += random.uniform(-jitter_range, jitter_range)  # nosec B311 - not crypto
    return max(0, delay)


def is_retryable_exception(
    exception: Exception,
    retryable_exceptions: tuple[type[Exception], ...] = TRANSIENT_EXCEPTIONS,
) -> bool:
    """Check if an exception is retryable."""
    return isinstance(exception, retryable_exceptions)


__all__ = [
    "TRANSIENT_EXCEPTIONS",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitState",
    "RetryConfig",
    "RetryError",
    "RetryStats",
    "calculate_backoff",
    "is_retryable_exception",
]
