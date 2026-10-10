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

from eventsource.application.subscriptions.retry_circuit import CircuitBreaker
from eventsource.application.subscriptions.retry_operation import (
    RetryableOperation,
    retry_async,
)
from eventsource.application.subscriptions.retry_types import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    RetryConfig,
    RetryError,
    RetryStats,
    calculate_backoff,
    is_retryable_exception,
)

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
