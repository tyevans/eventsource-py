"""
Retry policy abstraction for projection event processing.

This module provides configurable retry behavior for projections,
addressing the Single Responsibility Principle violation where
retry logic was embedded in CheckpointTrackingProjection.

The module reuses the existing RetryConfig from subscriptions.retry
for consistency across the codebase.

Example:
    >>> from eventsource.projections.retry import (
    ...     RetryPolicy,
    ...     ExponentialBackoffRetryPolicy,
    ... )
    >>>
    >>> # Use default retry settings
    >>> policy = ExponentialBackoffRetryPolicy()
    >>>
    >>> # Or customize
    >>> from eventsource.subscriptions.retry import RetryConfig
    >>> policy = ExponentialBackoffRetryPolicy(
    ...     config=RetryConfig(max_retries=5, initial_delay=1.0)
    ... )
"""

import logging
from typing import Protocol, runtime_checkable

from eventsource.subscriptions.retry import RetryConfig, calculate_backoff

logger = logging.getLogger(__name__)


@runtime_checkable
class RetryPolicy(Protocol):
    """
    Protocol for retry policies in projections.

    Defines the interface for determining retry behavior when
    event processing fails. Implementations can provide different
    strategies (exponential backoff, fixed delay, no retry, etc.)

    This abstraction allows retry behavior to be:
    - Configured independently of projection logic
    - Tested in isolation
    - Swapped without modifying projection code
    """

    @property
    def max_retries(self) -> int:
        """
        Maximum number of retry attempts.

        Does not include the initial attempt, so max_retries=3
        means up to 4 total attempts (1 initial + 3 retries).
        """
        ...

    def get_backoff(self, attempt: int) -> float:
        """
        Calculate the delay before the next retry attempt.

        Args:
            attempt: The current attempt number (0-based).
                    After first failure, attempt=0.

        Returns:
            Delay in seconds before the next attempt.
        """
        ...

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """
        Determine if a retry should be attempted.

        Args:
            attempt: The current attempt number (0-based)
            error: The exception that caused the failure

        Returns:
            True if another retry should be attempted, False otherwise.
        """
        ...


class ExponentialBackoffRetryPolicy:
    """
    Retry policy with exponential backoff.

    Uses the existing RetryConfig and calculate_backoff from
    subscriptions.retry for consistency.

    Default settings match the original CheckpointTrackingProjection:
    - max_retries: 3
    - initial_delay: 2.0 seconds (was RETRY_BACKOFF_BASE)
    - exponential_base: 2.0

    Backoff progression (default): 2s, 4s, 8s

    Example:
        >>> policy = ExponentialBackoffRetryPolicy()
        >>> policy.max_retries
        3
        >>> policy.get_backoff(0)  # After first failure
        2.0  # Plus jitter
        >>> policy.get_backoff(1)  # After second failure
        4.0  # Plus jitter
    """

    def __init__(self, config: RetryConfig | None = None) -> None:
        """
        Initialize the retry policy.

        Args:
            config: Retry configuration. If None, uses defaults that
                   match the original CheckpointTrackingProjection behavior:
                   - max_retries=3
                   - initial_delay=2.0
                   - exponential_base=2.0
                   - jitter=0.0 (deterministic for projection processing)
        """
        self._config = config or RetryConfig(
            max_retries=3,
            initial_delay=2.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=0.0,  # Projections use deterministic backoff
        )

    @property
    def max_retries(self) -> int:
        """Get the maximum number of retry attempts."""
        return self._config.max_retries

    @property
    def config(self) -> RetryConfig:
        """Get the underlying retry configuration."""
        return self._config

    def get_backoff(self, attempt: int) -> float:
        """
        Calculate backoff delay for the given attempt.

        Uses exponential backoff with optional jitter.

        Args:
            attempt: Current attempt number (0-based)

        Returns:
            Delay in seconds
        """
        return calculate_backoff(attempt, self._config)

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """
        Determine if retry should be attempted.

        Currently retries all exceptions up to max_retries.
        Subclasses can override to filter by exception type.

        Args:
            attempt: Current attempt number (0-based)
            error: The exception that occurred

        Returns:
            True if retry should be attempted
        """
        return attempt < self._config.max_retries

    def __repr__(self) -> str:
        return (
            f"ExponentialBackoffRetryPolicy("
            f"max_retries={self._config.max_retries}, "
            f"initial_delay={self._config.initial_delay}, "
            f"exponential_base={self._config.exponential_base})"
        )


class NoRetryPolicy:
    """
    Retry policy that never retries.

    Useful for:
    - Testing failure handling without delays
    - Fail-fast scenarios
    - When retries are handled at a higher level

    Example:
        >>> policy = NoRetryPolicy()
        >>> policy.should_retry(0, ValueError("test"))
        False
    """

    @property
    def max_retries(self) -> int:
        """No retries - always 0."""
        return 0

    def get_backoff(self, attempt: int) -> float:
        """No backoff needed since we don't retry."""
        return 0.0

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """Never retry."""
        return False

    def __repr__(self) -> str:
        return "NoRetryPolicy()"


class FilteredRetryPolicy:
    """
    Retry policy that filters which exceptions to retry.

    Wraps another policy and adds exception type filtering.
    Only retries if the exception is an instance of one of the
    allowed exception types.

    Example:
        >>> from eventsource.subscriptions.retry import TRANSIENT_EXCEPTIONS
        >>>
        >>> # Only retry transient failures
        >>> policy = FilteredRetryPolicy(
        ...     base_policy=ExponentialBackoffRetryPolicy(),
        ...     retryable_exceptions=TRANSIENT_EXCEPTIONS,
        ... )
        >>> policy.should_retry(0, ConnectionError("timeout"))
        True
        >>> policy.should_retry(0, ValueError("invalid"))
        False
    """

    def __init__(
        self,
        base_policy: RetryPolicy,
        retryable_exceptions: tuple[type[Exception], ...],
    ) -> None:
        """
        Initialize the filtered retry policy.

        Args:
            base_policy: The underlying retry policy to delegate to
            retryable_exceptions: Tuple of exception types to retry
        """
        self._base_policy = base_policy
        self._retryable_exceptions = retryable_exceptions

    @property
    def max_retries(self) -> int:
        """Delegate to base policy."""
        return self._base_policy.max_retries

    def get_backoff(self, attempt: int) -> float:
        """Delegate to base policy."""
        return self._base_policy.get_backoff(attempt)

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """
        Check if exception is retryable and base policy allows retry.

        Args:
            attempt: Current attempt number (0-based)
            error: The exception that occurred

        Returns:
            True only if exception is retryable and within retry limit
        """
        if not isinstance(error, self._retryable_exceptions):
            return False
        return self._base_policy.should_retry(attempt, error)

    def __repr__(self) -> str:
        exception_names = [e.__name__ for e in self._retryable_exceptions]
        return f"FilteredRetryPolicy(base={self._base_policy!r}, exceptions={exception_names})"


# Default policy that matches original CheckpointTrackingProjection behavior
DEFAULT_RETRY_POLICY = ExponentialBackoffRetryPolicy()


__all__ = [
    "RetryPolicy",
    "ExponentialBackoffRetryPolicy",
    "NoRetryPolicy",
    "FilteredRetryPolicy",
    "DEFAULT_RETRY_POLICY",
]
