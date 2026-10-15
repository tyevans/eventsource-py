"""Retry policy abstraction for projection event processing.

Provides configurable retry behavior for projections, reusing RetryConfig
from subscriptions.retry for consistency across the codebase.
"""

import logging
from typing import Protocol, runtime_checkable

from eventsource.application.subscriptions.retry import RetryConfig, calculate_backoff

logger = logging.getLogger(__name__)


@runtime_checkable
class ProjectionRetryPolicy(Protocol):
    """Protocol for retry policies in projections.

    Defines the interface for determining retry behavior when event processing fails.
    """

    @property
    def max_retries(self) -> int:
        """Maximum number of retry attempts (excluding initial attempt)."""
        ...

    def get_backoff(self, attempt: int) -> float:
        """Calculate delay in seconds before the next retry attempt (0-based)."""
        ...

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """Determine if another retry should be attempted (0-based attempt)."""
        ...


class ExponentialBackoffRetryPolicy:
    """Retry policy with exponential backoff using RetryConfig and calculate_backoff.

    Default settings match CheckpointTrackingProjection:
    max_retries=3, initial_delay=2.0s, exponential_base=2.0, jitter=0.0.
    """

    def __init__(self, config: RetryConfig | None = None) -> None:
        """Initialize the retry policy with optional RetryConfig."""
        self._config = config or RetryConfig(
            max_retries=3,
            initial_delay=2.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=0.0,
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
        """Calculate backoff delay in seconds for the given 0-based attempt."""
        return calculate_backoff(attempt, self._config)

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """Determine if retry should be attempted (retries all exceptions up to max_retries)."""
        return attempt < self._config.max_retries

    def __repr__(self) -> str:
        return (
            f"ExponentialBackoffRetryPolicy("
            f"max_retries={self._config.max_retries}, "
            f"initial_delay={self._config.initial_delay}, "
            f"exponential_base={self._config.exponential_base})"
        )


class NoRetryPolicy:
    """Retry policy that never retries (useful for fail-fast or testing)."""

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
    """Retry policy that wraps a base policy and filters which exception types to retry."""

    def __init__(
        self,
        base_policy: ProjectionRetryPolicy,
        retryable_exceptions: tuple[type[Exception], ...],
    ) -> None:
        """Initialize with underlying base_policy and tuple of retryable_exceptions."""
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
        """Check if exception is retryable and base policy allows retry."""
        if not isinstance(error, self._retryable_exceptions):
            return False
        return self._base_policy.should_retry(attempt, error)

    def __repr__(self) -> str:
        exception_names = [e.__name__ for e in self._retryable_exceptions]
        return f"FilteredRetryPolicy(base={self._base_policy!r}, exceptions={exception_names})"


DEFAULT_RETRY_POLICY = ExponentialBackoffRetryPolicy()


__all__ = [
    "ProjectionRetryPolicy",
    "ExponentialBackoffRetryPolicy",
    "NoRetryPolicy",
    "FilteredRetryPolicy",
    "DEFAULT_RETRY_POLICY",
]
