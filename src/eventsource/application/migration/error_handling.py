"""
Runtime error handling for migration operations.

`ErrorHandler` composes the other three modules — it classifies a failure,
consults its retry configuration, and optionally guards the call with a
circuit breaker. `classify_exception` is the bridge from an arbitrary
caught exception to an `ErrorClassification`, and lives here rather than
with the taxonomy because it depends on both the taxonomy and the
vocabulary (ADR 0044).
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar
from uuid import UUID

from eventsource.application.migration.circuit_breaker import CircuitBreaker
from eventsource.application.migration.error_classification import (
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.application.migration.exceptions import MigrationError

logger = logging.getLogger(__name__)

# Type variable for generic async functions
T = TypeVar("T")

__all__ = [
    "ErrorHandler",
    "classify_exception",
]


class ErrorHandler:
    """
    Error handler with automatic retry and circuit breaker support.

    Provides a unified interface for handling migration errors with:
    - Automatic retry for transient errors with exponential backoff
    - Circuit breaker to prevent cascading failures
    - Structured logging with error classification
    - Alert hooks for critical/error severity

    Usage:
        >>> handler = ErrorHandler()
        >>>
        >>> # Execute with automatic retry
        >>> result = await handler.execute_with_retry(
        ...     async_operation,
        ...     operation_name="bulk_copy",
        ...     migration_id=migration_id,
        ... )
        >>>
        >>> # Use as decorator
        >>> @handler.with_retry(operation_name="sync_events")
        ... async def sync_events():
        ...     ...

    Attributes:
        circuit_breaker: Optional circuit breaker for the handler.
        alert_callback: Callback for alerting on errors.
        metrics_callback: Callback for recording error metrics.
    """

    def __init__(
        self,
        circuit_breaker: CircuitBreaker | None = None,
        alert_callback: Callable[[MigrationError], None] | None = None,
        metrics_callback: Callable[[MigrationError, bool], None] | None = None,
    ) -> None:
        """
        Initialize the error handler.

        Args:
            circuit_breaker: Optional circuit breaker for protecting operations.
            alert_callback: Callback invoked when errors with alerting severity occur.
            metrics_callback: Callback for recording error metrics (error, retried).
        """
        self.circuit_breaker = circuit_breaker
        self.alert_callback = alert_callback
        self.metrics_callback = metrics_callback

    async def execute_with_retry(
        self,
        operation: Callable[[], Coroutine[Any, Any, T]],
        operation_name: str,
        *,
        migration_id: UUID | None = None,
        retry_config: RetryConfig | None = None,
        on_retry: Callable[[int, Exception, float], None] | None = None,
    ) -> T:
        """
        Execute an operation with automatic retry for transient errors.

        Args:
            operation: Async callable to execute.
            operation_name: Name for logging and metrics.
            migration_id: Optional migration ID for error context.
            retry_config: Override retry configuration.
            on_retry: Callback invoked on each retry (attempt, exception, delay_ms).

        Returns:
            The result of the operation.

        Raises:
            MigrationError: If all retries are exhausted or error is non-transient.
            CircuitBreakerOpenError: If circuit breaker is open.
        """
        # Check circuit breaker if configured
        if self.circuit_breaker:
            ctx = await self.circuit_breaker.protect(operation_name, migration_id)
        else:
            ctx = None

        last_exception: Exception | None = None
        attempt = 0
        effective_config = retry_config or TRANSIENT_RETRY_CONFIG

        while attempt < effective_config.max_attempts:
            try:
                if ctx:
                    async with ctx:
                        result = await operation()
                else:
                    result = await operation()

                # Success - log if we had to retry
                if attempt > 0:
                    logger.info(
                        "Operation '%s' succeeded after %d retries",
                        operation_name,
                        attempt,
                    )

                return result

            except MigrationError as e:
                last_exception = e
                self._handle_error(e, operation_name)

                # Check if we should retry
                if not e.recoverability_type.should_retry:
                    logger.error(
                        "Non-retryable error in '%s': %s (code=%s)",
                        operation_name,
                        e.message,
                        e.error_code,
                    )
                    raise

                # Use error's retry config if available and no override
                config = retry_config or e.retry_config or TRANSIENT_RETRY_CONFIG

                # Check if we have retries left
                if attempt + 1 >= config.max_attempts:
                    logger.error(
                        "Exhausted %d retries for '%s': %s",
                        config.max_attempts,
                        operation_name,
                        e.message,
                    )
                    raise

                # Calculate delay and sleep
                delay_ms = config.get_delay_ms(attempt)
                delay_s = delay_ms / 1000.0

                logger.warning(
                    "Retryable error in '%s' (attempt %d/%d): %s. Retrying in %.1fs",
                    operation_name,
                    attempt + 1,
                    config.max_attempts,
                    e.message,
                    delay_s,
                )

                if on_retry:
                    on_retry(attempt, e, delay_ms)

                await asyncio.sleep(delay_s)
                attempt += 1

            except Exception as e:
                # Non-MigrationError exceptions are not automatically retried
                logger.exception(
                    "Unexpected error in '%s': %s",
                    operation_name,
                    str(e),
                )
                raise

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError(f"Unexpected state in retry loop for {operation_name}")

    def _handle_error(self, error: MigrationError, operation_name: str) -> None:
        """Handle an error by logging and potentially alerting."""
        classification = error.classification

        # Log at appropriate level
        logger.log(
            classification.severity.log_level,
            "Error in '%s': %s [code=%s, severity=%s, recoverable=%s]",
            operation_name,
            error.message,
            classification.error_code,
            classification.severity.value,
            classification.recoverability.value,
        )

        # Invoke alert callback if severity warrants it
        if classification.severity.should_alert and self.alert_callback:
            try:
                self.alert_callback(error)
            except Exception:
                logger.exception("Alert callback failed")

        # Record metrics
        if self.metrics_callback:
            try:
                self.metrics_callback(error, classification.recoverability.should_retry)
            except Exception:
                logger.exception("Metrics callback failed")

    def with_retry(
        self,
        operation_name: str,
        *,
        retry_config: RetryConfig | None = None,
    ) -> Callable[
        [Callable[..., Coroutine[Any, Any, T]]],
        Callable[..., Coroutine[Any, Any, T]],
    ]:
        """
        Decorator for automatic retry on transient errors.

        Args:
            operation_name: Name for logging and metrics.
            retry_config: Override retry configuration.

        Returns:
            Decorator function.

        Example:
            >>> handler = ErrorHandler()
            >>>
            >>> @handler.with_retry(operation_name="process_batch")
            ... async def process_batch(batch_id: str):
            ...     ...
        """

        def decorator(
            func: Callable[..., Coroutine[Any, Any, T]],
        ) -> Callable[..., Coroutine[Any, Any, T]]:
            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> T:
                return await self.execute_with_retry(
                    lambda: func(*args, **kwargs),
                    operation_name=operation_name,
                    retry_config=retry_config,
                )

            return wrapper

        return decorator


def classify_exception(exc: Exception) -> ErrorClassification:
    """
    Classify any exception and return its error classification.

    For MigrationError subclasses, returns their specific classification.
    For other exceptions, returns a generic classification.

    Args:
        exc: The exception to classify.

    Returns:
        ErrorClassification for the exception.

    Example:
        >>> try:
        ...     await risky_operation()
        ... except Exception as e:
        ...     classification = classify_exception(e)
        ...     if classification.severity.should_alert:
        ...         send_alert(e, classification)
    """
    if isinstance(exc, MigrationError):
        return exc.classification

    # Generic classification for non-migration errors
    return ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="UNKNOWN_ERROR",
        category="unknown",
        suggested_action="An unexpected error occurred. Review logs and contact support.",
    )
