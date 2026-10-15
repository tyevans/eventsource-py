"""Runtime error handling for migration operations (ADR 0044)."""

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
T = TypeVar("T")

__all__ = ["ErrorHandler", "classify_exception"]


class ErrorHandler:
    """Error handler with automatic retry, circuit breaker, logging, and alert support."""

    def __init__(
        self,
        circuit_breaker: CircuitBreaker | None = None,
        alert_callback: Callable[[MigrationError], None] | None = None,
        metrics_callback: Callable[[MigrationError, bool], None] | None = None,
    ) -> None:
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
        """Execute an operation with automatic retry for transient errors."""
        cb = self.circuit_breaker
        ctx = await cb.protect(operation_name, migration_id) if cb else None
        last_exception: Exception | None = None
        attempt, effective_config = 0, retry_config or TRANSIENT_RETRY_CONFIG

        while attempt < effective_config.max_attempts:
            try:
                if ctx:
                    async with ctx:
                        result = await operation()
                else:
                    result = await operation()
                if attempt > 0:
                    logger.info("Operation '%s' succeeded after %d retries", operation_name, attempt)  # fmt: skip
                return result
            except MigrationError as e:
                last_exception = e
                self._handle_error(e, operation_name)
                if not e.recoverability_type.should_retry:
                    logger.error("Non-retryable error in '%s': %s (code=%s)", operation_name, e.message, e.error_code)  # fmt: skip
                    raise
                config = retry_config or e.retry_config or TRANSIENT_RETRY_CONFIG
                if attempt + 1 >= config.max_attempts:
                    logger.error("Exhausted %d retries for '%s': %s", config.max_attempts, operation_name, e.message)  # fmt: skip
                    raise
                delay_ms = config.get_delay_ms(attempt)
                delay_s = delay_ms / 1000.0
                logger.warning("Retryable error in '%s' (attempt %d/%d): %s. Retrying in %.1fs", operation_name, attempt + 1, config.max_attempts, e.message, delay_s)  # fmt: skip
                if on_retry:
                    on_retry(attempt, e, delay_ms)
                await asyncio.sleep(delay_s)
                attempt += 1
            except Exception as e:
                logger.exception("Unexpected error in '%s': %s", operation_name, str(e))
                raise

        if last_exception:
            raise last_exception
        raise RuntimeError(f"Unexpected state in retry loop for {operation_name}")

    def _handle_error(self, error: MigrationError, operation_name: str) -> None:
        """Handle an error by logging and potentially alerting."""
        c = error.classification
        logger.log(c.severity.log_level, "Error in '%s': %s [code=%s, severity=%s, recoverable=%s]", operation_name, error.message, c.error_code, c.severity.value, c.recoverability.value)  # fmt: skip
        if c.severity.should_alert and self.alert_callback:
            try:
                self.alert_callback(error)
            except Exception:
                logger.exception("Alert callback failed")
        if self.metrics_callback:
            try:
                self.metrics_callback(error, c.recoverability.should_retry)
            except Exception:
                logger.exception("Metrics callback failed")

    def with_retry(
        self, operation_name: str, *, retry_config: RetryConfig | None = None
    ) -> Callable[
        [Callable[..., Coroutine[Any, Any, T]]],
        Callable[..., Coroutine[Any, Any, T]],
    ]:
        """Decorator for automatic retry on transient errors."""

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
    """Classify any exception and return its ErrorClassification."""
    if isinstance(exc, MigrationError):
        return exc.classification
    return ErrorClassification(
        severity=ErrorSeverity.ERROR,
        recoverability=ErrorRecoverability.FATAL,
        error_code="UNKNOWN_ERROR",
        category="unknown",
        suggested_action="An unexpected error occurred. Review logs and contact support.",
    )
