"""
Registry for error callbacks and handlers.
"""

from __future__ import annotations

import logging

from eventsource.application.subscriptions.error_handling_types import (
    ErrorCallback,
    ErrorCategory,
    ErrorInfo,
    ErrorSeverity,
    SyncErrorCallback,
)

logger = logging.getLogger(__name__)


class ErrorHandlerRegistry:
    """
    Registry for error callbacks and handlers.

    Allows registering multiple callbacks that will be invoked when
    errors occur. Supports filtering by error category and severity.

    Example:
        >>> registry = ErrorHandlerRegistry()
        >>> async def alert_on_critical(error: ErrorInfo):
        ...     if error.classification.severity == ErrorSeverity.CRITICAL:
        ...         await send_alert(error)
        >>> registry.register(alert_on_critical)
    """

    def __init__(self) -> None:
        """Initialize the error handler registry."""
        self._callbacks: list[ErrorCallback] = []
        self._sync_callbacks: list[SyncErrorCallback] = []
        self._category_callbacks: dict[ErrorCategory, list[ErrorCallback]] = {}
        self._severity_callbacks: dict[ErrorSeverity, list[ErrorCallback]] = {}

    def register(self, callback: ErrorCallback) -> None:
        """
        Register a callback for all errors.

        Args:
            callback: Async function to call on error
        """
        self._callbacks.append(callback)

    def register_sync(self, callback: SyncErrorCallback) -> None:
        """
        Register a synchronous callback for all errors.

        Args:
            callback: Sync function to call on error
        """
        self._sync_callbacks.append(callback)

    def register_for_category(
        self,
        category: ErrorCategory,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific category.

        Args:
            category: The error category to filter on
            callback: Async function to call for matching errors
        """
        if category not in self._category_callbacks:
            self._category_callbacks[category] = []
        self._category_callbacks[category].append(callback)

    def register_for_severity(
        self,
        severity: ErrorSeverity,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific severity.

        Args:
            severity: The error severity to filter on
            callback: Async function to call for matching errors
        """
        if severity not in self._severity_callbacks:
            self._severity_callbacks[severity] = []
        self._severity_callbacks[severity].append(callback)

    async def notify(self, error_info: ErrorInfo) -> None:
        """
        Notify all registered callbacks about an error.

        Invokes callbacks in order:
        1. General callbacks
        2. Category-specific callbacks
        3. Severity-specific callbacks

        Errors in callbacks are logged but don't prevent other callbacks.

        Args:
            error_info: The error information to broadcast
        """
        # Invoke sync callbacks first
        for sync_cb in self._sync_callbacks:
            try:
                sync_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in sync error callback: {e}",
                    extra={
                        "callback": sync_cb.__name__,
                        "error_info": error_info.to_dict(),
                    },
                )

        # Invoke general async callbacks
        for async_cb in self._callbacks:
            try:
                await async_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in error callback: {e}",
                    extra={
                        "callback": async_cb.__name__,
                        "error_info": error_info.to_dict(),
                    },
                )

        # Invoke category-specific callbacks
        category = error_info.classification.category
        if category in self._category_callbacks:
            for cat_cb in self._category_callbacks[category]:
                try:
                    await cat_cb(error_info)
                except Exception as e:
                    logger.error(
                        f"Error in category callback: {e}",
                        extra={
                            "callback": cat_cb.__name__,
                            "category": category.value,
                        },
                    )

        # Invoke severity-specific callbacks
        severity = error_info.classification.severity
        if severity in self._severity_callbacks:
            for sev_cb in self._severity_callbacks[severity]:
                try:
                    await sev_cb(error_info)
                except Exception as e:
                    logger.error(
                        f"Error in severity callback: {e}",
                        extra={
                            "callback": sev_cb.__name__,
                            "severity": severity.value,
                        },
                    )

    def clear(self) -> None:
        """Remove all registered callbacks."""
        self._callbacks.clear()
        self._sync_callbacks.clear()
        self._category_callbacks.clear()
        self._severity_callbacks.clear()


__all__ = ["ErrorHandlerRegistry"]
