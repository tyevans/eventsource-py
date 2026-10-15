"""Registry for error callbacks and handlers."""

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
    """Registry for error callbacks supporting category and severity filtering."""

    def __init__(self) -> None:
        """Initialize the error handler registry."""
        self._callbacks: list[ErrorCallback] = []
        self._sync_callbacks: list[SyncErrorCallback] = []
        self._category_callbacks: dict[ErrorCategory, list[ErrorCallback]] = {}
        self._severity_callbacks: dict[ErrorSeverity, list[ErrorCallback]] = {}

    def register(self, callback: ErrorCallback) -> None:
        """Register an async callback for all errors."""
        self._callbacks.append(callback)

    def register_sync(self, callback: SyncErrorCallback) -> None:
        """Register a synchronous callback for all errors."""
        self._sync_callbacks.append(callback)

    def register_for_category(self, category: ErrorCategory, callback: ErrorCallback) -> None:
        """Register an async callback for errors of a specific category."""
        self._category_callbacks.setdefault(category, []).append(callback)

    def register_for_severity(self, severity: ErrorSeverity, callback: ErrorCallback) -> None:
        """Register an async callback for errors of a specific severity."""
        self._severity_callbacks.setdefault(severity, []).append(callback)

    async def notify(self, error_info: ErrorInfo) -> None:
        """Notify all registered callbacks (sync, general, category, severity) about an error."""
        for sync_cb in self._sync_callbacks:
            try:
                sync_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in sync error callback: {e}",
                    extra={"callback": sync_cb.__name__, "error_info": error_info.to_dict()},
                )

        for async_cb in self._callbacks:
            try:
                await async_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in error callback: {e}",
                    extra={"callback": async_cb.__name__, "error_info": error_info.to_dict()},
                )

        category = error_info.classification.category
        for cat_cb in self._category_callbacks.get(category, ()):
            try:
                await cat_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in category callback: {e}",
                    extra={"callback": cat_cb.__name__, "category": category.value},
                )

        severity = error_info.classification.severity
        for sev_cb in self._severity_callbacks.get(severity, ()):
            try:
                await sev_cb(error_info)
            except Exception as e:
                logger.error(
                    f"Error in severity callback: {e}",
                    extra={"callback": sev_cb.__name__, "severity": severity.value},
                )

    def clear(self) -> None:
        """Remove all registered callbacks."""
        self._callbacks.clear()
        self._sync_callbacks.clear()
        self._category_callbacks.clear()
        self._severity_callbacks.clear()


__all__ = ["ErrorHandlerRegistry"]
