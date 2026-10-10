"""
Error handling integration for subscription management (facade).

This module re-exports error classification, tracking, dead letter queue integration,
and unified SubscriptionErrorHandler.
"""

from __future__ import annotations

from eventsource.application.subscriptions.error_handling_classifier import (
    ErrorClassifier,
    get_default_classifier,
)
from eventsource.application.subscriptions.error_handling_handler import (
    ErrorHandlerRegistry,
    SubscriptionErrorHandler,
)
from eventsource.application.subscriptions.error_handling_types import (
    ErrorCallback,
    ErrorCategory,
    ErrorClassification,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    ErrorSeverity,
    ErrorStats,
    SyncErrorCallback,
)

__all__ = [
    # Classification
    "ErrorCategory",
    "ErrorSeverity",
    "ErrorClassification",
    "ErrorClassifier",
    "get_default_classifier",
    # Tracking
    "ErrorInfo",
    "ErrorStats",
    # Callbacks
    "ErrorCallback",
    "SyncErrorCallback",
    "ErrorHandlerRegistry",
    # Strategy
    "ErrorHandlingStrategy",
    "ErrorHandlingConfig",
    # Handler
    "SubscriptionErrorHandler",
]
