"""Tests for subscription error handling."""

from tests.unit.application.subscriptions.test_error_handling.test_error_classification import (
    TestErrorCategory,
    TestErrorClassification,
    TestErrorClassifier,
    TestErrorSeverity,
    TestGetDefaultClassifier,
)
from tests.unit.application.subscriptions.test_error_handling.test_error_models import (
    TestErrorHandlerRegistry,
    TestErrorHandlingConfig,
    TestErrorHandlingStrategy,
    TestErrorInfo,
    TestErrorStats,
)
from tests.unit.application.subscriptions.test_error_handling.test_subscription_error_handler import (
    TestModuleImports,
    TestSubscriptionErrorHandler,
)

__all__ = [  # pragma: allowlist secret
    "TestErrorCategory",
    "TestErrorClassification",
    "TestErrorClassifier",
    "TestErrorHandlerRegistry",
    "TestErrorHandlingConfig",
    "TestErrorHandlingStrategy",
    "TestErrorInfo",
    "TestErrorSeverity",
    "TestErrorStats",
    "TestGetDefaultClassifier",
    "TestModuleImports",
    "TestSubscriptionErrorHandler",
]
