"""
Error classifier for subscription errors.
"""

from __future__ import annotations

import asyncio

from eventsource.application.subscriptions.error_handling_types import (
    ErrorCategory,
    ErrorClassification,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    ErrorSeverity,
    ErrorStats,
)


class ErrorClassifier:
    """
    Classifies errors to determine handling strategy.

    The classifier examines exceptions and categorizes them based on:
    - Exception type
    - Exception message patterns
    - Custom classification rules

    This enables consistent error handling across all subscription components.

    Example:
        >>> classifier = ErrorClassifier()
        >>> classification = classifier.classify(ConnectionError("timeout"))
        >>> classification.category
        ErrorCategory.TRANSIENT
        >>> classification.retryable
        True
    """

    # Default classification rules for common exception types
    _DEFAULT_CLASSIFICATIONS: dict[type[Exception], ErrorClassification] = {
        # Transient network/connection errors
        ConnectionError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Connection error - likely temporary",
        ),
        TimeoutError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Timeout error - operation took too long",
        ),
        asyncio.TimeoutError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Async timeout error",
        ),
        OSError: ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="OS-level error - may include network issues",
        ),
        # Permanent application errors
        ValueError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.MEDIUM,
            retryable=False,
            description="Invalid value - data/validation issue",
        ),
        TypeError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.HIGH,
            retryable=False,
            description="Type error - likely code bug",
        ),
        AttributeError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.HIGH,
            retryable=False,
            description="Attribute error - likely code bug",
        ),
        KeyError: ErrorClassification(
            category=ErrorCategory.APPLICATION,
            severity=ErrorSeverity.MEDIUM,
            retryable=False,
            description="Key error - missing expected data",
        ),
        # Critical errors
        MemoryError: ErrorClassification(
            category=ErrorCategory.INFRASTRUCTURE,
            severity=ErrorSeverity.CRITICAL,
            retryable=False,
            description="Memory exhausted",
        ),
        SystemError: ErrorClassification(
            category=ErrorCategory.INFRASTRUCTURE,
            severity=ErrorSeverity.CRITICAL,
            retryable=False,
            description="System error",
        ),
    }

    def __init__(self) -> None:
        """Initialize the error classifier."""
        self._custom_rules: dict[type[Exception], ErrorClassification] = {}
        self._pattern_rules: list[tuple[str, ErrorClassification]] = []

    def register_classification(
        self,
        exception_type: type[Exception],
        classification: ErrorClassification,
    ) -> None:
        """
        Register a custom classification for an exception type.

        Args:
            exception_type: The exception class to classify
            classification: The classification to apply
        """
        self._custom_rules[exception_type] = classification

    def register_pattern_rule(
        self,
        pattern: str,
        classification: ErrorClassification,
    ) -> None:
        """
        Register a classification rule based on error message pattern.

        Args:
            pattern: Substring to match in error message (case-insensitive)
            classification: The classification to apply if pattern matches
        """
        self._pattern_rules.append((pattern.lower(), classification))

    def classify(self, error: Exception) -> ErrorClassification:
        """
        Classify an error based on type and message.

        Classification priority:
        1. Custom rules (registered via register_classification)
        2. Pattern rules (registered via register_pattern_rule)
        3. Default classifications
        4. Unknown classification (fallback)

        Args:
            error: The exception to classify

        Returns:
            ErrorClassification with category, severity, and retry info
        """
        error_type = type(error)
        error_message = str(error).lower()

        # 1. Check custom rules first
        if error_type in self._custom_rules:
            return self._custom_rules[error_type]

        # 2. Check for pattern matches in error message
        for pattern, classification in self._pattern_rules:
            if pattern in error_message:
                return classification

        # 3. Check default classifications (including parent classes)
        for exc_type in error_type.__mro__:
            if exc_type in self._DEFAULT_CLASSIFICATIONS:
                return self._DEFAULT_CLASSIFICATIONS[exc_type]

        # 4. Fallback to unknown
        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            severity=ErrorSeverity.MEDIUM,
            retryable=False,
            description=f"Unclassified error: {error_type.__name__}",
        )

    def is_retryable(self, error: Exception) -> bool:
        """
        Check if an error should be retried.

        Args:
            error: The exception to check

        Returns:
            True if the error is retryable
        """
        return self.classify(error).retryable

    def is_transient(self, error: Exception) -> bool:
        """
        Check if an error is transient.

        Args:
            error: The exception to check

        Returns:
            True if the error is transient
        """
        return self.classify(error).category == ErrorCategory.TRANSIENT


# Default global classifier instance
_default_classifier = ErrorClassifier()


def get_default_classifier() -> ErrorClassifier:
    """Get the default error classifier instance."""
    return _default_classifier


__all__ = [
    "ErrorCategory",
    "ErrorClassification",
    "ErrorClassifier",
    "ErrorHandlingConfig",
    "ErrorHandlingStrategy",
    "ErrorInfo",
    "ErrorSeverity",
    "ErrorStats",
    "get_default_classifier",
]
