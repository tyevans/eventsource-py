"""Tests for error classification."""

import pytest

from eventsource.application.subscriptions.error_handling import (
    ErrorCategory,
    ErrorClassification,
    ErrorClassifier,
    ErrorSeverity,
    get_default_classifier,
)


class TestErrorClassification:
    """Tests for ErrorClassification dataclass."""

    def test_create_classification(self):
        classification = ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            description="Test classification",
        )
        assert classification.category == ErrorCategory.TRANSIENT
        assert classification.severity == ErrorSeverity.MEDIUM
        assert classification.retryable is True
        assert classification.description == "Test classification"

    def test_classification_is_frozen(self):
        classification = ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
        )
        with pytest.raises(AttributeError):
            classification.category = ErrorCategory.PERMANENT  # type: ignore


class TestErrorCategory:
    """Tests for ErrorCategory enum."""

    def test_category_values(self):
        assert ErrorCategory.TRANSIENT.value == "transient"
        assert ErrorCategory.PERMANENT.value == "permanent"
        assert ErrorCategory.INFRASTRUCTURE.value == "infrastructure"
        assert ErrorCategory.APPLICATION.value == "application"
        assert ErrorCategory.UNKNOWN.value == "unknown"


class TestErrorSeverity:
    """Tests for ErrorSeverity enum."""

    def test_severity_values(self):
        assert ErrorSeverity.LOW.value == "low"
        assert ErrorSeverity.MEDIUM.value == "medium"
        assert ErrorSeverity.HIGH.value == "high"
        assert ErrorSeverity.CRITICAL.value == "critical"


class TestErrorClassifier:
    """Tests for ErrorClassifier."""

    @pytest.fixture
    def classifier(self) -> ErrorClassifier:
        return ErrorClassifier()

    def test_classify_connection_error(self, classifier: ErrorClassifier):
        classification = classifier.classify(ConnectionError("Connection refused"))
        assert classification.category == ErrorCategory.TRANSIENT
        assert classification.retryable is True

    def test_classify_timeout_error(self, classifier: ErrorClassifier):
        classification = classifier.classify(TimeoutError("Operation timed out"))
        assert classification.category == ErrorCategory.TRANSIENT
        assert classification.retryable is True

    def test_classify_value_error(self, classifier: ErrorClassifier):
        classification = classifier.classify(ValueError("Invalid value"))
        assert classification.category == ErrorCategory.APPLICATION
        assert classification.retryable is False

    def test_classify_type_error(self, classifier: ErrorClassifier):
        classification = classifier.classify(TypeError("Wrong type"))
        assert classification.category == ErrorCategory.APPLICATION
        assert classification.severity == ErrorSeverity.HIGH
        assert classification.retryable is False

    def test_classify_memory_error(self, classifier: ErrorClassifier):
        classification = classifier.classify(MemoryError("Out of memory"))
        assert classification.category == ErrorCategory.INFRASTRUCTURE
        assert classification.severity == ErrorSeverity.CRITICAL
        assert classification.retryable is False

    def test_classify_unknown_error(self, classifier: ErrorClassifier):
        class CustomError(Exception):
            pass

        classification = classifier.classify(CustomError("Unknown"))
        assert classification.category == ErrorCategory.UNKNOWN
        assert classification.retryable is False

    def test_register_custom_classification(self, classifier: ErrorClassifier):
        class CustomError(Exception):
            pass

        custom_classification = ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.LOW,
            retryable=True,
            description="Custom error",
        )
        classifier.register_classification(CustomError, custom_classification)
        assert classifier.classify(CustomError("test")) == custom_classification

    def test_register_pattern_rule(self, classifier: ErrorClassifier):
        custom_classification = ErrorClassification(
            category=ErrorCategory.INFRASTRUCTURE,
            severity=ErrorSeverity.HIGH,
            retryable=True,
            description="Database error",
        )
        classifier.register_pattern_rule("database", custom_classification)

        class CustomError(Exception):
            pass

        assert (
            classifier.classify(CustomError("Database connection failed")) == custom_classification
        )

    def test_is_retryable(self, classifier: ErrorClassifier):
        assert classifier.is_retryable(ConnectionError("test")) is True
        assert classifier.is_retryable(ValueError("test")) is False

    def test_is_transient(self, classifier: ErrorClassifier):
        assert classifier.is_transient(TimeoutError("test")) is True
        assert classifier.is_transient(ValueError("test")) is False

    def test_classification_priority(self, classifier: ErrorClassifier):
        custom_classification = ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.LOW,
            retryable=True,
            description="Custom ValueError",
        )
        classifier.register_classification(ValueError, custom_classification)
        classification = classifier.classify(ValueError("test"))
        assert classification == custom_classification
        assert classification.retryable is True


class TestGetDefaultClassifier:
    """Tests for get_default_classifier."""

    def test_returns_classifier(self):
        assert isinstance(get_default_classifier(), ErrorClassifier)

    def test_returns_same_instance(self):
        assert get_default_classifier() is get_default_classifier()
