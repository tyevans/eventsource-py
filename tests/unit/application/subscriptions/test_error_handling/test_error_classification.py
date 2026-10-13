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
        """Test creating an error classification."""
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
        """Test that classification is immutable."""
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
        """Test all category values exist."""
        assert ErrorCategory.TRANSIENT.value == "transient"
        assert ErrorCategory.PERMANENT.value == "permanent"
        assert ErrorCategory.INFRASTRUCTURE.value == "infrastructure"
        assert ErrorCategory.APPLICATION.value == "application"
        assert ErrorCategory.UNKNOWN.value == "unknown"


class TestErrorSeverity:
    """Tests for ErrorSeverity enum."""

    def test_severity_values(self):
        """Test all severity values exist."""
        assert ErrorSeverity.LOW.value == "low"
        assert ErrorSeverity.MEDIUM.value == "medium"
        assert ErrorSeverity.HIGH.value == "high"
        assert ErrorSeverity.CRITICAL.value == "critical"


class TestErrorClassifier:
    """Tests for ErrorClassifier."""

    @pytest.fixture
    def classifier(self) -> ErrorClassifier:
        """Create a test classifier."""
        return ErrorClassifier()

    def test_classify_connection_error(self, classifier: ErrorClassifier):
        """Test classification of ConnectionError."""
        error = ConnectionError("Connection refused")
        classification = classifier.classify(error)

        assert classification.category == ErrorCategory.TRANSIENT
        assert classification.retryable is True

    def test_classify_timeout_error(self, classifier: ErrorClassifier):
        """Test classification of TimeoutError."""
        error = TimeoutError("Operation timed out")
        classification = classifier.classify(error)

        assert classification.category == ErrorCategory.TRANSIENT
        assert classification.retryable is True

    def test_classify_value_error(self, classifier: ErrorClassifier):
        """Test classification of ValueError."""
        error = ValueError("Invalid value")
        classification = classifier.classify(error)

        assert classification.category == ErrorCategory.APPLICATION
        assert classification.retryable is False

    def test_classify_type_error(self, classifier: ErrorClassifier):
        """Test classification of TypeError."""
        error = TypeError("Wrong type")
        classification = classifier.classify(error)

        assert classification.category == ErrorCategory.APPLICATION
        assert classification.severity == ErrorSeverity.HIGH
        assert classification.retryable is False

    def test_classify_memory_error(self, classifier: ErrorClassifier):
        """Test classification of MemoryError."""
        error = MemoryError("Out of memory")
        classification = classifier.classify(error)

        assert classification.category == ErrorCategory.INFRASTRUCTURE
        assert classification.severity == ErrorSeverity.CRITICAL
        assert classification.retryable is False

    def test_classify_unknown_error(self, classifier: ErrorClassifier):
        """Test classification of unknown error type."""

        class CustomError(Exception):
            pass

        error = CustomError("Unknown")
        classification = classifier.classify(error)

        assert classification.category == ErrorCategory.UNKNOWN
        assert classification.retryable is False

    def test_register_custom_classification(self, classifier: ErrorClassifier):
        """Test registering custom classification."""

        class CustomError(Exception):
            pass

        custom_classification = ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.LOW,
            retryable=True,
            description="Custom error",
        )

        classifier.register_classification(CustomError, custom_classification)

        error = CustomError("test")
        classification = classifier.classify(error)

        assert classification == custom_classification

    def test_register_pattern_rule(self, classifier: ErrorClassifier):
        """Test pattern-based classification."""
        custom_classification = ErrorClassification(
            category=ErrorCategory.INFRASTRUCTURE,
            severity=ErrorSeverity.HIGH,
            retryable=True,
            description="Database error",
        )

        classifier.register_pattern_rule("database", custom_classification)

        # Unknown error type but message matches pattern
        class CustomError(Exception):
            pass

        error = CustomError("Database connection failed")
        classification = classifier.classify(error)

        assert classification == custom_classification

    def test_is_retryable(self, classifier: ErrorClassifier):
        """Test is_retryable convenience method."""
        assert classifier.is_retryable(ConnectionError("test")) is True
        assert classifier.is_retryable(ValueError("test")) is False

    def test_is_transient(self, classifier: ErrorClassifier):
        """Test is_transient convenience method."""
        assert classifier.is_transient(TimeoutError("test")) is True
        assert classifier.is_transient(ValueError("test")) is False

    def test_classification_priority(self, classifier: ErrorClassifier):
        """Test that custom rules take priority."""

        # Register custom classification for ValueError
        custom_classification = ErrorClassification(
            category=ErrorCategory.TRANSIENT,
            severity=ErrorSeverity.LOW,
            retryable=True,
            description="Custom ValueError",
        )
        classifier.register_classification(ValueError, custom_classification)

        # Custom should override default
        error = ValueError("test")
        classification = classifier.classify(error)

        assert classification == custom_classification
        assert classification.retryable is True  # Unlike default


class TestGetDefaultClassifier:
    """Tests for get_default_classifier."""

    def test_returns_classifier(self):
        """Test that it returns a classifier instance."""
        classifier = get_default_classifier()
        assert isinstance(classifier, ErrorClassifier)

    def test_returns_same_instance(self):
        """Test that it returns the same instance."""
        classifier1 = get_default_classifier()
        classifier2 = get_default_classifier()
        assert classifier1 is classifier2
