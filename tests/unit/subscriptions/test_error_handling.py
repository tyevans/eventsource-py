"""
Unit tests for error handling integration.

Tests cover:
- Error classification (transient vs permanent, retryable vs fatal)
- Error callbacks/hooks for monitoring
- Error tracking and statistics
- Dead letter queue integration
- Error handler registry
- Subscription error handler
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.subscriptions.error_handling import (
    ErrorCategory,
    ErrorClassification,
    ErrorClassifier,
    ErrorHandlerRegistry,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    ErrorSeverity,
    ErrorStats,
    SubscriptionErrorHandler,
    get_default_classifier,
)

# =============================================================================
# Error Classification Tests
# =============================================================================


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


# =============================================================================
# Error Info and Stats Tests
# =============================================================================


class TestErrorInfo:
    """Tests for ErrorInfo dataclass."""

    @pytest.fixture
    def error_info(self) -> ErrorInfo:
        """Create test error info."""
        return ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=100,
            error_type="ValueError",
            error_message="Test error message",
            error_stacktrace="Traceback...",
            classification=ErrorClassification(
                category=ErrorCategory.APPLICATION,
                severity=ErrorSeverity.MEDIUM,
                retryable=False,
            ),
            subscription_name="TestSubscription",
            retry_count=2,
        )

    def test_error_info_fields(self, error_info: ErrorInfo):
        """Test error info fields are accessible."""
        assert error_info.event_type == "TestEvent"
        assert error_info.position == 100
        assert error_info.error_type == "ValueError"
        assert error_info.subscription_name == "TestSubscription"
        assert error_info.retry_count == 2
        assert error_info.sent_to_dlq is False

    def test_error_info_to_dict(self, error_info: ErrorInfo):
        """Test conversion to dictionary."""
        data = error_info.to_dict()

        assert data["event_type"] == "TestEvent"
        assert data["position"] == 100
        assert data["error_type"] == "ValueError"
        assert data["category"] == "application"
        assert data["severity"] == "medium"
        assert data["retryable"] is False
        assert "timestamp" in data


class TestErrorStats:
    """Tests for ErrorStats."""

    @pytest.fixture
    def stats(self) -> ErrorStats:
        """Create test error stats."""
        return ErrorStats()

    def test_initial_state(self, stats: ErrorStats):
        """Test initial statistics values."""
        assert stats.total_errors == 0
        assert stats.transient_errors == 0
        assert stats.permanent_errors == 0
        assert stats.dlq_count == 0
        assert stats.first_error_at is None
        assert stats.last_error_at is None

    def test_record_transient_error(self, stats: ErrorStats):
        """Test recording a transient error."""
        error_info = ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=1,
            error_type="ConnectionError",
            error_message="Connection refused",
            error_stacktrace="...",
            classification=ErrorClassification(
                category=ErrorCategory.TRANSIENT,
                severity=ErrorSeverity.MEDIUM,
                retryable=True,
            ),
        )

        stats.record_error(error_info)

        assert stats.total_errors == 1
        assert stats.transient_errors == 1
        assert stats.permanent_errors == 0
        assert stats.errors_by_category["transient"] == 1
        assert stats.errors_by_type["ConnectionError"] == 1
        assert stats.first_error_at is not None
        assert stats.last_error_at is not None

    def test_record_permanent_error(self, stats: ErrorStats):
        """Test recording a permanent error."""
        error_info = ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=1,
            error_type="ValueError",
            error_message="Invalid data",
            error_stacktrace="...",
            classification=ErrorClassification(
                category=ErrorCategory.APPLICATION,
                severity=ErrorSeverity.MEDIUM,
                retryable=False,
            ),
        )

        stats.record_error(error_info)

        assert stats.total_errors == 1
        assert stats.transient_errors == 0
        assert stats.permanent_errors == 1
        assert stats.errors_by_category["application"] == 1

    def test_record_dlq_error(self, stats: ErrorStats):
        """Test recording an error sent to DLQ."""
        error_info = ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=1,
            error_type="ValueError",
            error_message="Invalid data",
            error_stacktrace="...",
            classification=ErrorClassification(
                category=ErrorCategory.APPLICATION,
                severity=ErrorSeverity.MEDIUM,
                retryable=False,
            ),
            sent_to_dlq=True,
        )

        stats.record_error(error_info)

        assert stats.dlq_count == 1

    def test_to_dict(self, stats: ErrorStats):
        """Test conversion to dictionary."""
        data = stats.to_dict()

        assert "total_errors" in data
        assert "transient_errors" in data
        assert "permanent_errors" in data
        assert "dlq_count" in data
        assert "errors_by_type" in data
        assert "errors_by_category" in data


# =============================================================================
# Error Handler Registry Tests
# =============================================================================


class TestErrorHandlerRegistry:
    """Tests for ErrorHandlerRegistry."""

    @pytest.fixture
    def registry(self) -> ErrorHandlerRegistry:
        """Create test registry."""
        return ErrorHandlerRegistry()

    @pytest.fixture
    def error_info(self) -> ErrorInfo:
        """Create test error info."""
        return ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=1,
            error_type="ValueError",
            error_message="Test error",
            error_stacktrace="...",
            classification=ErrorClassification(
                category=ErrorCategory.APPLICATION,
                severity=ErrorSeverity.HIGH,
                retryable=False,
            ),
        )

    @pytest.mark.asyncio
    async def test_register_and_notify_callback(
        self,
        registry: ErrorHandlerRegistry,
        error_info: ErrorInfo,
    ):
        """Test registering and calling a callback."""
        callback = AsyncMock()
        registry.register(callback)

        await registry.notify(error_info)

        callback.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_register_sync_callback(
        self,
        registry: ErrorHandlerRegistry,
        error_info: ErrorInfo,
    ):
        """Test registering and calling a sync callback."""
        callback = MagicMock()
        registry.register_sync(callback)

        await registry.notify(error_info)

        callback.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_category_callback(
        self,
        registry: ErrorHandlerRegistry,
        error_info: ErrorInfo,
    ):
        """Test category-specific callback."""
        callback = AsyncMock()
        registry.register_for_category(ErrorCategory.APPLICATION, callback)

        await registry.notify(error_info)

        callback.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_category_callback_not_called_for_different_category(
        self,
        registry: ErrorHandlerRegistry,
    ):
        """Test category callback not called for different category."""
        callback = AsyncMock()
        registry.register_for_category(ErrorCategory.TRANSIENT, callback)

        error_info = ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=1,
            error_type="ValueError",
            error_message="Test error",
            error_stacktrace="...",
            classification=ErrorClassification(
                category=ErrorCategory.APPLICATION,  # Different category
                severity=ErrorSeverity.MEDIUM,
                retryable=False,
            ),
        )

        await registry.notify(error_info)

        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_severity_callback(
        self,
        registry: ErrorHandlerRegistry,
        error_info: ErrorInfo,
    ):
        """Test severity-specific callback."""
        callback = AsyncMock()
        registry.register_for_severity(ErrorSeverity.HIGH, callback)

        await registry.notify(error_info)

        callback.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_multiple_callbacks(
        self,
        registry: ErrorHandlerRegistry,
        error_info: ErrorInfo,
    ):
        """Test multiple callbacks are called."""
        callback1 = AsyncMock()
        callback2 = AsyncMock()
        callback3 = AsyncMock()

        registry.register(callback1)
        registry.register(callback2)
        registry.register(callback3)

        await registry.notify(error_info)

        callback1.assert_called_once()
        callback2.assert_called_once()
        callback3.assert_called_once()

    @pytest.mark.asyncio
    async def test_callback_error_does_not_stop_others(
        self,
        registry: ErrorHandlerRegistry,
        error_info: ErrorInfo,
    ):
        """Test that error in one callback doesn't stop others."""
        callback1 = AsyncMock(side_effect=RuntimeError("Callback error"))
        callback2 = AsyncMock()

        registry.register(callback1)
        registry.register(callback2)

        # Should not raise
        await registry.notify(error_info)

        # Second callback should still be called
        callback2.assert_called_once()

    def test_clear(self, registry: ErrorHandlerRegistry):
        """Test clearing all callbacks."""
        callback = AsyncMock()
        registry.register(callback)
        registry.register_for_category(ErrorCategory.APPLICATION, callback)
        registry.register_for_severity(ErrorSeverity.HIGH, callback)

        registry.clear()

        assert len(registry._callbacks) == 0
        assert len(registry._category_callbacks) == 0
        assert len(registry._severity_callbacks) == 0


# =============================================================================
# Error Handling Config Tests
# =============================================================================


class TestErrorHandlingConfig:
    """Tests for ErrorHandlingConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ErrorHandlingConfig()

        assert config.strategy == ErrorHandlingStrategy.RETRY_THEN_CONTINUE
        assert config.max_recent_errors == 100
        assert config.max_errors_before_stop is None
        assert config.dlq_enabled is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = ErrorHandlingConfig(
            strategy=ErrorHandlingStrategy.DLQ_ONLY,
            max_recent_errors=50,
            max_errors_before_stop=1000,
            dlq_enabled=False,
        )

        assert config.strategy == ErrorHandlingStrategy.DLQ_ONLY
        assert config.max_recent_errors == 50
        assert config.max_errors_before_stop == 1000
        assert config.dlq_enabled is False


class TestErrorHandlingStrategy:
    """Tests for ErrorHandlingStrategy enum."""

    def test_strategy_values(self):
        """Test all strategy values exist."""
        assert ErrorHandlingStrategy.STOP.value == "stop"
        assert ErrorHandlingStrategy.CONTINUE.value == "continue"
        assert ErrorHandlingStrategy.RETRY_THEN_CONTINUE.value == "retry_then_continue"
        assert ErrorHandlingStrategy.RETRY_THEN_DLQ.value == "retry_then_dlq"
        assert ErrorHandlingStrategy.DLQ_ONLY.value == "dlq_only"


# =============================================================================
# Subscription Error Handler Tests
# =============================================================================


class TestSubscriptionErrorHandler:
    """Tests for SubscriptionErrorHandler."""

    @pytest.fixture
    def handler(self) -> SubscriptionErrorHandler:
        """Create test error handler."""
        return SubscriptionErrorHandler(
            subscription_name="TestSubscription",
        )

    @pytest.fixture
    def mock_dlq_repo(self):
        """Create mock DLQ repository."""
        repo = AsyncMock()
        repo.add_failed_event = AsyncMock()
        return repo

    @pytest.fixture
    def handler_with_dlq(self, mock_dlq_repo) -> SubscriptionErrorHandler:
        """Create test error handler with DLQ."""
        return SubscriptionErrorHandler(
            subscription_name="TestSubscription",
            config=ErrorHandlingConfig(
                strategy=ErrorHandlingStrategy.RETRY_THEN_DLQ,
                dlq_enabled=True,
            ),
            dlq_repo=mock_dlq_repo,
        )

    @pytest.mark.asyncio
    async def test_handle_error_basic(self, handler: SubscriptionErrorHandler):
        """Test basic error handling."""
        error = ValueError("Test error")

        error_info = await handler.handle_error(error)

        assert error_info.error_type == "ValueError"
        assert error_info.error_message == "Test error"
        assert error_info.subscription_name == "TestSubscription"

    @pytest.mark.asyncio
    async def test_handle_error_with_stored_event(
        self,
        handler: SubscriptionErrorHandler,
    ):
        """Test error handling with stored event context."""
        error = ValueError("Test error")

        # Create mock stored event
        stored_event = MagicMock()
        stored_event.event_id = uuid4()
        stored_event.event_type = "OrderCreated"
        stored_event.global_position = 42

        error_info = await handler.handle_error(error, stored_event=stored_event)

        assert error_info.event_id == stored_event.event_id
        assert error_info.event_type == "OrderCreated"
        assert error_info.position == 42

    @pytest.mark.asyncio
    async def test_handle_error_updates_stats(
        self,
        handler: SubscriptionErrorHandler,
    ):
        """Test that handling error updates statistics."""
        error = ConnectionError("Connection failed")

        await handler.handle_error(error)

        assert handler.stats.total_errors == 1
        assert handler.stats.transient_errors == 1
        assert handler.total_errors == 1

    @pytest.mark.asyncio
    async def test_handle_error_tracks_recent_errors(
        self,
        handler: SubscriptionErrorHandler,
    ):
        """Test that recent errors are tracked."""
        error = ValueError("Test error")

        await handler.handle_error(error)

        assert len(handler.recent_errors) == 1
        assert handler.recent_errors[0].error_type == "ValueError"

    @pytest.mark.asyncio
    async def test_recent_errors_limited(self, handler: SubscriptionErrorHandler):
        """Test that recent errors are limited."""
        handler.config = ErrorHandlingConfig(max_recent_errors=5)

        for i in range(10):
            await handler.handle_error(ValueError(f"Error {i}"))

        assert len(handler.recent_errors) == 5
        # Should keep most recent
        assert handler.recent_errors[0].error_message == "Error 5"
        assert handler.recent_errors[-1].error_message == "Error 9"

    @pytest.mark.asyncio
    async def test_handle_error_sends_to_dlq(
        self,
        handler_with_dlq: SubscriptionErrorHandler,
        mock_dlq_repo,
    ):
        """Test that permanent errors are sent to DLQ."""
        error = ValueError("Invalid data")  # Non-retryable error

        await handler_with_dlq.handle_error(error)

        mock_dlq_repo.add_failed_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_error_does_not_send_transient_to_dlq(
        self,
        handler_with_dlq: SubscriptionErrorHandler,
        mock_dlq_repo,
    ):
        """Test that transient errors are not sent to DLQ."""
        error = ConnectionError("Connection failed")  # Retryable error

        await handler_with_dlq.handle_error(error)

        mock_dlq_repo.add_failed_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_error_callback(self, handler: SubscriptionErrorHandler):
        """Test registering error callback."""
        callback = AsyncMock()
        handler.on_error(callback)

        await handler.handle_error(ValueError("Test"))

        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_category_callback(self, handler: SubscriptionErrorHandler):
        """Test category-specific callback."""
        callback = AsyncMock()
        handler.on_category(ErrorCategory.APPLICATION, callback)

        await handler.handle_error(ValueError("Test"))  # APPLICATION category

        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_severity_callback(self, handler: SubscriptionErrorHandler):
        """Test severity-specific callback."""
        callback = AsyncMock()
        handler.on_severity(ErrorSeverity.HIGH, callback)

        await handler.handle_error(TypeError("Test"))  # HIGH severity

        callback.assert_called_once()

    def test_should_continue(self, handler: SubscriptionErrorHandler):
        """Test should_continue with no limit."""
        assert handler.should_continue() is True

    @pytest.mark.asyncio
    async def test_should_continue_with_limit(self):
        """Test should_continue with error limit."""
        config = ErrorHandlingConfig(max_errors_before_stop=5)
        handler = SubscriptionErrorHandler(
            subscription_name="Test",
            config=config,
        )

        for _ in range(4):
            await handler.handle_error(ValueError("Test"))

        assert handler.should_continue() is True

        await handler.handle_error(ValueError("Test"))

        assert handler.should_continue() is False

    def test_should_retry(self, handler: SubscriptionErrorHandler):
        """Test should_retry method."""
        assert handler.should_retry(ConnectionError("test")) is True
        assert handler.should_retry(ValueError("test")) is False

    def test_get_health_status(self, handler: SubscriptionErrorHandler):
        """Test getting health status."""
        status = handler.get_health_status()

        assert "healthy" in status
        assert "warnings" in status
        assert "errors" in status
        assert "stats" in status

    @pytest.mark.asyncio
    async def test_clear_stats(self, handler: SubscriptionErrorHandler):
        """Test clearing statistics."""
        await handler.handle_error(ValueError("Test"))

        assert handler.total_errors == 1
        assert len(handler.recent_errors) == 1

        await handler.clear_stats()

        assert handler.total_errors == 0
        assert len(handler.recent_errors) == 0


# =============================================================================
# Module Imports Test
# =============================================================================


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_error_handling(self):
        """Test all exports are importable."""
        from eventsource.subscriptions.error_handling import (
            ErrorCategory,
            ErrorClassification,
            ErrorClassifier,
            ErrorHandlerRegistry,
            ErrorHandlingConfig,
            ErrorHandlingStrategy,
            ErrorInfo,
            ErrorSeverity,
            ErrorStats,
            SubscriptionErrorHandler,
            get_default_classifier,
        )

        assert ErrorCategory is not None
        assert ErrorSeverity is not None
        assert ErrorClassification is not None
        assert ErrorClassifier is not None
        assert ErrorInfo is not None
        assert ErrorStats is not None
        assert ErrorHandlerRegistry is not None
        assert ErrorHandlingStrategy is not None
        assert ErrorHandlingConfig is not None
        assert SubscriptionErrorHandler is not None
        assert get_default_classifier is not None
