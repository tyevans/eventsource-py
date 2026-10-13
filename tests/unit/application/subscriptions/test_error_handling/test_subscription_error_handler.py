"""Tests for SubscriptionErrorHandler and module imports."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.subscriptions.error_handling import (
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorSeverity,
    SubscriptionErrorHandler,
)
from tests.unit.application.subscriptions.test_error_handling._fixtures import pos


class TestSubscriptionErrorHandler:
    """Tests for SubscriptionErrorHandler."""

    @pytest.fixture
    def handler(self) -> SubscriptionErrorHandler:
        """Create test error handler."""
        return SubscriptionErrorHandler(
            subscription_name="TestSubscription",
        )

    @pytest.fixture
    def handler_with_dlq(self, mock_dlq_repo: AsyncMock) -> SubscriptionErrorHandler:
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
    async def test_handle_error_with_envelope(
        self,
        handler: SubscriptionErrorHandler,
    ):
        """Test error handling with envelope context."""
        error = ValueError("Test error")

        event = MagicMock()
        event.event_id = uuid4()
        event.event_type = "OrderCreated"
        envelope = MagicMock()
        envelope.event = event
        envelope.position = pos(42)

        error_info = await handler.handle_error(error, envelope=envelope)

        assert error_info.event_id == event.event_id
        assert error_info.event_type == "OrderCreated"
        assert error_info.position == pos(42)

    @pytest.mark.asyncio
    async def test_handle_error_without_envelope_has_no_position(
        self,
        handler: SubscriptionErrorHandler,
    ):
        """With no envelope there is no position -- None, not a sentinel."""
        error_info = await handler.handle_error(ValueError("Test error"))

        assert error_info.position is None

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
        mock_dlq_repo: AsyncMock,
    ):
        """Test that permanent errors are sent to DLQ."""
        error = ValueError("Invalid data")  # Non-retryable error

        await handler_with_dlq.handle_error(error)

        mock_dlq_repo.add_failed_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_error_does_not_send_transient_to_dlq(
        self,
        handler_with_dlq: SubscriptionErrorHandler,
        mock_dlq_repo: AsyncMock,
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


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_error_handling(self):
        """Test all exports are importable."""
        from eventsource.application.subscriptions.error_handling import (
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
