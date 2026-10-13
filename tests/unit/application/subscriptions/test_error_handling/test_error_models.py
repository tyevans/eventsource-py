"""Tests for error models, stats, and handler registry."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.subscriptions.error_handling import (
    ErrorCategory,
    ErrorClassification,
    ErrorHandlerRegistry,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    ErrorSeverity,
    ErrorStats,
)
from tests.unit.application.subscriptions.test_error_handling._fixtures import pos


class TestErrorInfo:
    """Tests for ErrorInfo dataclass."""

    @pytest.fixture
    def error_info(self) -> ErrorInfo:
        """Create test error info."""
        return ErrorInfo(
            event_id=uuid4(),
            event_type="TestEvent",
            position=pos(100),
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
        assert error_info.position == pos(100)
        assert error_info.error_type == "ValueError"
        assert error_info.subscription_name == "TestSubscription"
        assert error_info.retry_count == 2
        assert error_info.sent_to_dlq is False

    def test_error_info_to_dict(self, error_info: ErrorInfo):
        """Test conversion to dictionary."""
        data = error_info.to_dict()

        assert data["event_type"] == "TestEvent"
        assert data["position"] == pos(100).to_str()
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
            position=pos(1),
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
            position=pos(1),
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
            position=pos(1),
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
            position=pos(1),
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
            position=pos(1),
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
