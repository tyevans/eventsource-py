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


def _make_error_info(**kwargs: object) -> ErrorInfo:
    cat = kwargs.pop("category", ErrorCategory.APPLICATION)
    sev = kwargs.pop("severity", ErrorSeverity.MEDIUM)
    ret = kwargs.pop("retryable", False)
    position = kwargs.pop("position", 1)
    defaults: dict[str, object] = {
        "event_id": uuid4(),
        "event_type": "TestEvent",
        "position": pos(int(position)),  # type: ignore[arg-type]
        "error_type": "ValueError",
        "error_message": "Test error",
        "error_stacktrace": "...",
        "classification": ErrorClassification(category=cat, severity=sev, retryable=ret),  # type: ignore[arg-type]
        "subscription_name": "",
        "retry_count": 0,
        "sent_to_dlq": False,
    }
    return ErrorInfo(**(defaults | kwargs))  # type: ignore[arg-type]


class TestErrorInfo:
    @pytest.fixture
    def error_info(self) -> ErrorInfo:
        return _make_error_info(
            position=100,
            error_message="Test error message",
            error_stacktrace="Traceback...",
            subscription_name="TestSubscription",
            retry_count=2,
        )

    def test_error_info_fields(self, error_info: ErrorInfo):
        assert error_info.event_type == "TestEvent" and error_info.position == pos(100)
        assert error_info.error_type == "ValueError"
        assert error_info.subscription_name == "TestSubscription"
        assert error_info.retry_count == 2 and error_info.sent_to_dlq is False

    def test_error_info_to_dict(self, error_info: ErrorInfo):
        data = error_info.to_dict()
        assert data["event_type"] == "TestEvent" and data["position"] == pos(100).to_str()
        assert data["error_type"] == "ValueError" and data["category"] == "application"
        assert data["severity"] == "medium" and data["retryable"] is False and "timestamp" in data


class TestErrorStats:
    @pytest.fixture
    def stats(self) -> ErrorStats:
        return ErrorStats()

    def test_initial_state(self, stats: ErrorStats):
        vals = (stats.total_errors, stats.transient_errors, stats.permanent_errors, stats.dlq_count)
        assert vals == (0, 0, 0, 0) and stats.first_error_at is None and stats.last_error_at is None

    def test_record_transient_error(self, stats: ErrorStats):
        info = _make_error_info(
            error_type="ConnectionError",
            error_message="Connection refused",
            category=ErrorCategory.TRANSIENT,
            retryable=True,
        )
        stats.record_error(info)
        assert (stats.total_errors, stats.transient_errors, stats.permanent_errors) == (1, 1, 0)
        assert stats.errors_by_category["transient"] == 1
        assert stats.errors_by_type["ConnectionError"] == 1
        assert stats.first_error_at is not None and stats.last_error_at is not None

    def test_record_permanent_error(self, stats: ErrorStats):
        stats.record_error(_make_error_info(error_message="Invalid data"))
        assert (stats.total_errors, stats.transient_errors, stats.permanent_errors) == (1, 0, 1)
        assert stats.errors_by_category["application"] == 1

    def test_record_dlq_error(self, stats: ErrorStats):
        stats.record_error(_make_error_info(error_message="Invalid data", sent_to_dlq=True))
        assert stats.dlq_count == 1

    def test_to_dict(self, stats: ErrorStats):
        expected = (
            "total_errors transient_errors permanent_errors "
            "dlq_count errors_by_type errors_by_category"
        )
        assert set(expected.split()) <= stats.to_dict().keys()


class TestErrorHandlerRegistry:
    @pytest.fixture
    def registry(self) -> ErrorHandlerRegistry:
        return ErrorHandlerRegistry()

    @pytest.fixture
    def error_info(self) -> ErrorInfo:
        return _make_error_info(severity=ErrorSeverity.HIGH)

    @pytest.mark.asyncio
    async def test_register_and_notify_callback(self, registry, error_info):
        cb = AsyncMock()
        registry.register(cb)
        await registry.notify(error_info)
        cb.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_register_sync_callback(self, registry, error_info):
        cb = MagicMock()
        registry.register_sync(cb)
        await registry.notify(error_info)
        cb.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_category_callback(self, registry, error_info):
        cb = AsyncMock()
        registry.register_for_category(ErrorCategory.APPLICATION, cb)
        await registry.notify(error_info)
        cb.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_category_callback_not_called_for_different_category(self, registry):
        cb = AsyncMock()
        registry.register_for_category(ErrorCategory.TRANSIENT, cb)
        await registry.notify(_make_error_info(category=ErrorCategory.APPLICATION))
        cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_severity_callback(self, registry, error_info):
        cb = AsyncMock()
        registry.register_for_severity(ErrorSeverity.HIGH, cb)
        await registry.notify(error_info)
        cb.assert_called_once_with(error_info)

    @pytest.mark.asyncio
    async def test_multiple_callbacks(self, registry, error_info):
        callbacks = [AsyncMock(), AsyncMock(), AsyncMock()]
        for cb in callbacks:
            registry.register(cb)
        await registry.notify(error_info)
        for cb in callbacks:
            cb.assert_called_once()

    @pytest.mark.asyncio
    async def test_callback_error_does_not_stop_others(self, registry, error_info):
        registry.register(AsyncMock(side_effect=RuntimeError("Callback error")))
        cb2 = AsyncMock()
        registry.register(cb2)
        await registry.notify(error_info)
        cb2.assert_called_once()

    def test_clear(self, registry: ErrorHandlerRegistry):
        cb = AsyncMock()
        registry.register(cb)
        registry.register_for_category(ErrorCategory.APPLICATION, cb)
        registry.register_for_severity(ErrorSeverity.HIGH, cb)
        registry.clear()
        assert not (
            registry._callbacks or registry._category_callbacks or registry._severity_callbacks
        )


class TestErrorHandlingConfig:
    def test_default_config(self):
        config = ErrorHandlingConfig()
        assert config.strategy == ErrorHandlingStrategy.RETRY_THEN_CONTINUE
        assert config.max_recent_errors == 100 and config.max_errors_before_stop is None
        assert config.dlq_enabled is True

    def test_custom_config(self):
        config = ErrorHandlingConfig(
            strategy=ErrorHandlingStrategy.DLQ_ONLY,
            max_recent_errors=50,
            max_errors_before_stop=1000,
            dlq_enabled=False,
        )
        assert config.strategy == ErrorHandlingStrategy.DLQ_ONLY and config.dlq_enabled is False
        assert config.max_recent_errors == 50 and config.max_errors_before_stop == 1000


class TestErrorHandlingStrategy:
    def test_strategy_values(self):
        expected = ["stop", "continue", "retry_then_continue", "retry_then_dlq", "dlq_only"]
        assert [s.value for s in ErrorHandlingStrategy] == expected
