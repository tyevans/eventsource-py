"""Unit tests for observability tracing utilities."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest


class TestOTELAvailable:
    """Tests for OTEL_AVAILABLE constant."""

    def test_otel_available_is_boolean(self):
        from eventsource.observability import OTEL_AVAILABLE

        assert isinstance(OTEL_AVAILABLE, bool)

    def test_otel_available_reflects_import(self):
        from eventsource.observability import OTEL_AVAILABLE

        assert OTEL_AVAILABLE is True


class TestGetTracer:
    """Tests for get_tracer function."""

    def test_returns_tracer_when_available(self):
        from eventsource.observability import get_tracer

        tracer = get_tracer("test_module")
        assert tracer is not None

    def test_returns_none_when_unavailable(self, monkeypatch):
        monkeypatch.setattr("eventsource.observability.tracing.OTEL_AVAILABLE", False)
        from eventsource.observability.tracing import get_tracer

        assert get_tracer("test_module") is None


class TestShouldTrace:
    """Tests for should_trace function."""

    def test_true_when_enabled_and_available(self):
        from eventsource.observability import should_trace

        assert should_trace(True) is True

    def test_false_when_disabled(self):
        from eventsource.observability import should_trace

        assert should_trace(False) is False

    def test_false_when_unavailable(self, monkeypatch):
        monkeypatch.setattr("eventsource.observability.tracing.OTEL_AVAILABLE", False)
        from eventsource.observability.tracing import should_trace

        assert should_trace(True) is False


class TestTracedDecorator:
    """Tests for @traced decorator."""

    @pytest.fixture
    def mock_tracer(self):
        tracer = Mock()
        span = MagicMock()
        tracer.start_as_current_span.return_value.__enter__ = Mock(return_value=span)
        tracer.start_as_current_span.return_value.__exit__ = Mock(return_value=None)
        return tracer

    @pytest.mark.asyncio
    async def test_async_method_traced(self, mock_tracer):
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        result = await MyClass().operation()
        assert result == "result"
        mock_tracer.start_as_current_span.assert_called_once_with("test.operation", attributes={})

    def test_sync_method_traced(self, mock_tracer):
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation")
            def operation(self) -> str:
                return "result"

        result = MyClass().operation()
        assert result == "result"
        mock_tracer.start_as_current_span.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_span_when_disabled(self, mock_tracer):
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = False

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        result = await MyClass().operation()
        assert result == "result"
        mock_tracer.start_as_current_span.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_span_when_tracer_none(self):
        from eventsource.observability import traced

        class MyClass:
            _tracer = None
            _enable_tracing = True

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        result = await MyClass().operation()
        assert result == "result"

    @pytest.mark.asyncio
    async def test_static_attributes_included(self, mock_tracer):
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation", attributes={"key": "value", "count": 42})
            async def operation(self) -> None:
                pass

        await MyClass().operation()
        mock_tracer.start_as_current_span.assert_called_once_with(
            "test.operation", attributes={"key": "value", "count": 42}
        )

    @pytest.mark.asyncio
    async def test_exception_propagates(self, mock_tracer):
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation")
            async def operation(self) -> None:
                raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            await MyClass().operation()

    @pytest.mark.asyncio
    async def test_missing_tracer_attribute(self):
        from eventsource.observability import traced

        class MyClass:
            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        result = await MyClass().operation()
        assert result == "result"

    def test_preserves_function_metadata(self):
        from eventsource.observability import traced

        class MyClass:
            _tracer = None
            _enable_tracing = False

            @traced("test.operation")
            async def my_method(self) -> None:
                """My docstring."""
                pass

        obj = MyClass()
        assert obj.my_method.__name__ == "my_method"
        assert obj.my_method.__doc__ == "My docstring."


class TestImports:
    """Test module imports work correctly."""

    def test_import_from_observability(self):
        from eventsource.observability import (
            OTEL_AVAILABLE,
            get_tracer,
            should_trace,
            traced,
        )

        assert OTEL_AVAILABLE is not None
        assert get_tracer is not None
        assert should_trace is not None
        assert traced is not None

    def test_import_from_tracing(self):
        from eventsource.observability.tracing import (
            OTEL_AVAILABLE,
            get_tracer,
            should_trace,
            traced,
        )

        assert OTEL_AVAILABLE is not None
        assert get_tracer is not None
        assert should_trace is not None
        assert traced is not None
