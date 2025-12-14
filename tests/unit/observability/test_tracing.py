"""
Unit tests for observability tracing utilities.

Tests for:
- OTEL_AVAILABLE constant
- get_tracer() function
- should_trace() function
- @traced decorator
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest


class TestOTELAvailable:
    """Tests for OTEL_AVAILABLE constant."""

    def test_otel_available_is_boolean(self):
        """OTEL_AVAILABLE is a boolean."""
        from eventsource.observability import OTEL_AVAILABLE

        assert isinstance(OTEL_AVAILABLE, bool)

    def test_otel_available_reflects_import(self):
        """OTEL_AVAILABLE is True when opentelemetry is installed."""
        from eventsource.observability import OTEL_AVAILABLE

        # In test environment, OpenTelemetry should be available
        assert OTEL_AVAILABLE is True


class TestGetTracer:
    """Tests for get_tracer function."""

    def test_returns_tracer_when_available(self):
        """Returns tracer when OpenTelemetry is available."""
        from eventsource.observability import get_tracer

        tracer = get_tracer("test_module")
        assert tracer is not None

    def test_returns_none_when_unavailable(self, monkeypatch):
        """Returns None when OpenTelemetry is not available."""
        monkeypatch.setattr("eventsource.observability.tracing.OTEL_AVAILABLE", False)

        from eventsource.observability.tracing import get_tracer

        result = get_tracer("test_module")
        assert result is None


class TestShouldTrace:
    """Tests for should_trace function."""

    def test_true_when_enabled_and_available(self):
        """Returns True when tracing enabled and OTEL available."""
        from eventsource.observability import should_trace

        # OTEL is available in test environment
        assert should_trace(True) is True

    def test_false_when_disabled(self):
        """Returns False when tracing disabled."""
        from eventsource.observability import should_trace

        assert should_trace(False) is False

    def test_false_when_unavailable(self, monkeypatch):
        """Returns False when OTEL not available."""
        monkeypatch.setattr("eventsource.observability.tracing.OTEL_AVAILABLE", False)

        from eventsource.observability.tracing import should_trace

        assert should_trace(True) is False


class TestTracedDecorator:
    """Tests for @traced decorator."""

    @pytest.fixture
    def mock_tracer(self):
        """Create mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        tracer.start_as_current_span.return_value.__enter__ = Mock(return_value=span)
        tracer.start_as_current_span.return_value.__exit__ = Mock(return_value=None)
        return tracer

    @pytest.mark.asyncio
    async def test_async_method_traced(self, mock_tracer):
        """Async method creates span when tracing enabled."""
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        obj = MyClass()
        result = await obj.operation()

        assert result == "result"
        mock_tracer.start_as_current_span.assert_called_once_with("test.operation", attributes={})

    def test_sync_method_traced(self, mock_tracer):
        """Sync method creates span when tracing enabled."""
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation")
            def operation(self) -> str:
                return "result"

        obj = MyClass()
        result = obj.operation()

        assert result == "result"
        mock_tracer.start_as_current_span.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_span_when_disabled(self, mock_tracer):
        """No span created when tracing disabled."""
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = False

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        obj = MyClass()
        result = await obj.operation()

        assert result == "result"
        mock_tracer.start_as_current_span.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_span_when_tracer_none(self):
        """No span created when tracer is None."""
        from eventsource.observability import traced

        class MyClass:
            _tracer = None
            _enable_tracing = True

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        obj = MyClass()
        result = await obj.operation()

        assert result == "result"  # Method still executes

    @pytest.mark.asyncio
    async def test_static_attributes_included(self, mock_tracer):
        """Static attributes are passed to span."""
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation", attributes={"key": "value", "count": 42})
            async def operation(self) -> None:
                pass

        obj = MyClass()
        await obj.operation()

        mock_tracer.start_as_current_span.assert_called_once_with(
            "test.operation", attributes={"key": "value", "count": 42}
        )

    @pytest.mark.asyncio
    async def test_exception_propagates(self, mock_tracer):
        """Exceptions from method propagate through decorator."""
        from eventsource.observability import traced

        class MyClass:
            _tracer = mock_tracer
            _enable_tracing = True

            @traced("test.operation")
            async def operation(self) -> None:
                raise ValueError("test error")

        obj = MyClass()
        with pytest.raises(ValueError, match="test error"):
            await obj.operation()

    @pytest.mark.asyncio
    async def test_missing_tracer_attribute(self):
        """Method works when class lacks _tracer attribute."""
        from eventsource.observability import traced

        class MyClass:
            # No _tracer or _enable_tracing

            @traced("test.operation")
            async def operation(self) -> str:
                return "result"

        obj = MyClass()
        result = await obj.operation()

        assert result == "result"

    def test_preserves_function_metadata(self):
        """Decorator preserves function name and docstring."""
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
        """All exports are importable from observability."""
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
        """All exports are importable from tracing module."""
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
