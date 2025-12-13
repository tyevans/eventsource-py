"""
Unit tests for tracer protocol and implementations.

Tests for:
- Tracer Protocol (runtime_checkable)
- NullTracer class
- OpenTelemetryTracer class
- MockTracer class
- create_tracer() factory function
"""

from __future__ import annotations

import contextlib
from typing import Any

import pytest

from eventsource.observability import (
    OTEL_AVAILABLE,
    MockTracer,
    NullTracer,
    OpenTelemetryTracer,
    Tracer,
    create_tracer,
)


class TestTracerProtocol:
    """Tests for Tracer protocol."""

    def test_protocol_is_runtime_checkable(self):
        """Tracer protocol supports isinstance checks."""
        # The protocol should be marked as runtime_checkable
        assert hasattr(Tracer, "__protocol_attrs__") or hasattr(Tracer, "_is_runtime_protocol")

    def test_null_tracer_implements_protocol(self):
        """NullTracer implements Tracer protocol."""
        tracer = NullTracer()
        assert isinstance(tracer, Tracer)

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_otel_tracer_implements_protocol(self):
        """OpenTelemetryTracer implements Tracer protocol."""
        tracer = OpenTelemetryTracer(__name__)
        assert isinstance(tracer, Tracer)

    def test_mock_tracer_implements_protocol(self):
        """MockTracer implements Tracer protocol."""
        tracer = MockTracer()
        assert isinstance(tracer, Tracer)

    def test_custom_implementation_matches_protocol(self):
        """Custom implementations can match the protocol."""
        from eventsource.observability import SpanKindEnum

        class CustomTracer:
            def span(
                self,
                name: str,
                attributes: dict[str, Any] | None = None,
            ):
                return contextlib.nullcontext()

            @property
            def enabled(self) -> bool:
                return False

            def start_span(
                self,
                name: str,
                kind: SpanKindEnum = SpanKindEnum.INTERNAL,
                attributes: dict[str, Any] | None = None,
                context: Any | None = None,
            ) -> None:
                return None

            def span_with_kind(
                self,
                name: str,
                kind: SpanKindEnum = SpanKindEnum.INTERNAL,
                attributes: dict[str, Any] | None = None,
                context: Any | None = None,
            ):
                return contextlib.nullcontext()

        tracer = CustomTracer()
        assert isinstance(tracer, Tracer)


class TestNullTracer:
    """Tests for NullTracer class."""

    def test_span_is_context_manager(self):
        """span() returns a context manager."""
        tracer = NullTracer()
        with tracer.span("test") as span:
            assert span is None

    def test_span_with_attributes(self):
        """span() accepts attributes without error."""
        tracer = NullTracer()
        with tracer.span("test", {"key": "value", "count": 42}) as span:
            assert span is None

    def test_span_with_none_attributes(self):
        """span() accepts None attributes."""
        tracer = NullTracer()
        with tracer.span("test", None) as span:
            assert span is None

    def test_enabled_is_false(self):
        """enabled property returns False."""
        tracer = NullTracer()
        assert tracer.enabled is False

    def test_nested_spans_work(self):
        """Nested spans work without error."""
        tracer = NullTracer()
        with tracer.span("outer") as outer, tracer.span("inner") as inner:
            assert outer is None
            assert inner is None

    def test_span_does_nothing_on_exception(self):
        """span() handles exceptions correctly (no interference)."""
        tracer = NullTracer()
        with pytest.raises(ValueError, match="test error"), tracer.span("test"):
            raise ValueError("test error")


class TestOpenTelemetryTracer:
    """Tests for OpenTelemetryTracer class."""

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_initialization(self):
        """OpenTelemetryTracer initializes with tracer name."""
        tracer = OpenTelemetryTracer(__name__)
        assert tracer._tracer is not None

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_creates_real_spans(self):
        """span() creates real OpenTelemetry spans."""
        tracer = OpenTelemetryTracer(__name__)
        with tracer.span("test.span") as span:
            assert span is not None
            # Verify it's an OTEL span
            assert hasattr(span, "set_attribute")
            assert hasattr(span, "add_event")

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_span_with_attributes(self):
        """span() passes attributes to the span."""
        tracer = OpenTelemetryTracer(__name__)
        with tracer.span("test.span", {"key": "value"}) as span:
            assert span is not None

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_enabled_is_true(self):
        """enabled property returns True."""
        tracer = OpenTelemetryTracer(__name__)
        assert tracer.enabled is True

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_nested_spans(self):
        """Nested spans create parent-child relationships."""
        tracer = OpenTelemetryTracer(__name__)
        with tracer.span("outer") as outer, tracer.span("inner") as inner:
            assert outer is not None
            assert inner is not None

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_span_handles_exception(self):
        """span() properly handles exceptions."""
        tracer = OpenTelemetryTracer(__name__)
        with pytest.raises(ValueError, match="test error"), tracer.span("test"):
            raise ValueError("test error")

    @pytest.mark.skipif(OTEL_AVAILABLE, reason="OTEL is installed")
    def test_raises_without_otel(self):
        """OpenTelemetryTracer raises ImportError when OTEL not installed."""
        with pytest.raises(ImportError):
            OpenTelemetryTracer(__name__)


class TestMockTracer:
    """Tests for MockTracer class."""

    def test_records_spans(self):
        """MockTracer records span information."""
        tracer = MockTracer()
        with tracer.span("test.operation", {"key": "value"}):
            pass

        assert len(tracer.spans) == 1
        assert tracer.spans[0] == ("test.operation", {"key": "value"})

    def test_records_multiple_spans(self):
        """MockTracer records multiple spans."""
        tracer = MockTracer()
        with tracer.span("first"):
            pass
        with tracer.span("second", {"attr": 123}):
            pass

        assert len(tracer.spans) == 2
        assert tracer.spans[0] == ("first", None)
        assert tracer.spans[1] == ("second", {"attr": 123})

    def test_records_nested_spans(self):
        """MockTracer records nested spans in order."""
        tracer = MockTracer()
        with tracer.span("outer"), tracer.span("inner"):
            pass

        assert tracer.span_names == ["outer", "inner"]

    def test_span_names_property(self):
        """span_names returns list of just span names."""
        tracer = MockTracer()
        with tracer.span("op1", {"key": "value"}):
            pass
        with tracer.span("op2"):
            pass

        assert tracer.span_names == ["op1", "op2"]

    def test_enabled_is_true(self):
        """enabled property returns True (for attribute computation)."""
        tracer = MockTracer()
        assert tracer.enabled is True

    def test_clear_method(self):
        """clear() removes all recorded spans."""
        tracer = MockTracer()
        with tracer.span("test"):
            pass

        assert len(tracer.spans) == 1
        tracer.clear()
        assert len(tracer.spans) == 0

    def test_span_yields_none(self):
        """span() yields None (not a real span)."""
        tracer = MockTracer()
        with tracer.span("test") as span:
            assert span is None

    def test_handles_exception(self):
        """MockTracer records span even when exception occurs."""
        tracer = MockTracer()
        with pytest.raises(ValueError), tracer.span("failing"):
            raise ValueError("error")

        # Span is recorded before exception
        assert tracer.span_names == ["failing"]


class TestCreateTracer:
    """Tests for create_tracer() factory function."""

    def test_returns_null_when_disabled(self):
        """Returns NullTracer when tracing is disabled."""
        tracer = create_tracer(__name__, enable_tracing=False)
        assert isinstance(tracer, NullTracer)

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_returns_otel_when_enabled_and_available(self):
        """Returns OpenTelemetryTracer when enabled and OTEL available."""
        tracer = create_tracer(__name__, enable_tracing=True)
        assert isinstance(tracer, OpenTelemetryTracer)

    @pytest.mark.skipif(OTEL_AVAILABLE, reason="OTEL is installed")
    def test_returns_null_when_otel_not_available(self):
        """Returns NullTracer when OTEL not available."""
        tracer = create_tracer(__name__, enable_tracing=True)
        assert isinstance(tracer, NullTracer)

    def test_default_enable_tracing_is_true(self):
        """Default enable_tracing parameter is True."""
        tracer = create_tracer(__name__)
        # If OTEL is available, should return OpenTelemetryTracer
        # If OTEL is not available, should return NullTracer
        assert isinstance(tracer, (NullTracer, OpenTelemetryTracer))

    def test_returns_tracer_protocol(self):
        """Return value implements Tracer protocol."""
        tracer = create_tracer(__name__, enable_tracing=True)
        assert isinstance(tracer, Tracer)

        tracer = create_tracer(__name__, enable_tracing=False)
        assert isinstance(tracer, Tracer)


class TestTracerUsagePatterns:
    """Tests demonstrating common tracer usage patterns."""

    def test_component_with_injected_tracer(self):
        """Component can receive tracer via dependency injection."""

        class MyComponent:
            def __init__(self, tracer: Tracer | None = None):
                self._tracer = tracer or NullTracer()

            def operation(self, item_id: str) -> str:
                with self._tracer.span("component.operation", {"item.id": item_id}):
                    return f"processed: {item_id}"

        # With NullTracer (no tracing)
        component = MyComponent()
        result = component.operation("123")
        assert result == "processed: 123"

        # With MockTracer (for testing)
        mock_tracer = MockTracer()
        component = MyComponent(tracer=mock_tracer)
        result = component.operation("456")
        assert result == "processed: 456"
        assert mock_tracer.spans == [("component.operation", {"item.id": "456"})]

    @pytest.mark.asyncio
    async def test_async_component_with_tracer(self):
        """Async component can use tracer."""

        class AsyncComponent:
            def __init__(self, tracer: Tracer | None = None):
                self._tracer = tracer or NullTracer()

            async def operation(self, item_id: str) -> str:
                with self._tracer.span("async.operation", {"item.id": item_id}):
                    return f"async: {item_id}"

        mock_tracer = MockTracer()
        component = AsyncComponent(tracer=mock_tracer)
        result = await component.operation("789")

        assert result == "async: 789"
        assert mock_tracer.span_names == ["async.operation"]

    def test_conditional_attribute_computation(self):
        """enabled property allows conditional expensive computation."""

        expensive_called = False

        def expensive_computation() -> dict[str, Any]:
            nonlocal expensive_called
            expensive_called = True
            return {"expensive": "value"}

        # With NullTracer, skip expensive computation
        tracer = NullTracer()
        with tracer.span("operation"):
            if tracer.enabled:
                _attrs = expensive_computation()
        assert expensive_called is False

        # With MockTracer, do expensive computation
        mock_tracer = MockTracer()
        with mock_tracer.span("operation"):
            if mock_tracer.enabled:
                _attrs = expensive_computation()
        assert expensive_called is True

    def test_create_tracer_with_enable_tracing_pattern(self):
        """create_tracer works with enable_tracing constructor pattern."""

        class MyStore:
            def __init__(self, enable_tracing: bool = True):
                self._tracer = create_tracer(__name__, enable_tracing)

            def save(self, item_id: str) -> str:
                with self._tracer.span("store.save", {"item.id": item_id}):
                    return f"saved: {item_id}"

        # Disabled tracing
        store = MyStore(enable_tracing=False)
        assert isinstance(store._tracer, NullTracer)
        result = store.save("123")
        assert result == "saved: 123"

    def test_tracer_with_explicit_override(self):
        """Explicit tracer overrides enable_tracing."""

        class MyStore:
            def __init__(
                self,
                tracer: Tracer | None = None,
                enable_tracing: bool = True,
            ):
                self._tracer = tracer or create_tracer(__name__, enable_tracing)

        # Explicit tracer takes precedence
        mock_tracer = MockTracer()
        store = MyStore(tracer=mock_tracer, enable_tracing=False)
        assert store._tracer is mock_tracer


class TestImports:
    """Test module imports work correctly."""

    def test_import_from_observability(self):
        """All tracer exports are importable from observability."""
        from eventsource.observability import (
            MockTracer,
            NullTracer,
            OpenTelemetryTracer,
            Tracer,
            create_tracer,
        )

        assert Tracer is not None
        assert NullTracer is not None
        assert OpenTelemetryTracer is not None
        assert MockTracer is not None
        assert create_tracer is not None

    def test_import_from_tracer_module(self):
        """All exports are importable from tracer module."""
        from eventsource.observability.tracer import (
            MockTracer,
            NullTracer,
            OpenTelemetryTracer,
            Tracer,
            create_tracer,
        )

        assert Tracer is not None
        assert NullTracer is not None
        assert OpenTelemetryTracer is not None
        assert MockTracer is not None
        assert create_tracer is not None

    def test_otel_available_still_exported(self):
        """OTEL_AVAILABLE is still exported from observability."""
        from eventsource.observability import OTEL_AVAILABLE

        assert isinstance(OTEL_AVAILABLE, bool)
