"""Unit tests for Tracer protocol, NullTracer, OpenTelemetryTracer, and MockTracer."""

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
)


class TestTracerProtocol:
    """Tests for Tracer protocol."""

    def test_protocol_is_runtime_checkable(self):
        """Tracer protocol supports isinstance checks."""
        # `_is_runtime_protocol` is the attribute @runtime_checkable itself
        # sets, stable across Python versions -- unlike `__protocol_attrs__`
        # (a CPython 3.12+ implementation detail unrelated to isinstance
        # support).
        assert getattr(Tracer, "_is_runtime_protocol", False)

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
