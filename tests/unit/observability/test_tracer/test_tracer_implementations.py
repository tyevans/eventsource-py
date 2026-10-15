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
    def test_protocol_is_runtime_checkable(self):
        assert getattr(Tracer, "_is_runtime_protocol", False)

    def test_null_tracer_implements_protocol(self):
        tracer = NullTracer()
        assert isinstance(tracer, Tracer)

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_otel_tracer_implements_protocol(self):
        tracer = OpenTelemetryTracer(__name__)
        assert isinstance(tracer, Tracer)

    def test_mock_tracer_implements_protocol(self):
        tracer = MockTracer()
        assert isinstance(tracer, Tracer)

    def test_custom_implementation_matches_protocol(self):
        from eventsource.observability import SpanKindEnum

        class CustomTracer:
            def span(self, name: str, attributes: dict[str, Any] | None = None):
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
    def test_span_is_context_manager(self):
        tracer = NullTracer()
        with tracer.span("test") as span:
            assert span is None

    def test_span_with_attributes(self):
        tracer = NullTracer()
        with tracer.span("test", {"key": "value", "count": 42}) as span:
            assert span is None

    def test_span_with_none_attributes(self):
        tracer = NullTracer()
        with tracer.span("test", None) as span:
            assert span is None

    def test_enabled_is_false(self):
        tracer = NullTracer()
        assert tracer.enabled is False

    def test_nested_spans_work(self):
        tracer = NullTracer()
        with tracer.span("outer") as outer, tracer.span("inner") as inner:
            assert outer is None
            assert inner is None

    def test_span_does_nothing_on_exception(self):
        tracer = NullTracer()
        with pytest.raises(ValueError, match="test error"), tracer.span("test"):
            raise ValueError("test error")


class TestOpenTelemetryTracer:
    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_initialization(self):
        tracer = OpenTelemetryTracer(__name__)
        assert tracer._tracer is not None

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_creates_real_spans(self):
        tracer = OpenTelemetryTracer(__name__)
        with tracer.span("test.span") as span:
            assert span is not None
            assert hasattr(span, "set_attribute")
            assert hasattr(span, "add_event")

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_span_with_attributes(self):
        tracer = OpenTelemetryTracer(__name__)
        with tracer.span("test.span", {"key": "value"}) as span:
            assert span is not None

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_enabled_is_true(self):
        tracer = OpenTelemetryTracer(__name__)
        assert tracer.enabled is True

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_nested_spans(self):
        tracer = OpenTelemetryTracer(__name__)
        with tracer.span("outer") as outer, tracer.span("inner") as inner:
            assert outer is not None
            assert inner is not None

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_span_handles_exception(self):
        tracer = OpenTelemetryTracer(__name__)
        with pytest.raises(ValueError, match="test error"), tracer.span("test"):
            raise ValueError("test error")

    @pytest.mark.skipif(OTEL_AVAILABLE, reason="OTEL is installed")
    def test_raises_without_otel(self):
        with pytest.raises(ImportError):
            OpenTelemetryTracer(__name__)


class TestMockTracer:
    def test_records_spans(self):
        tracer = MockTracer()
        with tracer.span("test.operation", {"key": "value"}):
            pass
        assert len(tracer.spans) == 1
        assert tracer.spans[0] == ("test.operation", {"key": "value"})

    def test_records_multiple_spans(self):
        tracer = MockTracer()
        with tracer.span("first"):
            pass
        with tracer.span("second", {"attr": 123}):
            pass
        assert len(tracer.spans) == 2
        assert tracer.spans[0] == ("first", None)
        assert tracer.spans[1] == ("second", {"attr": 123})

    def test_records_nested_spans(self):
        tracer = MockTracer()
        with tracer.span("outer"), tracer.span("inner"):
            pass
        assert tracer.span_names == ["outer", "inner"]

    def test_span_names_property(self):
        tracer = MockTracer()
        with tracer.span("op1", {"key": "value"}):
            pass
        with tracer.span("op2"):
            pass
        assert tracer.span_names == ["op1", "op2"]

    def test_enabled_is_true(self):
        tracer = MockTracer()
        assert tracer.enabled is True

    def test_clear_method(self):
        tracer = MockTracer()
        with tracer.span("test"):
            pass
        assert len(tracer.spans) == 1
        tracer.clear()
        assert len(tracer.spans) == 0

    def test_span_yields_none(self):
        tracer = MockTracer()
        with tracer.span("test") as span:
            assert span is None

    def test_handles_exception(self):
        tracer = MockTracer()
        with pytest.raises(ValueError), tracer.span("failing"):
            raise ValueError("error")
        assert tracer.span_names == ["failing"]
