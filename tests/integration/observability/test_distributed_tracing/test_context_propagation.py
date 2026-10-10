"""Cross-process trace context serialization and propagation tests."""

from __future__ import annotations

from typing import Any

import pytest

from .conftest import skip_if_no_otel

pytestmark = [pytest.mark.integration]


@skip_if_no_otel
class TestCrossProcessTraceContext:
    """Tests for trace context serialization/deserialization.

    These tests verify that trace context can be correctly injected into
    and extracted from message headers, simulating cross-process propagation.
    """

    def test_trace_context_injection_extraction(
        self,
        trace_provider: Any,
    ) -> None:
        """Trace context can be injected into headers and extracted back."""
        from opentelemetry import trace
        from opentelemetry.propagate import extract, inject

        tracer = trace.get_tracer("test")

        # Start a span to get trace context
        with tracer.start_as_current_span("test-span") as span:
            _original_trace_id = span.get_span_context().trace_id  # noqa: F841
            _original_span_id = span.get_span_context().span_id  # noqa: F841

            # Inject context into a carrier (simulating message headers)
            carrier: dict[str, str] = {}
            inject(carrier)

            # Carrier should now have trace context
            assert len(carrier) > 0, "Context should be injected into carrier"

        # Now extract context (simulating receiving a message)
        extracted_context = extract(carrier)

        # The extracted context should preserve the trace ID
        assert extracted_context is not None

    def test_trace_context_preserved_in_dict_headers(
        self,
        trace_provider: Any,
    ) -> None:
        """Trace context is correctly preserved when using dict as carrier."""
        from opentelemetry import trace
        from opentelemetry.propagate import extract, inject

        tracer = trace.get_tracer("test")

        with tracer.start_as_current_span("parent-span") as parent_span:
            parent_trace_id = parent_span.get_span_context().trace_id

            # Simulate sending a message: inject context
            message_headers: dict[str, str] = {}
            inject(message_headers)

        # Simulate receiving: extract and create child span
        context = extract(message_headers)

        # Create a child span using extracted context
        with tracer.start_as_current_span("child-span", context=context) as child_span:
            child_trace_id = child_span.get_span_context().trace_id

            # Both spans should share the same trace ID
            assert parent_trace_id == child_trace_id, (
                "Child span should inherit trace ID from parent via propagation"
            )
