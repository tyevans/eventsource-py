"""OpenTelemetry tracer implementation."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any

from eventsource.observability.tracer.protocol import SpanKindEnum

if TYPE_CHECKING:
    from opentelemetry.trace import Span


class OpenTelemetryTracer:
    """
    OpenTelemetry tracer implementation.

    Wraps the OpenTelemetry tracer API to conform to our Tracer protocol.
    Creates real spans when OpenTelemetry is available and configured.

    Args:
        tracer_name: Name for the tracer (typically __name__)

    Raises:
        ImportError: If OpenTelemetry is not installed

    Example:
        >>> from eventsource.observability import OTEL_AVAILABLE
        >>> if OTEL_AVAILABLE:
        ...     tracer = OpenTelemetryTracer(__name__)
        ...     with tracer.span("operation"):
        ...         do_work()
    """

    def __init__(self, tracer_name: str) -> None:
        """
        Initialize OpenTelemetry tracer.

        Args:
            tracer_name: Name for the tracer (typically __name__)

        Raises:
            ImportError: If OpenTelemetry is not installed
        """
        from opentelemetry import trace

        self._tracer = trace.get_tracer(tracer_name)

    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> AbstractContextManager[Span | None]:
        """
        Create an OpenTelemetry span context.

        Args:
            name: Span name
            attributes: Span attributes (optional)

        Returns:
            Context manager yielding the OpenTelemetry Span
        """
        return self._tracer.start_as_current_span(
            name,
            attributes=attributes or {},
        )

    @property
    def enabled(self) -> bool:
        """Always returns True for OpenTelemetryTracer."""
        return True

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Span:
        """
        Start a new span with SpanKind for distributed tracing.

        Args:
            name: Span name
            kind: The span kind (PRODUCER, CONSUMER, etc.)
            attributes: Span attributes (optional)
            context: Optional OpenTelemetry context for linking to parent spans
                    in distributed tracing scenarios (e.g., consumer linking to producer)

        Returns:
            The OpenTelemetry Span. Caller MUST call span.end().
        """
        from opentelemetry.trace import SpanKind as OtelSpanKind

        # Map our enum to OpenTelemetry's SpanKind
        kind_mapping = {
            SpanKindEnum.INTERNAL: OtelSpanKind.INTERNAL,
            SpanKindEnum.PRODUCER: OtelSpanKind.PRODUCER,
            SpanKindEnum.CONSUMER: OtelSpanKind.CONSUMER,
            SpanKindEnum.CLIENT: OtelSpanKind.CLIENT,
            SpanKindEnum.SERVER: OtelSpanKind.SERVER,
        }
        otel_kind = kind_mapping.get(kind, OtelSpanKind.INTERNAL)

        return self._tracer.start_span(
            name,
            kind=otel_kind,
            attributes=attributes or {},
            context=context,
        )

    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> AbstractContextManager[Span | None]:
        """
        Create an OpenTelemetry span context with SpanKind.

        Args:
            name: Span name
            kind: The span kind (PRODUCER, CONSUMER, etc.)
            attributes: Span attributes (optional)
            context: Optional OpenTelemetry context for linking to parent spans
                    in distributed tracing scenarios

        Returns:
            Context manager yielding the OpenTelemetry Span
        """
        from opentelemetry.trace import SpanKind as OtelSpanKind

        # Map our enum to OpenTelemetry's SpanKind
        kind_mapping = {
            SpanKindEnum.INTERNAL: OtelSpanKind.INTERNAL,
            SpanKindEnum.PRODUCER: OtelSpanKind.PRODUCER,
            SpanKindEnum.CONSUMER: OtelSpanKind.CONSUMER,
            SpanKindEnum.CLIENT: OtelSpanKind.CLIENT,
            SpanKindEnum.SERVER: OtelSpanKind.SERVER,
        }
        otel_kind = kind_mapping.get(kind, OtelSpanKind.INTERNAL)

        return self._tracer.start_as_current_span(
            name,
            context=context,
            kind=otel_kind,
            attributes=attributes or {},
        )


__all__ = ["OpenTelemetryTracer"]
