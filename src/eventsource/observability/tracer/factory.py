"""Factory function for creating tracers."""

from __future__ import annotations

from eventsource.observability.tracer.null import NullTracer
from eventsource.observability.tracer.opentelemetry import OpenTelemetryTracer
from eventsource.observability.tracer.protocol import Tracer
from eventsource.observability.tracing import OTEL_AVAILABLE


def create_tracer(
    name: str,
    enable_tracing: bool = True,
) -> Tracer:
    """
    Factory function to create the appropriate tracer.

    Creates an OpenTelemetryTracer if tracing is enabled and OTEL is available,
    otherwise returns a NullTracer.

    This function provides backward compatibility with the `enable_tracing`
    parameter pattern used throughout the library.

    Args:
        name: Tracer name (typically __name__)
        enable_tracing: Whether tracing should be enabled (default True)

    Returns:
        OpenTelemetryTracer if enabled and available, NullTracer otherwise

    Example:
        >>> # In component constructor
        >>> def __init__(self, enable_tracing: bool = True):
        ...     self._tracer = create_tracer(__name__, enable_tracing)
        >>>
        >>> # With explicit tracer
        >>> def __init__(self, tracer: Tracer | None = None, enable_tracing: bool = True):
        ...     self._tracer = tracer or create_tracer(__name__, enable_tracing)
    """
    if enable_tracing and OTEL_AVAILABLE:
        return OpenTelemetryTracer(name)
    return NullTracer()


__all__ = ["create_tracer"]
