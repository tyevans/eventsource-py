"""
Tracer protocol and implementations for composition-based tracing.

This module provides a tracer abstraction that can be injected into components
as a dependency, replacing the inheritance-based TracingMixin pattern.
"""

from __future__ import annotations

from eventsource.observability.tracer.factory import create_tracer
from eventsource.observability.tracer.mock import MockTracer
from eventsource.observability.tracer.null import NullTracer
from eventsource.observability.tracer.opentelemetry import OpenTelemetryTracer
from eventsource.observability.tracer.protocol import SpanKindEnum, Tracer

__all__ = [
    "MockTracer",
    "NullTracer",
    "OpenTelemetryTracer",
    "SpanKindEnum",
    "Tracer",
    "create_tracer",
]
