"""
Tracer protocol and implementations for composition-based tracing.

This module provides a tracer abstraction that can be injected into components
as a dependency, replacing the inheritance-based TracingMixin pattern.

The key benefits of composition-based tracing:
- Components have single responsibility (no tracing concern)
- Easy to mock for testing
- Swappable tracer implementations
- No inheritance hierarchy issues

Example:
    >>> from eventsource.observability import create_tracer, NullTracer
    >>>
    >>> # Create tracer based on configuration
    >>> tracer = create_tracer(__name__, enable_tracing=True)
    >>>
    >>> # Or explicitly use NullTracer for testing
    >>> tracer = NullTracer()
    >>>
    >>> # Use in component
    >>> class MyStore:
    ...     def __init__(self, tracer: Tracer | None = None):
    ...         self._tracer = tracer or NullTracer()
    ...
    ...     async def save(self, item_id: str) -> None:
    ...         with self._tracer.span("my_store.save", {"item.id": item_id}):
    ...             await self._do_save(item_id)
"""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from contextlib import AbstractContextManager
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from opentelemetry.trace import Span

# Import OTEL availability check from tracing module
from eventsource.observability.tracing import OTEL_AVAILABLE


class SpanKindEnum(Enum):
    """
    Span kinds for distributed tracing.

    Used to indicate the role a span plays in a trace. This is a simplified
    enumeration that maps to OpenTelemetry's SpanKind when OTEL is available.

    Values:
        INTERNAL: Default span kind for internal operations
        PRODUCER: For producer/publisher operations (e.g., publishing to a queue)
        CONSUMER: For consumer/subscriber operations (e.g., receiving from a queue)
        CLIENT: For client operations (e.g., making HTTP requests)
        SERVER: For server operations (e.g., handling HTTP requests)
    """

    INTERNAL = "internal"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    CLIENT = "client"
    SERVER = "server"


@runtime_checkable
class Tracer(Protocol):
    """
    Protocol for tracers that can create tracing spans.

    Tracers are injected into components as dependencies, enabling
    composition-based tracing rather than inheritance-based.

    Implementations:
    - NullTracer: No-op tracer for when tracing is disabled
    - OpenTelemetryTracer: Wrapper around OpenTelemetry tracer

    Example:
        >>> class MyComponent:
        ...     def __init__(self, tracer: Tracer | None = None):
        ...         self._tracer = tracer or NullTracer()
        ...
        ...     async def operation(self) -> None:
        ...         with self._tracer.span("component.operation"):
        ...             await self._do_operation()
    """

    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> AbstractContextManager[Span | None]:
        """
        Create a tracing span context manager.

        Args:
            name: Span name (e.g., "event_store.append_events")
            attributes: Span attributes (optional)

        Returns:
            Context manager that yields Span or None

        Example:
            >>> with tracer.span("operation", {"key": "value"}) as span:
            ...     # Do traced work
            ...     if span:
            ...         span.set_attribute("result", "success")
        """
        ...

    @property
    def enabled(self) -> bool:
        """
        Check if tracing is enabled.

        Returns:
            True if tracing is active and will create real spans

        Example:
            >>> if tracer.enabled:
            ...     # Prepare expensive attributes only if needed
            ...     attrs = compute_expensive_attributes()
        """
        ...

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Span | None:
        """
        Start a new span with optional SpanKind for distributed tracing.

        Unlike the `span()` context manager, this method returns a span
        that must be manually ended by calling `span.end()`. This is needed
        for messaging scenarios where the span must be kept alive across
        async operations (e.g., RabbitMQ publish with context propagation).

        Args:
            name: Span name (e.g., "eventsource.event_bus.publish")
            kind: The span kind (PRODUCER, CONSUMER, etc.)
            attributes: Span attributes (optional)
            context: Optional context for linking to parent spans in distributed
                    tracing scenarios (e.g., consumer linking to producer)

        Returns:
            The Span object if tracing is enabled, None otherwise.
            Caller MUST call span.end() when the operation is complete.

        Example:
            >>> span = tracer.start_span(
            ...     "publish",
            ...     kind=SpanKindEnum.PRODUCER,
            ...     attributes={"messaging.system": "rabbitmq"},
            ... )
            >>> try:
            ...     do_publish()
            ...     if span:
            ...         span.set_status(Status(StatusCode.OK))
            ... finally:
            ...     if span:
            ...         span.end()
        """
        ...

    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> AbstractContextManager[Span | None]:
        """
        Create a tracing span context manager with SpanKind.

        Like `span()` but allows specifying the span kind for distributed
        tracing scenarios (e.g., PRODUCER for publishing, CONSUMER for consuming).

        Args:
            name: Span name
            kind: The span kind (PRODUCER, CONSUMER, etc.)
            attributes: Span attributes (optional)
            context: Optional context for linking to parent spans in distributed
                    tracing scenarios (e.g., consumer linking to producer)

        Returns:
            Context manager that yields Span or None

        Example:
            >>> with tracer.span_with_kind(
            ...     "publish",
            ...     kind=SpanKindEnum.PRODUCER,
            ...     attributes={"messaging.system": "kafka"},
            ... ) as span:
            ...     do_publish()
            ...     if span:
            ...         span.set_status(Status(StatusCode.OK))
        """
        ...


class NullTracer:
    """
    No-op tracer implementation for when tracing is disabled.

    This tracer creates no spans and has minimal overhead. Use it:
    - When tracing is explicitly disabled
    - In unit tests to avoid tracing side effects
    - As a default when no tracer is provided

    Example:
        >>> tracer = NullTracer()
        >>> with tracer.span("operation"):  # Does nothing
        ...     do_work()
        >>> tracer.enabled  # False
    """

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> Generator[None, None, None]:
        """Create a no-op span context (yields None)."""
        yield None

    @property
    def enabled(self) -> bool:
        """Always returns False for NullTracer."""
        return False

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> None:
        """Return None (no-op for disabled tracing)."""
        return None

    @contextlib.contextmanager
    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Generator[None, None, None]:
        """Create a no-op span context with kind (yields None)."""
        yield None


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


class MockTracer:
    """
    Mock tracer for testing that records span information.

    This tracer is useful for testing components that use tracing,
    allowing you to verify that spans are created with the expected
    names and attributes.

    Example:
        >>> tracer = MockTracer()
        >>> with tracer.span("operation", {"key": "value"}):
        ...     pass
        >>> assert tracer.spans == [("operation", {"key": "value"})]
        >>> assert tracer.span_names == ["operation"]
    """

    def __init__(self) -> None:
        """Initialize MockTracer with empty span list."""
        self.spans: list[tuple[str, dict[str, Any] | None]] = []

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> Generator[None, None, None]:
        """Record span and yield None."""
        self.spans.append((name, attributes))
        yield None

    @property
    def enabled(self) -> bool:
        """Returns True to enable attribute computation in tests."""
        return True

    @property
    def span_names(self) -> list[str]:
        """Get just the span names for easy assertions."""
        return [name for name, _ in self.spans]

    def clear(self) -> None:
        """Clear recorded spans."""
        self.spans.clear()

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> None:
        """Record span and return None (mock spans don't need to be ended)."""
        self.spans.append((name, attributes))
        return None

    @contextlib.contextmanager
    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Generator[None, None, None]:
        """Record span with kind and yield None."""
        self.spans.append((name, attributes))
        yield None


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


__all__ = [
    "Tracer",
    "NullTracer",
    "OpenTelemetryTracer",
    "MockTracer",
    "SpanKindEnum",
    "create_tracer",
]
