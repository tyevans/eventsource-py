"""Tracer protocol and span kind enumeration."""

from __future__ import annotations

from contextlib import AbstractContextManager
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from opentelemetry.trace import Span


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
            name: Span name (e.g., "eventsource.snapshot.save")
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


__all__ = [
    "SpanKindEnum",
    "Tracer",
]
