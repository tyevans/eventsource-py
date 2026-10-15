"""Tracer protocol and span kind enumeration."""

from __future__ import annotations

from contextlib import AbstractContextManager
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from opentelemetry.trace import Span


class SpanKindEnum(Enum):
    """Span kinds indicating the role a span plays in a distributed trace."""

    INTERNAL = "internal"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    CLIENT = "client"
    SERVER = "server"


@runtime_checkable
class Tracer(Protocol):
    """Protocol for tracers that create distributed tracing spans."""

    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> AbstractContextManager[Span | None]:
        """Create a tracing span context manager yielding a Span or None."""
        ...

    @property
    def enabled(self) -> bool:
        """Return True if tracing is active and will create real spans."""
        ...

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Span | None:
        """Start a span that must be manually ended by calling ``span.end()``."""
        ...

    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> AbstractContextManager[Span | None]:
        """Create a tracing span context manager with an explicit SpanKind."""
        ...


__all__ = [
    "SpanKindEnum",
    "Tracer",
]
