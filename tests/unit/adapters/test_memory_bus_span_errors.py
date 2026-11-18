"""A failing handler's exception never reaches exported span events with payload values.

The bus is driven through its public ``publish`` path. The handler validates the
event's payload with pydantic, so the raised ``ValidationError`` echoes the marker
value. The assertion reads only the finished spans the bus emitted.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from eventsource.observability import SpanKindEnum

MARKER = "victim-secret@example.com"


class AccountOpened(DomainEvent):
    aggregate_type: str = "Account"
    owner_ref: str


class _Owner(BaseModel):
    owner_id: UUID


class _IsolatedOtelTracer:
    """A public ``Tracer`` bound to a private TracerProvider."""

    def __init__(self, provider: TracerProvider) -> None:
        self._tracer = provider.get_tracer("test")

    @property
    def enabled(self) -> bool:
        return True

    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        return self._tracer.start_as_current_span(name, attributes=attributes or {})

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Any:
        return self._tracer.start_span(name, attributes=attributes or {})

    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Any:
        return self.span(name, attributes)


@pytest.fixture
def otel() -> Iterator[tuple[_IsolatedOtelTracer, InMemorySpanExporter]]:
    provider = TracerProvider()
    spans = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    yield _IsolatedOtelTracer(provider), spans
    provider.shutdown()


async def test_failed_dispatch_keeps_payload_marker_out_of_span_events(
    otel: tuple[_IsolatedOtelTracer, InMemorySpanExporter],
) -> None:
    tracer, spans = otel
    bus = InMemoryEventBus(tracer=tracer)  # type: ignore[arg-type]

    async def handler(event: AccountOpened) -> None:
        _Owner.model_validate({"owner_id": event.owner_ref})

    bus.subscribe(AccountOpened, handler)  # type: ignore[arg-type]
    await bus.publish([AccountOpened(aggregate_id=UUID(int=1), owner_ref=MARKER)])

    assert bus.get_stats()["handler_errors"] == 1
    finished = spans.get_finished_spans()
    recorded = [event for span in finished for event in span.events if event.name == "exception"]
    assert recorded, "the failed handler should record an exception event"
    for span in finished:
        assert MARKER not in str(span.status.description)
        for event in span.events:
            assert MARKER not in str(event.attributes)
            assert "exception.stacktrace" not in (event.attributes or {})
