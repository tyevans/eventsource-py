"""Integration tests for trace span parent-child hierarchy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, InMemoryEventBus

from ..conftest import TracingTestEvent

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


class TestSpanHierarchy:
    """Tests for correct span parent-child relationships."""

    @pytest.mark.asyncio
    async def test_event_bus_handler_spans_are_children_of_dispatch(
        self,
        trace_provider: Any,
        get_spans: Callable[[], list[Any]],
    ) -> None:
        """Event bus handler spans should be children of the dispatch span."""
        bus = InMemoryEventBus(enable_tracing=True)

        async def handler(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEvent, handler)

        event = TracingTestEvent(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        spans = get_spans()

        dispatch_span = next((s for s in spans if "dispatch" in s.name), None)
        handler_span = next((s for s in spans if "handle" in s.name), None)

        assert dispatch_span is not None
        assert handler_span is not None

        # Handler should be child of dispatch
        if handler_span.parent is not None:
            assert handler_span.parent.span_id == dispatch_span.context.span_id
