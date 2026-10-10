"""Integration tests for standard trace attributes propagation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
)

from ..conftest import TracingTestEvent
from .aggregate import TracingTestAggregate

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


class TestStandardAttributes:
    """Tests for standard attribute usage across components."""

    @pytest.mark.asyncio
    async def test_event_bus_uses_standard_attributes(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
    ) -> None:
        """Event bus spans use standard ATTR_* attribute keys."""
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

        dispatch_span = find_span("dispatch")
        handler_span = find_span("handle")

        assert dispatch_span is not None
        assert handler_span is not None

        # Dispatch span should have event type
        dispatch_attrs = dict(dispatch_span.attributes)
        assert ATTR_EVENT_TYPE in dispatch_attrs

        # Handler span should have handler name
        handler_attrs = dict(handler_span.attributes)
        assert ATTR_HANDLER_NAME in handler_attrs

    @pytest.mark.asyncio
    async def test_repository_uses_standard_attributes(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
    ) -> None:
        """Repository spans use standard ATTR_* attribute keys."""
        store = InMemoryEventStore()
        repo = AggregateRepository(  # type: ignore[type-var]
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=True,
        )

        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        span = find_span("repository.save")
        assert span is not None

        attributes = dict(span.attributes)
        assert ATTR_AGGREGATE_ID in attributes
        assert ATTR_AGGREGATE_TYPE in attributes
