"""Integration tests for trace ID consistency across components."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository

from ..conftest import TracingTestEventCreated
from .aggregate import TracingTestAggregate

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventsource import DomainEvent

pytestmark = [pytest.mark.integration]


class TestTraceIdConsistency:
    """Tests for trace ID consistency across operations."""

    @pytest.mark.asyncio
    async def test_all_spans_in_operation_share_trace_id(
        self,
        trace_provider: Any,
        get_spans: Callable[[], list[Any]],
    ) -> None:
        """All spans created during a single logical operation share the same trace ID."""
        store = InMemoryEventStore()
        bus = InMemoryEventBus(enable_tracing=True)
        repo = AggregateRepository(  # type: ignore[type-var]
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            event_publisher=bus,
            enable_tracing=True,
        )

        # Register a handler
        async def handler(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEventCreated, handler)

        # Perform a save which should:
        # 1. Create repository.save span
        # 2. Publish event, creating bus dispatch/handle spans
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        spans = get_spans()

        # Should have multiple spans
        assert len(spans) >= 2, f"Should have multiple spans, got {len(spans)}"

        # All spans should share the same trace ID
        trace_ids = {span.context.trace_id for span in spans}
        assert len(trace_ids) == 1, (
            f"All spans should share the same trace ID, but found {len(trace_ids)} different IDs"
        )
