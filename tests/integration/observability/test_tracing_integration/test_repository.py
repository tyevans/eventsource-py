"""Integration tests for repository tracing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.observability import ATTR_AGGREGATE_ID, ATTR_AGGREGATE_TYPE

from .aggregate import TracingTestAggregate

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


class TestRepositoryTracing:
    """Integration tests for repository tracing."""

    @pytest.mark.asyncio
    async def test_repository_save_creates_span(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
    ) -> None:
        """AggregateRepository.save creates a span."""
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
        assert span is not None, "repository.save should create a span"

        attributes = dict(span.attributes)
        assert ATTR_AGGREGATE_ID in attributes
        assert ATTR_AGGREGATE_TYPE in attributes

    @pytest.mark.asyncio
    async def test_repository_load_creates_span(
        self,
        trace_provider: Any,
        trace_exporter: Any,
        find_span: Callable[[str], Any | None],
    ) -> None:
        """AggregateRepository.load creates a span."""
        store = InMemoryEventStore()
        repo = AggregateRepository(  # type: ignore[type-var]
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=True,
        )

        # First save an aggregate
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")
        await repo.save(aggregate)

        # Clear exporter to isolate load span
        trace_exporter.clear()

        # Load the aggregate
        await repo.load(aggregate_id)

        span = find_span("repository.load")
        assert span is not None, "repository.load should create a span"
