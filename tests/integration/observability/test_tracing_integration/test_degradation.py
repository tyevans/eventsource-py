"""Integration tests for tracing graceful degradation when disabled."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource import InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion, collect

from ..conftest import TracingTestEvent
from .aggregate import TracingTestAggregate

if TYPE_CHECKING:
    from eventsource import DomainEvent

pytestmark = [pytest.mark.integration]


class TestTracingGracefulDegradation:
    """Tests that tracing degrades gracefully when disabled."""

    @pytest.mark.asyncio
    async def test_event_store_works_without_tracing(
        self,
        sample_aggregate_id: UUID,
    ) -> None:
        """Event store operations work correctly when tracing is disabled."""
        store = InMemoryEventStore()

        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        stream = StreamId(aggregate_id=sample_aggregate_id, category="TracingTestAggregate")
        result = await store.append(stream, [event], ExpectedVersion.exact(0))

        assert result.position is not None

        events = await collect(store.read_stream(stream))
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_event_bus_works_without_tracing(self) -> None:
        """Event bus operations work correctly when tracing is disabled."""
        bus = InMemoryEventBus(enable_tracing=False)

        handled_events: list[TracingTestEvent] = []

        async def handler(event: DomainEvent) -> None:
            assert isinstance(event, TracingTestEvent)
            handled_events.append(event)

        bus.subscribe(TracingTestEvent, handler)

        event = TracingTestEvent(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        assert len(handled_events) == 1
        assert handled_events[0].name == "Test"

    @pytest.mark.asyncio
    async def test_repository_works_without_tracing(self) -> None:
        """Repository operations work correctly when tracing is disabled."""
        store = InMemoryEventStore()
        repo = AggregateRepository(  # type: ignore[type-var]
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        loaded = await repo.load(aggregate_id)
        assert loaded.state.name == "Test"
