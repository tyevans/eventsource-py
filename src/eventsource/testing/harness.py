"""
Test harness providing pre-configured in-memory infrastructure.

This module provides an InMemoryTestHarness class that bundles all the in-memory
implementations needed for testing event-sourced applications. It eliminates
boilerplate setup code and ensures consistent configuration across tests.

Example:
    >>> from eventsource.testing import InMemoryTestHarness
    >>>
    >>> async def test_order_processing():
    ...     harness = InMemoryTestHarness()
    ...     repo = harness.create_repository(OrderAggregate)
    ...
    ...     # Create and save an aggregate
    ...     order = OrderAggregate.create(customer_id=customer_id)
    ...     await repo.save(order)
    ...
    ...     # Verify events were published
    ...     assert len(harness.published_events) == 1
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryReadModelRepository,
    InMemorySnapshotStore,
)
from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates import AggregateRepository
from eventsource.application.projections.replay import ReplayReport, replay
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.ports.handlers import EventSubscriber
from eventsource.ports.readmodels.model import ReadModel
from eventsource.testing.assertions import EventAssertions

TEvent = TypeVar("TEvent", bound=DomainEvent)
TAggregate = TypeVar("TAggregate", bound=AggregateRoot[Any])
TModel = TypeVar("TModel", bound=ReadModel)


class InMemoryTestHarness:
    """
    Pre-configured test infrastructure for event sourcing tests.

    Provides in-memory implementations of all infrastructure components,
    wired together and ready to use:
    - event_store: InMemoryEventStore for persisting events
    - event_bus: InMemoryEventBus for publishing/subscribing to events
    - checkpoint_repo: InMemoryCheckpointRepository for projection checkpoints
    - dlq_repo: InMemoryDLQRepository for dead letter queue
    - snapshot_store: InMemorySnapshotStore for aggregate state snapshots

    Thread Safety:
        The harness itself is not thread-safe, but the underlying
        components (InMemoryEventBus) have thread-safe operations.
        Use one harness per test to avoid cross-test contamination.
    """

    def __init__(self) -> None:
        """Initialize the test harness with fresh in-memory components (tracing disabled)."""
        self._event_store = InMemoryEventStore()
        self._event_bus = InMemoryEventBus(enable_tracing=False)
        self._checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
        self._dlq_repo = InMemoryDLQRepository(enable_tracing=False)
        self._snapshot_store = InMemorySnapshotStore(enable_tracing=False)

    @property
    def event_store(self) -> InMemoryEventStore:
        """Get the in-memory event store."""
        return self._event_store

    @property
    def event_bus(self) -> InMemoryEventBus:
        """Get the in-memory event bus."""
        return self._event_bus

    @property
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        """Get the in-memory checkpoint repository."""
        return self._checkpoint_repo

    @property
    def dlq_repo(self) -> InMemoryDLQRepository:
        """Get the in-memory dead letter queue repository."""
        return self._dlq_repo

    @property
    def snapshot_store(self) -> InMemorySnapshotStore:
        """Get the in-memory snapshot store."""
        return self._snapshot_store

    @property
    def assertions(self) -> EventAssertions:
        """Get an EventAssertions helper bound to currently published events."""
        return EventAssertions(self.published_events)

    @property
    def published_events(self) -> list[DomainEvent]:
        """Get all events published through the event bus in publication order."""
        return self._event_bus.published_events

    async def append_events(
        self,
        *events: DomainEvent,
        expected_version: ExpectedVersion | None = None,
    ) -> None:
        """Group events by aggregate stream and append them to the event store."""
        if not events:
            return
        by_aggregate: dict[tuple[UUID, str], list[DomainEvent]] = {}
        for event in events:
            key = (event.aggregate_id, event.aggregate_type)
            by_aggregate.setdefault(key, []).append(event)
        version_constraint = expected_version or ExpectedVersion.any_()
        for (agg_id, agg_type), agg_events in by_aggregate.items():
            await self._event_store.append(
                StreamId(aggregate_id=agg_id, category=agg_type),
                agg_events,
                version_constraint,
            )

    async def replay(
        self,
        projections: Sequence[EventSubscriber] | EventSubscriber,
        **kwargs: Any,
    ) -> ReplayReport:
        """Replay events from the harness event store into one or more projections."""
        return await replay(self._event_store, projections, **kwargs)

    def create_repository(
        self,
        aggregate_factory: type[TAggregate],
        *,
        snapshot_threshold: int | None = None,
        with_snapshots: bool = False,
    ) -> AggregateRepository[TAggregate]:
        """Create an AggregateRepository pre-wired to the harness store, bus, and optional snapshot store."""
        return AggregateRepository(
            event_store=self._event_store,
            aggregate_factory=aggregate_factory,
            event_publisher=self._event_bus,
            snapshot_store=self._snapshot_store
            if (with_snapshots or snapshot_threshold is not None)
            else None,
            snapshot_threshold=snapshot_threshold,
            enable_tracing=False,
        )

    def create_read_model_repository(
        self,
        model_class: type[TModel],
    ) -> InMemoryReadModelRepository[TModel]:
        """Create an InMemoryReadModelRepository with tracing disabled for tests."""
        return InMemoryReadModelRepository(model_class, enable_tracing=False)

    def reset(self) -> None:
        """Reset all state and create fresh in-memory components."""
        self._event_store = InMemoryEventStore()
        self._event_bus = InMemoryEventBus(enable_tracing=False)
        self._checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
        self._dlq_repo = InMemoryDLQRepository(enable_tracing=False)
        self._snapshot_store = InMemorySnapshotStore(enable_tracing=False)

    def clear_published_events(self) -> None:
        """Clear only the published events, keeping other state."""
        self._event_bus.clear_published_events()

    def published_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        """Return published events matching ``event_type`` in publication order."""
        return self._event_bus.published_of_type(event_type)

    def get_events_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        """Filter published events by type."""
        return self._event_bus.published_of_type(event_type)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"InMemoryTestHarness(published={len(self.published_events)})"


__all__ = ["InMemoryTestHarness"]
