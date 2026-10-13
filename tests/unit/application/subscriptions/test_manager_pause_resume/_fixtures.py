"""Shared fixtures and test helpers for manager pause/resume tests."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    SubscriptionManager,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion

# --- Sample Event Classes ---

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class PauseTestEvent(DomainEvent):
    """Simple test event for pause testing."""

    aggregate_type: str = "PauseAggregate"
    data: str = "test"


# --- Mock Subscribers ---


class MockSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self, name: str | None = None) -> None:
        self.handled_events: list[DomainEvent] = []
        self._name = name

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [PauseTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class OrderProjection(MockSubscriber):
    """Named projection for testing."""

    pass


class CustomerProjection(MockSubscriber):
    """Another named projection for testing."""

    pass


# --- Fixtures ---


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore (a real GlobalEventFeed)."""
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh InMemoryEventBus."""
    return InMemoryEventBus(enable_tracing=False)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def manager(
    event_store: InMemoryEventStore,
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
) -> SubscriptionManager:
    """Create a SubscriptionManager."""
    return SubscriptionManager(event_store, event_bus, checkpoint_repo)


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = PauseTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="PauseAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events
