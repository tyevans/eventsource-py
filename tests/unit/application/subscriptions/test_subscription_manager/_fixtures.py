"""Shared fixtures and test helpers for SubscriptionManager tests."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    SubscriptionConfig,
    SubscriptionManager,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion

# --- Sample Event Classes ---

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class ManagerTestEvent(DomainEvent):
    """Simple test event for manager testing."""

    aggregate_type: str = "ManagerAggregate"
    data: str = "test"


@register_event(registry=_REGISTRY)
class AnotherManagerEvent(DomainEvent):
    """Another test event type."""

    aggregate_type: str = "ManagerAggregate"
    value: int = 0


# --- Mock Subscribers ---


class MockSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self, name: str | None = None) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()
        self.handle_delay: float = 0.0
        self._name = name

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [ManagerTestEvent, AnotherManagerEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.handle_delay > 0:
            await asyncio.sleep(self.handle_delay)
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
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


@pytest.fixture
def subscriber() -> MockSubscriber:
    """Create a test subscriber."""
    return MockSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = ManagerTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="ManagerAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events
