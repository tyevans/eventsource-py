"""Shared fixtures, models, and helpers for TransitionCoordinator tests."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

# --- Sample Event Classes ---

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class TransitionTestEvent(DomainEvent):
    """Simple test event for transition testing."""

    aggregate_type: str = "TransitionAggregate"
    data: str = "test"


@register_event(registry=_REGISTRY)
class AnotherTransitionEvent(DomainEvent):
    """Another test event type."""

    aggregate_type: str = "TransitionAggregate"
    value: int = 0


# --- Mock Subscriber ---


class MockTransitionSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()
        self.handle_delay: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TransitionTestEvent, AnotherTransitionEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.handle_delay > 0:
            await asyncio.sleep(self.handle_delay)
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


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
def subscriber() -> MockTransitionSubscriber:
    """Create a test subscriber."""
    return MockTransitionSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )


@pytest.fixture
def subscription(subscriber: MockTransitionSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription."""
    return Subscription(
        name="TransitionTestSubscription",
        config=config,
        subscriber=subscriber,
    )


@pytest.fixture
def coordinator(
    event_store: InMemoryEventStore,
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscription: Subscription,
) -> TransitionCoordinator:
    """Create a TransitionCoordinator."""
    return TransitionCoordinator(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        subscription=subscription,
    )


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = TransitionTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="TransitionAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events


async def current(store: InMemoryEventStore) -> Position:
    """The feed's current position, asserted present (the store is non-empty)."""
    position = await store.current_position()
    assert position is not None
    return position
