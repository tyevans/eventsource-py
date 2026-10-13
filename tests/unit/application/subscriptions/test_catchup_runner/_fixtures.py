"""Shared fixtures, models, and helpers for CatchUpRunner tests."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import CatchUpRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

# --- Sample Event Classes ---

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class SampleTestEvent(DomainEvent):
    """Simple test event."""

    aggregate_type: str = "SampleAggregate"
    data: str = "test"


@register_event(registry=_REGISTRY)
class SampleFailingEvent(DomainEvent):
    """Event that should cause handler to fail."""

    aggregate_type: str = "SampleAggregate"
    should_fail: bool = True


# --- Mock Subscriber ---


class MockSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SampleTestEvent, SampleFailingEvent]

    async def handle(self, event: DomainEvent) -> None:
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


# --- Fixtures ---


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore (a real GlobalEventFeed)."""
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


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


@pytest.fixture
def subscription(subscriber: MockSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription."""
    return Subscription(
        name="TestSubscription",
        config=config,
        subscriber=subscriber,
    )


@pytest.fixture
def runner(
    event_store: InMemoryEventStore,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscription: Subscription,
) -> CatchUpRunner:
    """Create a CatchUpRunner."""
    return CatchUpRunner(event_store, checkpoint_repo, subscription)


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = SampleTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
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


async def position_after(store: InMemoryEventStore, n: int) -> Position:
    """The token of the nth (1-based) event in the feed."""
    envelopes = [e async for e in store.read_all()]
    position = envelopes[n - 1].position
    assert position is not None
    return position
