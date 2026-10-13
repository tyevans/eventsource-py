"""Shared fixtures, models, and helpers for LiveRunner tests."""

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
from eventsource.application.subscriptions.runners import LiveRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

# --- Sample Event Classes ---

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class LiveTestEvent(DomainEvent):
    """Simple test event for live processing."""

    aggregate_type: str = "LiveAggregate"
    data: str = "test"


@register_event(registry=_REGISTRY)
class AnotherTestEvent(DomainEvent):
    """Another test event type."""

    aggregate_type: str = "LiveAggregate"
    value: int = 0


# --- Mock Subscriber ---


class MockLiveSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()
        self.handle_delay: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveTestEvent, AnotherTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.handle_delay > 0:
            await asyncio.sleep(self.handle_delay)
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


# --- Recording Feed ---


class _RecordingFeed:
    """Wraps a GlobalEventFeed and records the options passed to read_all."""

    def __init__(self, inner: InMemoryEventStore) -> None:
        self._inner = inner
        self.read_all_calls: list[tuple[Position | None, object]] = []

    async def read_all(self, from_position=None, options=None):
        self.read_all_calls.append((from_position, options))
        async for envelope in self._inner.read_all(from_position, options):
            yield envelope

    async def current_position(self):
        return await self._inner.current_position()


# --- Fixtures ---


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh InMemoryEventBus."""
    return InMemoryEventBus(enable_tracing=False)


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore (a real GlobalEventFeed)."""
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def subscriber() -> MockLiveSubscriber:
    """Create a test subscriber."""
    return MockLiveSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )


@pytest.fixture
def subscription(subscriber: MockLiveSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription."""
    return Subscription(
        name="LiveTestSubscription",
        config=config,
        subscriber=subscriber,
    )


@pytest.fixture
def runner(
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
    event_store: InMemoryEventStore,
    subscription: Subscription,
) -> LiveRunner:
    """Create a LiveRunner."""
    return LiveRunner(
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        event_feed=event_store,
        subscription=subscription,
    )


# --- Helpers ---


async def append_event(store: InMemoryEventStore, event: DomainEvent) -> Position:
    """Append one event directly to the store, returning its feed position."""
    result = await store.append(
        StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
        [event],
        ExpectedVersion.no_stream(),
    )
    assert result.position is not None
    return result.position


async def wake(bus: InMemoryEventBus) -> None:
    """Publish a bus notification to wake a live runner into draining the feed.

    The published event's content is never delivered -- only events already
    committed to the store (via `append_event`) are; this call exists purely
    to trigger `_handle_live_event`.
    """
    await bus.publish([LiveTestEvent(aggregate_id=uuid4(), data="__wake__")])
