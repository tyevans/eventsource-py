"""Unit tests for `stop()` interrupting a drain that is blocked on `pause` (task #25).

Contract: `stop()` exits a paused drain without requiring `resume()`, and a
stopped-while-paused subscription still reports as paused (stopping is not an
implicit operator resume).
"""

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
from eventsource.application.subscriptions.runners import CatchUpRunner, LiveRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

_REGISTRY = EventRegistry()
BOUND = 5.0


@register_event(registry=_REGISTRY)
class StopTestEvent(DomainEvent):
    """Simple test event for stop/pause interaction tests."""

    aggregate_type: str = "StopAggregate"
    data: str = "test"


class GatedSubscriber:
    """Blocks inside `handle()` until released so tests catch the runner mid-drain."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.handled: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [StopTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.entered.set()
        await self.release.wait()
        self.handled.append(event)


@pytest.fixture
def event_store() -> InMemoryEventStore:
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    return InMemoryEventBus()


async def add_events(store: InMemoryEventStore, count: int) -> None:
    for i in range(count):
        aggregate_id = uuid4()
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="StopAggregate"),
            [StopTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")],
            ExpectedVersion.no_stream(),
        )


async def current(store: InMemoryEventStore) -> Position:
    position = await store.current_position()
    assert position is not None
    return position


class TestCatchUpStopWhilePaused:
    async def test_stop_interrupts_a_paused_drain(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A catch-up run parked in `wait_if_paused()` exits when stopped."""
        subscriber = GatedSubscriber()
        config = SubscriptionConfig(
            batch_size=1,
            processing_timeout=BOUND * 10,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="paused-catchup", config=config, subscriber=subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        target = await current(event_store)
        run = asyncio.create_task(runner.run_until_position(target))

        await asyncio.wait_for(subscriber.entered.wait(), timeout=BOUND)
        await subscription.pause()
        subscriber.release.set()
        await asyncio.sleep(0.05)

        await asyncio.wait_for(runner.stop(), timeout=BOUND)
        result = await asyncio.wait_for(run, timeout=BOUND)
        assert not result.completed

    async def test_stop_while_paused_does_not_resume_the_subscription(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Stopping is not an implicit resume; subscription remains paused."""
        subscriber = GatedSubscriber()
        config = SubscriptionConfig(
            batch_size=1,
            processing_timeout=BOUND * 10,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="still-paused", config=config, subscriber=subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        run = asyncio.create_task(runner.run_until_position(await current(event_store)))
        await asyncio.wait_for(subscriber.entered.wait(), timeout=BOUND)
        await subscription.pause()
        subscriber.release.set()
        await asyncio.sleep(0.05)

        await asyncio.wait_for(runner.stop(), timeout=BOUND)
        await asyncio.wait_for(run, timeout=BOUND)
        assert subscription.is_paused


class TestLiveStopWhilePaused:
    async def test_stop_interrupts_a_paused_drain(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Live runner drain parked in `wait_if_paused()` exits when stopped."""
        subscriber = GatedSubscriber()
        config = SubscriptionConfig(
            batch_size=1,
            processing_timeout=BOUND * 10,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        )
        subscription = Subscription(name="paused-live", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()
        try:
            await add_events(event_store, 5)
            drain = asyncio.create_task(
                event_bus.publish([StopTestEvent(aggregate_id=uuid4(), data="__wake__")])
            )
            await asyncio.wait_for(subscriber.entered.wait(), timeout=BOUND)
            await subscription.pause()
            subscriber.release.set()
            await asyncio.sleep(0.05)

            await asyncio.wait_for(runner.stop(), timeout=BOUND)
            await asyncio.wait_for(drain, timeout=BOUND)
        finally:
            subscriber.release.set()

        assert subscription.is_paused
