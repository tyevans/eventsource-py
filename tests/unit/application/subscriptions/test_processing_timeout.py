"""
Unit tests for `SubscriptionConfig.processing_timeout` enforcement.

Covers task #13: the field was validated at construction and read *nowhere*.
Both runners awaited the subscriber's handler as a bare `await`, so a handler
that hung blocked its subscription forever -- no timeout, no error, no metric,
and no log line. The subscription simply stopped making progress.

These tests assert from the documented contract: `processing_timeout` bounds a
single handler call. There are exactly three handler dispatch sites --
`CatchUpRunner`'s `handle_batch()` and its per-event `handle()`, and
`LiveRunner`'s `handle()` -- and each must be bounded. A timed-out call is a
handler failure like any other, so `continue_on_error` governs what happens
next and the handler circuit breaker sees it.
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

# Long enough that a broken implementation visibly hangs rather than racing,
# short enough that the suite does not pay for it: every test here sets
# processing_timeout well below this.
HANG_SECONDS = 30.0

# The timeout each test configures. Small so an enforced timeout fires fast.
SHORT_TIMEOUT = 0.05


@register_event(registry=_REGISTRY)
class TimeoutTestEvent(DomainEvent):
    """Simple test event for processing-timeout tests."""

    aggregate_type: str = "TimeoutAggregate"
    data: str = "test"


class HangingSubscriber:
    """Hangs in `handle()` far longer than any configured timeout."""

    def __init__(self) -> None:
        self.handle_started = asyncio.Event()
        self.completed_normally = False

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TimeoutTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.handle_started.set()
        await asyncio.sleep(HANG_SECONDS)
        self.completed_normally = True


class HangingBatchSubscriber:
    """Hangs in `handle_batch()`, the other catch-up dispatch site."""

    def __init__(self) -> None:
        self.batch_started = asyncio.Event()
        self.completed_normally = False
        self.single_calls: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TimeoutTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.single_calls.append(event)

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_started.set()
        await asyncio.sleep(HANG_SECONDS)
        self.completed_normally = True


class SlowButFineSubscriber:
    """Takes real time, but finishes inside the configured budget."""

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.handled: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TimeoutTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        await asyncio.sleep(self.delay)
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


async def add_events(store: InMemoryEventStore, count: int) -> list[DomainEvent]:
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = TimeoutTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="TimeoutAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events


async def wake(bus: InMemoryEventBus) -> None:
    """Publish a bus notification to wake a live runner into draining the feed.

    The published event's content is never delivered -- only events already
    committed to the store are. Mirrors `test_live_runner.py`'s idiom.
    """
    await bus.publish([TimeoutTestEvent(aggregate_id=uuid4(), data="__wake__")])


async def current(store: InMemoryEventStore) -> Position:
    position = await store.current_position()
    assert position is not None
    return position


class TestCatchUpRunnerTimeout:
    """Both catch-up dispatch sites are bounded."""

    async def test_hanging_handle_batch_times_out(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A `handle_batch()` that hangs is abandoned at processing_timeout
        rather than blocking the runner forever."""
        subscriber = HangingBatchSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            processing_timeout=SHORT_TIMEOUT,
            continue_on_error=True,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="timeout-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        # The whole run must finish in well under HANG_SECONDS. Without
        # enforcement this outer bound is what fails, after hanging.
        async with asyncio.timeout(10.0):
            await runner.run_until_position(await current(event_store))

        assert subscriber.batch_started.is_set()
        assert not subscriber.completed_normally

    async def test_hanging_handle_times_out(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The per-event `handle()` path is bounded too."""
        subscriber = HangingSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            processing_timeout=SHORT_TIMEOUT,
            continue_on_error=True,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="timeout-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 2)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        async with asyncio.timeout(10.0):
            await runner.run_until_position(await current(event_store))

        assert subscriber.handle_started.is_set()
        assert not subscriber.completed_normally

    async def test_handler_inside_budget_is_untouched(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A handler slower than zero but faster than the budget still runs to
        completion -- the timeout bounds hangs, it does not police latency."""
        subscriber = SlowButFineSubscriber(delay=0.01)
        config = SubscriptionConfig(
            batch_size=10,
            processing_timeout=5.0,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="ok-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert len(subscriber.handled) == 3

    async def test_timeout_is_a_handler_failure_and_stops_when_told(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """With continue_on_error=False a timed-out handler stops the
        subscription, exactly as any other handler exception would -- the
        timeout is not a special quiet case."""
        subscriber = HangingSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            processing_timeout=SHORT_TIMEOUT,
            continue_on_error=False,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="strict-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 2)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        async with asyncio.timeout(10.0):
            result = await runner.run_until_position(await current(event_store))

        assert not result.completed
        assert not subscriber.completed_normally

    async def test_timeout_does_not_checkpoint_past_the_hung_event(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """continue_on_error=False must not advance the checkpoint past an
        event whose handler never completed -- otherwise the timeout converts
        a hang into silent data loss on restart."""
        subscriber = HangingSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            processing_timeout=SHORT_TIMEOUT,
            continue_on_error=False,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(name="nocheckpoint-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 2)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        async with asyncio.timeout(10.0):
            await runner.run_until_position(await current(event_store))

        checkpoint = await checkpoint_repo.get_position(subscription_id="nocheckpoint-sub")
        assert checkpoint is None


class TestLiveRunnerTimeout:
    """The live runner's single dispatch site is bounded."""

    async def test_hanging_handle_times_out(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A hung live handler does not wedge the subscription: the call is
        abandoned at processing_timeout and `stop()` still completes."""
        subscriber = HangingSubscriber()
        config = SubscriptionConfig(
            processing_timeout=SHORT_TIMEOUT,
            continue_on_error=True,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        )
        subscription = Subscription(name="live-timeout", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()
        try:
            await add_events(event_store, 1)
            await wake(event_bus)
            async with asyncio.timeout(10.0):
                await asyncio.wait_for(subscriber.handle_started.wait(), timeout=5.0)
                # Give the enforced timeout room to fire and the loop to move on.
                await asyncio.sleep(SHORT_TIMEOUT * 4)
            assert not subscriber.completed_normally
        finally:
            async with asyncio.timeout(10.0):
                await runner.stop()
