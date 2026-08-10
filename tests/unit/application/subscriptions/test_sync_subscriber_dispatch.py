"""
Unit tests for `SyncSubscriber` dispatch in both runners.

Covers task #28. `SyncSubscriber` is a published Protocol whose `handle()` is
declared **not** async:

    def handle(self, event: DomainEvent) -> None: ...

Both runners awaited the result of `subscriber.handle(event)` unconditionally.
A sync handler returns `None`, and `await None` raises
`TypeError: object NoneType can't be used in 'await' expression`.

That `TypeError` was raised *inside* the runner's generic `except Exception`
around handler dispatch, so it was recorded as a handler failure and attributed
to user code. The subscriber's own body ran fine every time -- the failure was
the runner awaiting its return value. Under `continue_on_error=True` every
event "failed" forever while the handler was in fact working, and under
`continue_on_error=False` the subscription stopped on the first event. Either
way the reported cause pointed at the user's handler.

These tests assert from the published Protocol: a subscriber matching
`SyncSubscriber` is dispatched to, its side effects happen, and it is not
recorded as a failure.
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
from eventsource.ports.subscribers import SyncSubscriber

_REGISTRY = EventRegistry()

BOUND = 5.0


@register_event(registry=_REGISTRY)
class SyncTestEvent(DomainEvent):
    """Simple test event for sync-subscriber dispatch tests."""

    aggregate_type: str = "SyncAggregate"
    data: str = "test"


class PlainSyncSubscriber:
    """Implements exactly `SyncSubscriber` -- `handle()` is NOT async.

    This is the shape the Protocol documents for in-memory state updates,
    metrics collection, and test mocks.
    """

    def __init__(self) -> None:
        self.handled: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SyncTestEvent]

    def handle(self, event: DomainEvent) -> None:
        self.handled.append(event)


class AsyncSubscriber:
    """The control: an ordinary async subscriber must be unaffected."""

    def __init__(self) -> None:
        self.handled: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SyncTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.handled.append(event)


class RaisingSyncSubscriber:
    """A sync handler that genuinely fails, to prove failures are still
    reported -- the fix must not swallow real errors along with the phantom
    TypeError."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SyncTestEvent]

    def handle(self, event: DomainEvent) -> None:
        raise ValueError("genuine handler failure")


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
            StreamId(aggregate_id=aggregate_id, category="SyncAggregate"),
            [SyncTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")],
            ExpectedVersion.no_stream(),
        )


async def current(store: InMemoryEventStore) -> Position:
    position = await store.current_position()
    assert position is not None
    return position


def make_config(**kwargs: object) -> SubscriptionConfig:
    base: dict[str, object] = {
        "batch_size": 10,
        "checkpoint_strategy": CheckpointStrategy.EVERY_BATCH,
    }
    base.update(kwargs)
    return SubscriptionConfig(**base)  # type: ignore[arg-type]


class TestProtocolShape:
    def test_plain_sync_subscriber_satisfies_the_protocol(self) -> None:
        """Guards the premise of every other test here: if this stops being a
        SyncSubscriber, these tests are no longer about the published contract.
        """
        assert isinstance(PlainSyncSubscriber(), SyncSubscriber)


class TestCatchUpSyncDispatch:
    async def test_sync_subscriber_receives_events(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A sync handler's side effects actually happen on catch-up."""
        subscriber = PlainSyncSubscriber()
        subscription = Subscription(
            name="sync-catchup", config=make_config(), subscriber=subscriber
        )
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert len(subscriber.handled) == 3

    async def test_sync_subscriber_is_not_recorded_as_a_failure(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The assertion that separates a real fix from one that merely stops
        the exception escaping: the handler ran AND nothing was counted as a
        failure. Before the fix, `handled` filled up and every event was still
        recorded as failed."""
        subscriber = PlainSyncSubscriber()
        subscription = Subscription(name="sync-nofail", config=make_config(), subscriber=subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.events_processed == 3
        assert subscription.events_failed == 0

    async def test_sync_subscriber_stops_subscription_when_strict(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """With continue_on_error=False a *working* sync handler must not stop
        the subscription. This is the sharpest form of the bug: the phantom
        TypeError killed subscriptions whose handlers were fine."""
        subscriber = PlainSyncSubscriber()
        subscription = Subscription(
            name="sync-strict",
            config=make_config(continue_on_error=False),
            subscriber=subscriber,
        )
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.error is None
        assert len(subscriber.handled) == 3

    async def test_genuine_sync_failure_is_still_reported(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The fix must not swallow real errors from sync handlers."""
        subscription = Subscription(
            name="sync-raises",
            config=make_config(continue_on_error=True),
            subscriber=RaisingSyncSubscriber(),
        )
        await add_events(event_store, 2)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        await runner.run_until_position(await current(event_store))

        assert subscription.events_failed == 2

    async def test_async_subscriber_unaffected(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Control -- the ordinary async path must not change."""
        subscriber = AsyncSubscriber()
        subscription = Subscription(
            name="async-control", config=make_config(), subscriber=subscriber
        )
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert len(subscriber.handled) == 3


class TestLiveSyncDispatch:
    async def test_sync_subscriber_receives_live_events(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The live path dispatches to a sync handler too, and does not record
        it as a failure."""
        subscriber = PlainSyncSubscriber()
        subscription = Subscription(
            name="sync-live",
            config=make_config(checkpoint_strategy=CheckpointStrategy.EVERY_EVENT),
            subscriber=subscriber,
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()
        try:
            await add_events(event_store, 1)
            async with asyncio.timeout(BOUND):
                await event_bus.publish([SyncTestEvent(aggregate_id=uuid4(), data="__wake__")])
        finally:
            await runner.stop()

        assert len(subscriber.handled) == 1
        assert runner.stats.events_failed == 0
