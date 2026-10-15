"""Unit tests for `SubscriptionConfig.processing_timeout` enforcement."""

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
HANG_SECONDS = 30.0
SHORT_TIMEOUT = 0.05


@register_event(registry=_REGISTRY)
class TimeoutTestEvent(DomainEvent):
    aggregate_type: str = "TimeoutAggregate"
    data: str = "test"


class _BaseTimeoutSubscriber:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TimeoutTestEvent]


class HangingSubscriber(_BaseTimeoutSubscriber):
    def __init__(self) -> None:
        self.handle_started = asyncio.Event()
        self.completed_normally = False

    async def handle(self, event: DomainEvent) -> None:
        self.handle_started.set()
        await asyncio.sleep(HANG_SECONDS)
        self.completed_normally = True


class HangingBatchSubscriber(_BaseTimeoutSubscriber):
    def __init__(self) -> None:
        self.batch_started = asyncio.Event()
        self.completed_normally = False
        self.single_calls: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.single_calls.append(event)

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_started.set()
        await asyncio.sleep(HANG_SECONDS)
        self.completed_normally = True


class SlowButFineSubscriber(_BaseTimeoutSubscriber):
    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.handled: list[DomainEvent] = []

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
        aid = uuid4()
        event = TimeoutTestEvent(aggregate_id=aid, data=f"event_{i}")
        stream = StreamId(aggregate_id=aid, category="TimeoutAggregate")
        await store.append(stream, [event], ExpectedVersion.no_stream())
        events.append(event)
    return events


async def wake(bus: InMemoryEventBus) -> None:
    await bus.publish([TimeoutTestEvent(aggregate_id=uuid4(), data="__wake__")])


async def current(store: InMemoryEventStore) -> Position:
    position = await store.current_position()
    assert position is not None
    return position


def _make_catchup_sub(
    name: str,
    subscriber: object,
    *,
    timeout: float = SHORT_TIMEOUT,
    continue_on_error: bool = True,
) -> Subscription:
    config = SubscriptionConfig(
        batch_size=10,
        processing_timeout=timeout,
        continue_on_error=continue_on_error,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )
    return Subscription(name=name, config=config, subscriber=subscriber)  # type: ignore[arg-type]


class TestCatchUpRunnerTimeout:
    async def test_hanging_handle_batch_times_out(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        sub = HangingBatchSubscriber()
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, _make_catchup_sub("timeout-sub", sub))
        async with asyncio.timeout(10.0):
            await runner.run_until_position(await current(event_store))
        assert sub.batch_started.is_set() and not sub.completed_normally

    async def test_hanging_handle_times_out(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        sub = HangingSubscriber()
        await add_events(event_store, 2)
        runner = CatchUpRunner(event_store, checkpoint_repo, _make_catchup_sub("timeout-sub", sub))
        async with asyncio.timeout(10.0):
            await runner.run_until_position(await current(event_store))
        assert sub.handle_started.is_set() and not sub.completed_normally

    async def test_handler_inside_budget_is_untouched(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        sub = SlowButFineSubscriber(delay=0.01)
        await add_events(event_store, 3)
        sub_obj = _make_catchup_sub("ok-sub", sub, timeout=5.0)
        result = await CatchUpRunner(event_store, checkpoint_repo, sub_obj).run_until_position(
            await current(event_store)
        )
        assert result.completed and len(sub.handled) == 3

    async def test_timeout_is_a_handler_failure_and_stops_when_told(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        sub = HangingSubscriber()
        await add_events(event_store, 2)
        sub_obj = _make_catchup_sub("strict-sub", sub, continue_on_error=False)
        async with asyncio.timeout(10.0):
            result = await CatchUpRunner(event_store, checkpoint_repo, sub_obj).run_until_position(
                await current(event_store)
            )
        assert not result.completed and not sub.completed_normally

    async def test_timeout_does_not_checkpoint_past_the_hung_event(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        sub = HangingSubscriber()
        await add_events(event_store, 2)
        sub_obj = _make_catchup_sub("nocheckpoint-sub", sub, continue_on_error=False)
        async with asyncio.timeout(10.0):
            await CatchUpRunner(event_store, checkpoint_repo, sub_obj).run_until_position(
                await current(event_store)
            )
        assert await checkpoint_repo.get_position(subscription_id="nocheckpoint-sub") is None


class TestLiveRunnerTimeout:
    async def test_hanging_handle_times_out(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
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
                await asyncio.sleep(SHORT_TIMEOUT * 4)
            assert not subscriber.completed_normally
        finally:
            async with asyncio.timeout(10.0):
                await runner.stop()
