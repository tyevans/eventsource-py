"""Unit tests for grouped `handle_batch()` dispatch on the live path (ADR 0063)."""

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
from eventsource.ports.positions import ExpectedVersion

_REGISTRY = EventRegistry()
BOUND = 5.0


@register_event(registry=_REGISTRY)
class LiveGroupEvent(DomainEvent):
    """Simple test event for live grouped-dispatch tests."""

    aggregate_type: str = "LiveGroupAggregate"
    data: str = "test"


class RecordingBatchSubscriber:
    """Records each `handle_batch()` call as a distinct list."""

    def __init__(self) -> None:
        self.batch_calls: list[list[DomainEvent]] = []
        self.saw_batch = asyncio.Event()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveGroupEvent]

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_calls.append(list(events))
        self.saw_batch.set()


class GatedBatchSubscriber:
    """Blocks inside `handle_batch()` until released."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.batch_calls: list[list[DomainEvent]] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveGroupEvent]

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.entered.set()
        await self.release.wait()
        self.batch_calls.append(list(events))


class HangingBatchSubscriber:
    """Never returns from `handle_batch()`."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveGroupEvent]

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.entered.set()
        await asyncio.sleep(BOUND * 10)


class RaisingBatchSubscriber:
    """`handle_batch()` raises; `handle()` records the per-event fallback."""

    def __init__(self) -> None:
        self.batch_attempts: list[int] = []
        self.single_calls: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveGroupEvent]

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_attempts.append(len(events))
        raise RuntimeError("batch handler exploded")

    async def handle(self, event: DomainEvent) -> None:
        self.single_calls.append(event)


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
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="LiveGroupAggregate")
        await store.append(
            stream, [LiveGroupEvent(aggregate_id=agg_id, data=f"e{i}")], ExpectedVersion.no_stream()
        )


def make_config(**overrides: object) -> SubscriptionConfig:
    kwargs: dict[str, object] = {
        "checkpoint_strategy": CheckpointStrategy.EVERY_EVENT,
        "continue_on_error": True,
        **overrides,
    }
    return SubscriptionConfig(**kwargs)  # type: ignore[arg-type]


def make_runner(
    event_bus: InMemoryEventBus,
    event_store: InMemoryEventStore,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscriber: object,
    name: str,
    config: SubscriptionConfig | None = None,
) -> LiveRunner:
    sub = Subscription(name=name, config=config or make_config(), subscriber=subscriber)
    return LiveRunner(
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        event_feed=event_store,
        subscription=sub,
    )


class TestLiveGroupedDispatch:
    async def test_available_events_are_delivered_as_one_batch(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """Three events already on the feed reach the subscriber in one `handle_batch()` call."""
        subscriber = RecordingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-grouped")
        await runner.start()
        try:
            await add_events(event_store, 3)
            await event_bus.publish([LiveGroupEvent(aggregate_id=uuid4(), data="__wake__")])
            await asyncio.wait_for(subscriber.saw_batch.wait(), timeout=BOUND)
        finally:
            await runner.stop()
        assert len(subscriber.batch_calls) == 1
        assert [e.data for e in subscriber.batch_calls[0]] == ["e0", "e1", "e2"]
        assert runner.stats.events_processed == 3
        assert runner.stats.events_failed == 0

    async def test_batch_is_a_page_not_an_accumulator(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """A lone event is dispatched immediately rather than waiting for more events."""
        subscriber = RecordingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-one")
        await runner.start()
        try:
            await add_events(event_store, 1)
            await event_bus.publish([LiveGroupEvent(aggregate_id=uuid4(), data="__wake__")])
            await asyncio.wait_for(subscriber.saw_batch.wait(), timeout=BOUND)
        finally:
            await runner.stop()
        assert len(subscriber.batch_calls) == 1
        assert len(subscriber.batch_calls[0]) == 1

    async def test_stop_before_dispatch_delivers_nothing(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """A drain entered after `stop()` dispatches no batch at all."""
        subscriber = RecordingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-stopped")
        await runner.start()
        await add_events(event_store, 3)
        await runner.stop()
        assert await runner._drain_feed() == 0
        assert subscriber.batch_calls == []

    async def test_stop_interrupts_a_paused_grouped_drain(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """A drain parked in `wait_if_paused()` mid-scan is released by `stop()`."""
        subscriber = RecordingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-paused")
        await runner.start()
        await add_events(event_store, 3)
        await runner.subscription.pause()
        drain = asyncio.create_task(runner._drain_feed())
        await asyncio.sleep(0)
        assert not drain.done()
        await runner.stop()
        await asyncio.wait_for(drain, timeout=BOUND)
        assert subscriber.batch_calls == []
        assert runner.subscription.is_paused

    async def test_processing_timeout_bounds_the_batch_call(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """`processing_timeout` bounds `handle_batch()` so a hung batch handler cannot wedge."""
        subscriber = HangingBatchSubscriber()
        cfg = make_config(processing_timeout=0.05)
        runner = make_runner(
            event_bus, event_store, checkpoint_repo, subscriber, "live-timeout", cfg
        )
        await runner.start()
        try:
            await add_events(event_store, 3)
            await asyncio.wait_for(runner._drain_feed(), timeout=BOUND)
        finally:
            await runner.stop()
        assert subscriber.entered.is_set()
        assert runner.stats.events_failed == 3

    async def test_raising_batch_handler_falls_back_to_per_event(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """A `handle_batch()` that raises falls back to per-event delivery."""
        subscriber = RaisingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-raise")
        await runner.start()
        try:
            await add_events(event_store, 3)
            await asyncio.wait_for(runner._drain_feed(), timeout=BOUND)
        finally:
            await runner.stop()
        assert subscriber.batch_attempts == [3]
        assert [e.data for e in subscriber.single_calls] == ["e0", "e1", "e2"]
        assert runner.stats.events_processed == 3

    async def test_grouped_drain_checkpoints_the_page(
        self, event_bus, event_store, checkpoint_repo
    ) -> None:
        """`EVERY_BATCH` checkpoints at the last envelope after the batch completes."""
        subscriber = GatedBatchSubscriber()
        cfg = make_config(checkpoint_strategy=CheckpointStrategy.EVERY_BATCH)
        runner = make_runner(
            event_bus, event_store, checkpoint_repo, subscriber, "live-checkpoint", cfg
        )
        await runner.start()
        await add_events(event_store, 3)
        drain = asyncio.create_task(runner._drain_feed())
        await asyncio.wait_for(subscriber.entered.wait(), timeout=BOUND)
        assert await checkpoint_repo.get_position("live-checkpoint") is None
        subscriber.release.set()
        await asyncio.wait_for(drain, timeout=BOUND)
        await runner.stop()
        assert len(subscriber.batch_calls) == 1
        assert await checkpoint_repo.get_position("live-checkpoint") is not None
