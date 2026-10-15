"""Unit tests for grouped `handle_batch()` dispatch on the live path (ADR 0063)."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryEventBus,
    InMemoryEventStore,
)
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import LiveRunner
from eventsource.domain import DomainEvent, EventRegistry, StreamId, register_event
from eventsource.ports.positions import ExpectedVersion

_REGISTRY, BOUND = EventRegistry(), 5.0


@register_event(registry=_REGISTRY)
class LiveGroupEvent(DomainEvent):
    aggregate_type: str = "LiveGroupAggregate"
    data: str = "test"


class _BaseBatchSub:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveGroupEvent]


class RecordingBatchSubscriber(_BaseBatchSub):
    def __init__(self) -> None:
        self.batch_calls, self.saw_batch = [], asyncio.Event()

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_calls.append(list(events))
        self.saw_batch.set()


class GatedBatchSubscriber(_BaseBatchSub):
    def __init__(self) -> None:
        self.entered, self.release, self.batch_calls = asyncio.Event(), asyncio.Event(), []

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.entered.set()
        await self.release.wait()
        self.batch_calls.append(list(events))


class HangingBatchSubscriber(_BaseBatchSub):
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.entered.set()
        await asyncio.sleep(BOUND * 10)


class RaisingBatchSubscriber(_BaseBatchSub):
    def __init__(self) -> None:
        self.batch_attempts, self.single_calls = [], []

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
    kw = {"checkpoint_strategy": CheckpointStrategy.EVERY_EVENT, "continue_on_error": True}
    return SubscriptionConfig(**(kw | overrides))  # type: ignore[arg-type]


def make_runner(bus, store, repo, sub, name: str, cfg=None) -> LiveRunner:
    s = Subscription(name=name, config=cfg or make_config(), subscriber=sub)
    return LiveRunner(event_bus=bus, checkpoint_repo=repo, event_feed=store, subscription=s)


async def _start_with_events(bus, store, repo, sub, name: str, n: int = 3, cfg=None) -> LiveRunner:
    r = make_runner(bus, store, repo, sub, name, cfg)
    await r.start()
    await add_events(store, n)
    return r


async def _run_and_stop(bus, store, repo, sub, name: str, n: int, wake: bool = False, cfg=None):
    r = await _start_with_events(bus, store, repo, sub, name, n, cfg)
    try:
        if wake:
            await bus.publish([LiveGroupEvent(aggregate_id=uuid4(), data="__wake__")])
            await asyncio.wait_for(sub.saw_batch.wait(), timeout=BOUND)
        else:
            await asyncio.wait_for(r._drain_feed(), timeout=BOUND)
    finally:
        await r.stop()
    return r


class TestLiveGroupedDispatch:
    @pytest.fixture(autouse=True)
    def _setup(self, event_bus, event_store, checkpoint_repo) -> None:
        self.bus, self.store, self.repo = event_bus, event_store, checkpoint_repo
        self.deps = (event_bus, event_store, checkpoint_repo)

    async def test_available_events_are_delivered_as_one_batch(self) -> None:
        sub = RecordingBatchSubscriber()
        r = await _run_and_stop(*self.deps, sub, "live-grouped", 3, True)
        assert [[e.data for e in b] for b in sub.batch_calls] == [["e0", "e1", "e2"]]
        assert (r.stats.events_processed, r.stats.events_failed) == (3, 0)

    async def test_batch_is_a_page_not_an_accumulator(self) -> None:
        sub = RecordingBatchSubscriber()
        await _run_and_stop(*self.deps, sub, "live-one", 1, True)
        assert len(sub.batch_calls) == 1 and len(sub.batch_calls[0]) == 1

    async def test_stop_before_dispatch_delivers_nothing(self) -> None:
        sub = RecordingBatchSubscriber()
        r = await _start_with_events(*self.deps, sub, "live-stopped")
        await r.stop()
        assert await r._drain_feed() == 0 and sub.batch_calls == []

    async def test_stop_interrupts_a_paused_grouped_drain(self) -> None:
        sub = RecordingBatchSubscriber()
        r = await _start_with_events(*self.deps, sub, "live-paused")
        await r.subscription.pause()
        drain = asyncio.create_task(r._drain_feed())
        await asyncio.sleep(0)
        assert not drain.done()
        await r.stop()
        await asyncio.wait_for(drain, timeout=BOUND)
        assert sub.batch_calls == [] and r.subscription.is_paused

    async def test_processing_timeout_bounds_the_batch_call(self) -> None:
        sub, cfg = HangingBatchSubscriber(), make_config(processing_timeout=0.05)
        r = await _run_and_stop(*self.deps, sub, "live-timeout", 3, cfg=cfg)
        assert sub.entered.is_set() and r.stats.events_failed == 3

    async def test_raising_batch_handler_falls_back_to_per_event(self) -> None:
        sub = RaisingBatchSubscriber()
        r = await _run_and_stop(*self.deps, sub, "live-raise", 3)
        assert sub.batch_attempts == [3] and r.stats.events_processed == 3
        assert [e.data for e in sub.single_calls] == ["e0", "e1", "e2"]

    async def test_grouped_drain_checkpoints_the_page(self) -> None:
        sub = GatedBatchSubscriber()
        cfg = make_config(checkpoint_strategy=CheckpointStrategy.EVERY_BATCH)
        r = await _start_with_events(*self.deps, sub, "live-checkpoint", cfg=cfg)
        drain = asyncio.create_task(r._drain_feed())
        await asyncio.wait_for(sub.entered.wait(), timeout=BOUND)
        assert await self.repo.get_position("live-checkpoint") is None
        sub.release.set()
        await asyncio.wait_for(drain, timeout=BOUND)
        await r.stop()
        assert len(sub.batch_calls) == 1
        assert await self.repo.get_position("live-checkpoint") is not None
