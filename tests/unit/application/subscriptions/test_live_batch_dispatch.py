"""
Unit tests for grouped `handle_batch()` dispatch on the **live** path.

Covers task #34 / ADR 0063. The catch-up runner has grouped its reads into
`handle_batch()` calls since ADR 0059; the live runner delivered one event per
call, so the same subscriber saw two different delivery shapes depending on
which runner happened to be driving it.

The decision recorded in ADR 0063 is that the live batch is **a page the feed
already returned, never an accumulator** -- nothing is held back waiting for
more events. These tests pin both halves of that: that a page really is
delivered as one call, and that the stop/pause/timeout guarantees PRs #137 and
#139 established are unchanged.
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
from eventsource.application.subscriptions.runners import LiveRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion

_REGISTRY = EventRegistry()

# Every wait here is bounded: a correct implementation finishes in
# milliseconds, and a regression becomes a fast failure instead of a hang.
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
    """Blocks inside `handle_batch()` until released, so a test can catch the
    runner mid-delivery rather than racing its loop."""

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
    """Never returns from `handle_batch()` -- what `processing_timeout` exists
    to bound."""

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
        aggregate_id = uuid4()
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="LiveGroupAggregate"),
            [LiveGroupEvent(aggregate_id=aggregate_id, data=f"e{i}")],
            ExpectedVersion.no_stream(),
        )


def make_config(**overrides: object) -> SubscriptionConfig:
    kwargs: dict[str, object] = {
        "checkpoint_strategy": CheckpointStrategy.EVERY_EVENT,
        "continue_on_error": True,
    }
    kwargs.update(overrides)
    return SubscriptionConfig(**kwargs)  # type: ignore[arg-type]


def make_runner(
    event_bus: InMemoryEventBus,
    event_store: InMemoryEventStore,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscriber: object,
    name: str,
    config: SubscriptionConfig | None = None,
) -> LiveRunner:
    subscription = Subscription(
        name=name,
        config=config or make_config(),
        subscriber=subscriber,
    )
    return LiveRunner(
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        event_feed=event_store,
        subscription=subscription,
    )


class TestLiveGroupedDispatch:
    async def test_available_events_are_delivered_as_one_batch(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The load-bearing assertion: three events already on the feed reach
        the subscriber in **one** `handle_batch()` call. Before ADR 0063 the
        live runner made three one-event calls."""
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
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A lone event is dispatched immediately rather than waiting for
        company. This is the property that makes ADR 0063 latency-neutral, and
        it is what a time-window accumulator would have broken."""
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
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Stop is still checked per envelope during the scan, so a drain
        entered after `stop()` dispatches no batch at all."""
        subscriber = RecordingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-stopped")
        await runner.start()
        await add_events(event_store, 3)
        await runner.stop()

        assert await runner._drain_feed() == 0
        assert subscriber.batch_calls == []

    async def test_stop_interrupts_a_paused_grouped_drain(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The PR #139 guarantee, on the grouped path: a drain parked in
        `wait_if_paused()` mid-scan is released by `stop()`, not only by
        `resume()`. The scan is where pause is observed, so grouping must not
        have moved the wait outside the stop-aware path."""
        subscriber = RecordingBatchSubscriber()
        runner = make_runner(event_bus, event_store, checkpoint_repo, subscriber, "live-paused")
        await runner.start()
        await add_events(event_store, 3)
        await runner.subscription.pause()

        drain = asyncio.create_task(runner._drain_feed())
        await asyncio.sleep(0)  # let the drain park in wait_if_paused
        assert not drain.done()

        await runner.stop()
        await asyncio.wait_for(drain, timeout=BOUND)

        assert subscriber.batch_calls == []
        # Stopping is not an implicit resume.
        assert runner.subscription.is_paused

    async def test_processing_timeout_bounds_the_batch_call(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The PR #137 guarantee, on the grouped path: `processing_timeout`
        bounds one `handle_batch()` exactly as it bounds one `handle()`. A
        hung batch handler must not wedge the subscription."""
        subscriber = HangingBatchSubscriber()
        runner = make_runner(
            event_bus,
            event_store,
            checkpoint_repo,
            subscriber,
            "live-timeout",
            config=make_config(processing_timeout=0.05),
        )
        await runner.start()
        try:
            await add_events(event_store, 3)
            await asyncio.wait_for(runner._drain_feed(), timeout=BOUND)
        finally:
            await runner.stop()

        assert subscriber.entered.is_set()
        # The timeout surfaced as an ordinary handler failure per event, via
        # the per-event fallback -- not as a hang.
        assert runner.stats.events_failed == 3

    async def test_raising_batch_handler_falls_back_to_per_event(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """ADR 0059's documented contract, matched by the live runner: a
        `handle_batch()` that raises falls back to per-event delivery rather
        than the runner inventing partial-batch semantics."""
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
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """`EVERY_BATCH` now has a batch to attach to on the live path, and
        the checkpoint lands at the last envelope of the page -- progress
        never outruns the completed batch."""
        subscriber = GatedBatchSubscriber()
        runner = make_runner(
            event_bus,
            event_store,
            checkpoint_repo,
            subscriber,
            "live-checkpoint",
            config=make_config(checkpoint_strategy=CheckpointStrategy.EVERY_BATCH),
        )
        await runner.start()
        await add_events(event_store, 3)

        drain = asyncio.create_task(runner._drain_feed())
        await asyncio.wait_for(subscriber.entered.wait(), timeout=BOUND)

        # Mid-batch: nothing is checkpointed yet, because the batch has not
        # settled. Progress never outruns completed work (ADR 0059).
        assert await checkpoint_repo.get_position("live-checkpoint") is None

        subscriber.release.set()
        await asyncio.wait_for(drain, timeout=BOUND)
        await runner.stop()

        assert len(subscriber.batch_calls) == 1
        assert await checkpoint_repo.get_position("live-checkpoint") is not None
