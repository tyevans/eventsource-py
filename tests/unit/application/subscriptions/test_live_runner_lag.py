"""Live-phase lag reports received-but-not-yet-delivered events.

`Subscription.lag` is `events_seen - events_delivered`, and its invariant
requires callers to keep the two symmetric across any boundary.

Before ADR 0047, the bus delivery receipt was the live seen-point, and the
live runner delivered the bus payload directly -- a design that also left
`_get_event_position` reading a `_position` attribute nothing ever set, so
live checkpointing never advanced. Now the bus is a wake-up signal only:
`LiveRunner` drains `GlobalEventFeed.read_all(from_position=...)` on each
wake-up, and it is the envelope pulled from the feed -- not the bus
notification -- that is the seen-point. Tests here seed a real
`InMemoryEventStore` and drain through `_handle_live_event`, mirroring
`test_live_runner.py`'s idiom.
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
from eventsource.ports.positions import ExpectedVersion

pytestmark = pytest.mark.asyncio


class LagTestEvent(DomainEvent):
    aggregate_type: str = "LagTestAggregate"


class BlockingSubscriber:
    """Blocks in `handle()` until released -- a stalled live subscriber."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.entered = asyncio.Event()
        self.handled: list[DomainEvent] = []
        self.fail = False

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LagTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.entered.set()
        await self.release.wait()
        if self.fail:
            raise ValueError("intentional failure")
        self.handled.append(event)


def _runner(
    subscriber: BlockingSubscriber, event_store: InMemoryEventStore, **config_kwargs
) -> LiveRunner:  # type: ignore[no-untyped-def]
    config = SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        **config_kwargs,
    )
    subscription = Subscription(name="LagSubscription", config=config, subscriber=subscriber)
    return LiveRunner(
        event_bus=InMemoryEventBus(enable_tracing=False),
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        event_feed=event_store,
        subscription=subscription,
    )


async def _committed_event(store: InMemoryEventStore) -> LagTestEvent:
    """A LagTestEvent already committed to the store, ready to be drained."""
    event = LagTestEvent(aggregate_id=uuid4())
    await store.append(
        StreamId(aggregate_id=event.aggregate_id, category="LagTestAggregate"),
        [event],
        ExpectedVersion.no_stream(),
    )
    return event


class TestLiveLagSignal:
    async def test_a_stalled_subscriber_shows_nonzero_lag(self) -> None:
        store = InMemoryEventStore()
        subscriber = BlockingSubscriber()
        runner = _runner(subscriber, store)

        events = [await _committed_event(store) for _ in range(3)]
        # A single drain reads all three envelopes and blocks delivering the
        # first, so one wake-up is enough to exercise the stall.
        task = asyncio.create_task(runner._handle_live_event(events[0]))
        await asyncio.wait_for(subscriber.entered.wait(), 1.0)

        assert runner.subscription.lag >= 1

        subscriber.release.set()
        await task

        assert runner.subscription.lag == 0

    async def test_seen_and_delivered_stay_symmetric(self) -> None:
        store = InMemoryEventStore()
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        runner = _runner(subscriber, store)

        for _ in range(4):
            event = await _committed_event(store)
            await runner._handle_live_event(event)

        assert runner.subscription._events_seen == runner.subscription._events_delivered
        assert runner.subscription.lag == 0


class TestNetZeroDisposal:
    async def test_a_filtered_event_leaves_no_lag(self) -> None:
        store = InMemoryEventStore()
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        # Subscribed to LagTestEvent only, so an unrelated type is filtered.
        runner = _runner(subscriber, store, event_types=(LagTestEvent,))

        class OtherEvent(DomainEvent):
            aggregate_type: str = "LagTestAggregate"

        other = OtherEvent(aggregate_id=uuid4())
        await store.append(
            StreamId(aggregate_id=other.aggregate_id, category="LagTestAggregate"),
            [other],
            ExpectedVersion.no_stream(),
        )

        await runner._handle_live_event(other)

        assert runner.subscription.lag == 0

    async def test_a_swallowed_failure_leaves_no_lag(self) -> None:
        store = InMemoryEventStore()
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        subscriber.fail = True
        runner = _runner(subscriber, store, continue_on_error=True)

        event = await _committed_event(store)
        await runner._handle_live_event(event)

        assert runner.subscription.lag == 0
        assert runner.subscription.events_failed == 1


class TestBufferedWakeUpsDoNotCountAsLag:
    """Buffering during transition no longer holds raw events, only wake-ups.

    Lag is recorded per envelope pulled from the feed (`_drain_feed`), so a
    buffered wake-up -- which drains nothing until `process_buffer()` runs
    -- contributes no lag on its own; the lag shows up once the drain
    actually reads the envelopes.
    """

    async def test_process_buffer_drain_is_the_lag_signal(self) -> None:
        store = InMemoryEventStore()
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        runner = _runner(subscriber, store)
        await runner.start(buffer_events=True)

        for _ in range(3):
            event = await _committed_event(store)
            await runner._handle_live_event(event)

        assert runner.buffer_size == 3
        assert runner.subscription.lag == 0

        await runner.process_buffer()

        assert runner.subscription.lag == 0
        assert len(subscriber.handled) == 3
