"""Live-phase lag reports received-but-not-yet-delivered events.

`Subscription.lag` is `events_seen - events_delivered`, and its invariant
requires callers to keep the two symmetric across any boundary. The
catch-up runner did; the live runner counted deliveries without ever
counting receipts, so lag read 0 throughout LIVE no matter how far behind
the subscriber was. The bus delivery receipt is the live seen-point.
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import LiveRunner
from eventsource.events.base import DomainEvent

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


def _runner(subscriber: BlockingSubscriber, **config_kwargs) -> LiveRunner:  # type: ignore[no-untyped-def]
    config = SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        **config_kwargs,
    )
    subscription = Subscription(name="LagSubscription", config=config, subscriber=subscriber)
    return LiveRunner(
        event_bus=InMemoryEventBus(enable_tracing=False),
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        subscription=subscription,
    )


def _event() -> LagTestEvent:
    return LagTestEvent(aggregate_id=uuid4())


class TestLiveLagSignal:
    async def test_a_stalled_subscriber_shows_nonzero_lag(self) -> None:
        subscriber = BlockingSubscriber()
        runner = _runner(subscriber)

        tasks = [asyncio.create_task(runner._handle_live_event(_event())) for _ in range(3)]
        await asyncio.wait_for(subscriber.entered.wait(), 1.0)

        assert runner.subscription.lag >= 1

        subscriber.release.set()
        await asyncio.gather(*tasks)

        assert runner.subscription.lag == 0

    async def test_seen_and_delivered_stay_symmetric(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        runner = _runner(subscriber)

        for _ in range(4):
            await runner._handle_live_event(_event())

        assert runner.subscription._events_seen == runner.subscription._events_delivered
        assert runner.subscription.lag == 0


class TestNetZeroDisposal:
    async def test_a_filtered_event_without_a_position_leaves_no_lag(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        # Subscribed to LagTestEvent only, so an unrelated type is filtered.
        runner = _runner(subscriber, event_types=(LagTestEvent,))

        class OtherEvent(DomainEvent):
            aggregate_type: str = "LagTestAggregate"

        await runner._handle_live_event(OtherEvent(aggregate_id=uuid4()))

        assert runner.subscription.lag == 0

    async def test_a_swallowed_failure_leaves_no_lag(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        subscriber.fail = True
        runner = _runner(subscriber, continue_on_error=True)

        await runner._handle_live_event(_event())

        assert runner.subscription.lag == 0
        assert runner.subscription.events_failed == 1


class TestBufferedEventsCountAsLag:
    async def test_transition_buffer_depth_is_lag(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        runner = _runner(subscriber)
        await runner.start(buffer_events=True)

        for _ in range(3):
            await runner._handle_live_event(_event())

        assert runner.subscription.lag == 3

        await runner.process_buffer()

        assert runner.subscription.lag == 0
