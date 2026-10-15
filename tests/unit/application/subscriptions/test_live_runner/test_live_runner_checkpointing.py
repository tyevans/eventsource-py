"""Checkpoint strategy and error handling tests for LiveRunner."""

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
from tests.unit.application.subscriptions.test_live_runner._fixtures import (
    LiveTestEvent,
    MockLiveSubscriber,
    append_event,
    wake,
)


class TestLiveRunnerCheckpointStrategies:
    @pytest.mark.asyncio
    async def test_every_event_strategy_checkpoints(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.EVERY_EVENT)
        subscription = Subscription(name="EveryEventLive", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="checkpoint_test")
        position = await append_event(event_store, event)
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert await checkpoint_repo.get_position("EveryEventLive") == position

    @pytest.mark.asyncio
    async def test_every_batch_strategy_checkpoints_live(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.EVERY_BATCH)
        subscription = Subscription(name="EveryBatchLive", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="batch_test")
        position = await append_event(event_store, event)
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert await checkpoint_repo.get_position("EveryBatchLive") == position

    @pytest.mark.asyncio
    async def test_periodic_strategy_respects_interval(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test PERIODIC strategy only saves when interval has elapsed."""
        interval = 30.0
        config = SubscriptionConfig(
            checkpoint_strategy=CheckpointStrategy.PERIODIC,
            checkpoint_interval_seconds=interval,
        )
        subscription = Subscription(name="PeriodicLive", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        event1 = LiveTestEvent(aggregate_id=uuid4(), data="first")
        await append_event(event_store, event1)
        await wake(event_bus)
        assert await checkpoint_repo.get_position("PeriodicLive") is None

        runner._last_checkpoint_time -= interval * 2
        event2 = LiveTestEvent(aggregate_id=uuid4(), data="second")
        position2 = await append_event(event_store, event2)
        await wake(event_bus)

        assert await checkpoint_repo.get_position("PeriodicLive") == position2


class TestLiveRunnerErrorHandling:
    @pytest.mark.asyncio
    async def test_continue_on_error_true(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
    ):
        subscriber = MockLiveSubscriber()
        subscriber.fail_on_event_types.add("LiveTestEvent")
        config = SubscriptionConfig(continue_on_error=True)
        subscription = Subscription(name="ContinueOnError", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data="fail"))
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert runner.stats.events_failed == 1
        assert subscription.events_failed == 1
        assert subscription.last_error is not None

    @pytest.mark.asyncio
    async def test_continue_on_error_false_raises(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
    ):
        subscriber = MockLiveSubscriber()
        subscriber.fail_on_event_types.add("LiveTestEvent")
        config = SubscriptionConfig(continue_on_error=False)
        subscription = Subscription(name="StopOnError", config=config, subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="fail")
        await append_event(event_store, event)
        with pytest.raises(ValueError):
            await runner._handle_live_event(event)
        assert runner.stats.events_failed == 1
