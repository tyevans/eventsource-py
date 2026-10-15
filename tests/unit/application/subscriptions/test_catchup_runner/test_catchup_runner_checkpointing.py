"""Checkpoint strategy and position tracking tests for CatchUpRunner."""

import asyncio

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import CatchUpRunner
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_catchup_runner._fixtures import (
    MockSubscriber,
    SampleTestEvent,
    add_events_to_store,
    current,
)


class TestCatchUpRunnerCheckpointStrategies:
    """Tests for different checkpoint strategies."""

    @pytest.mark.asyncio
    async def test_every_event_strategy(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 5)
        target = await current(event_store)
        config = SubscriptionConfig(
            batch_size=10, checkpoint_strategy=CheckpointStrategy.EVERY_EVENT
        )
        subscription = Subscription(name="EveryEventTest", config=config, subscriber=subscriber)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 5
        assert await checkpoint_repo.get_position("EveryEventTest") == target

    @pytest.mark.asyncio
    async def test_every_batch_strategy(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 15)
        target = await current(event_store)
        config = SubscriptionConfig(
            batch_size=5, checkpoint_strategy=CheckpointStrategy.EVERY_BATCH
        )
        subscription = Subscription(name="EveryBatchTest", config=config, subscriber=subscriber)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 15
        assert await checkpoint_repo.get_position("EveryBatchTest") == target

    @pytest.mark.asyncio
    async def test_periodic_strategy(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        await add_events_to_store(event_store, 10)
        target = await current(event_store)

        class SlowSubscriber:
            def __init__(self) -> None:
                self.handled_events: list[DomainEvent] = []

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [SampleTestEvent]

            async def handle(self, event: DomainEvent) -> None:
                await asyncio.sleep(0.001)
                self.handled_events.append(event)

        config = SubscriptionConfig(
            batch_size=100,
            checkpoint_strategy=CheckpointStrategy.PERIODIC,
            checkpoint_interval_seconds=0.005,
        )
        subscription = Subscription(name="PeriodicTest", config=config, subscriber=SlowSubscriber())
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 10
        checkpoint = await checkpoint_repo.get_position("PeriodicTest")
        assert checkpoint is not None and checkpoint <= target


class TestCatchUpRunnerPositionTracking:
    """Tests for position tracking behavior."""

    @pytest.mark.asyncio
    async def test_subscription_position_updated(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        await add_events_to_store(event_store, 10)
        target = await current(event_store)
        assert subscription.last_processed_position is None

        await runner.run_until_position(target_position=target)

        assert subscription.last_processed_position == target

    @pytest.mark.asyncio
    async def test_subscription_event_metadata_updated(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        await runner.run_until_position(target_position=target)

        assert subscription.last_event_id is not None
        assert subscription.last_event_type == "SampleTestEvent"
        assert subscription.events_processed == 3

    @pytest.mark.asyncio
    async def test_events_seen_tracked_for_lag(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        await add_events_to_store(event_store, 5)
        target = await current(event_store)
        assert subscription.lag == 0

        await runner.run_until_position(target_position=target)

        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_checkpoint_persisted_correctly(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        await runner.run_until_position(target_position=target)

        assert await checkpoint_repo.get_position("TestSubscription") == target
