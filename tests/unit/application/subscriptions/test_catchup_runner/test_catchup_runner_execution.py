"""Event processing, batch processing, and edge case tests for CatchUpRunner."""

from datetime import datetime

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import CatchUpRunner
from eventsource.ports.positions import Position
from tests.unit.application.subscriptions.test_catchup_runner._fixtures import (
    MockSubscriber,
    add_events_to_store,
    current,
    position_after,
)


class TestCatchUpRunnerBasic:
    """Basic event processing tests for CatchUpRunner."""

    @pytest.mark.asyncio
    async def test_empty_event_store_returns_completed(
        self, runner: CatchUpRunner, event_store: InMemoryEventStore
    ):
        assert await event_store.current_position() is None
        result = await runner.run_until_position(
            target_position=Position(store_id="memory", key=(1,))
        )
        assert result.completed is True and result.events_processed == 0
        assert result.final_position is None and result.error is None

    @pytest.mark.asyncio
    async def test_target_position_already_reached(
        self, runner: CatchUpRunner, subscription: Subscription
    ):
        position = Position(store_id="memory", key=(100,))
        subscription.last_processed_position = position
        result = await runner.run_until_position(target_position=position)
        assert result.completed is True and result.events_processed == 0
        assert result.final_position == position

    @pytest.mark.asyncio
    async def test_processes_all_events(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 5)
        target = await current(event_store)
        result = await runner.run_until_position(target_position=target)
        assert result.completed is True and result.events_processed == 5
        assert result.final_position == target and len(subscriber.handled_events) == 5


class TestCatchUpRunnerBatching:
    """Tests for batch processing behavior."""

    @pytest.mark.asyncio
    async def test_respects_batch_size(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 25)
        target = await current(event_store)
        config = SubscriptionConfig(
            batch_size=5, checkpoint_strategy=CheckpointStrategy.EVERY_BATCH
        )
        subscription = Subscription(name="BatchTest", config=config, subscriber=subscriber)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)
        assert result.completed is True and result.events_processed == 25
        assert await checkpoint_repo.get_position("BatchTest") == target

    @pytest.mark.asyncio
    async def test_batch_smaller_than_remaining(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 3)
        target = await current(event_store)
        subscription = Subscription(
            name="SmallBatchTest", config=SubscriptionConfig(batch_size=100), subscriber=subscriber
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)
        assert result.completed is True and result.events_processed == 3


class TestCatchUpRunnerEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_zero_batch_size_validation(self):
        with pytest.raises(ValueError):
            SubscriptionConfig(batch_size=0)

    @pytest.mark.asyncio
    async def test_target_behind_the_feed_delivers_nothing(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 3)
        result = await runner.run_until_position(
            target_position=Position(store_id="memory", key=(0,))
        )
        assert result.completed is True and result.events_processed == 0
        assert subscriber.handled_events == []

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 10)
        target = await current(event_store)
        subscription = Subscription(
            name="ResumeTest", config=SubscriptionConfig(batch_size=100), subscriber=subscriber
        )
        subscription.last_processed_position = await position_after(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)
        assert result.completed is True and result.events_processed == 5
        assert len(subscriber.handled_events) == 5

    @pytest.mark.asyncio
    async def test_single_event(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 1)
        target = await current(event_store)
        subscription = Subscription(
            name="SingleEventTest", config=SubscriptionConfig(), subscriber=subscriber
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)
        assert result.completed is True and result.events_processed == 1
        assert len(subscriber.handled_events) == 1

    @pytest.mark.asyncio
    async def test_last_processed_at_updated(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        await add_events_to_store(event_store, 3)
        target = await current(event_store)
        assert subscription.last_processed_at is None
        await runner.run_until_position(target_position=target)
        assert isinstance(subscription.last_processed_at, datetime)
