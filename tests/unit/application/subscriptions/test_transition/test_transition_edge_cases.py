"""Tests for edge cases and boundary conditions in transition."""

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
)
from tests.unit.application.subscriptions.test_transition._fixtures import (
    MockTransitionSubscriber,
    add_events_to_store,
    current,
)


class TestTransitionEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_transition_single_event(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscriber: MockTransitionSubscriber,
    ):
        """Test transition with exactly one event."""
        await add_events_to_store(event_store, 1)

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 1
        assert len(subscriber.handled_events) == 1

    @pytest.mark.asyncio
    async def test_transition_large_batch(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test transition with many events."""
        # Add 100 events
        await add_events_to_store(event_store, 100)

        config = SubscriptionConfig(batch_size=25)  # Multiple batches
        subscription = Subscription(
            name="LargeBatchTest",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 100
        assert len(subscriber.handled_events) == 100

    @pytest.mark.asyncio
    async def test_position_tracking_during_transition(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription position is correctly tracked during transition."""
        await add_events_to_store(event_store, 10)
        watermark = await current(event_store)

        assert subscription.last_processed_position is None

        result = await coordinator.execute()

        assert result.success is True
        assert subscription.last_processed_position == watermark
        assert result.final_position == watermark

    @pytest.mark.asyncio
    async def test_checkpoint_saved_during_transition(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test checkpoints are saved during transition."""
        await add_events_to_store(event_store, 10)

        config = SubscriptionConfig(
            batch_size=5,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(
            name="CheckpointSaveTest",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is True

        # Check checkpoint was saved
        position = await checkpoint_repo.get_position("CheckpointSaveTest")
        assert position == await current(event_store)

    @pytest.mark.asyncio
    async def test_continue_on_error_during_catchup(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test continue_on_error allows transition to complete despite errors."""
        subscriber = MockTransitionSubscriber()
        # Fail on first event only
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        # Add events
        await add_events_to_store(event_store, 5)

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=True,  # Continue despite errors
        )
        subscription = Subscription(
            name="ContinueOnErrorTransition",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        # Should complete despite errors
        assert result.success is True
        assert result.catchup_events_processed == 5
        assert subscription.events_failed == 5  # All events failed but continued

    @pytest.mark.asyncio
    async def test_catchup_runner_property(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        """Test catchup_runner property is available during transition."""
        await add_events_to_store(event_store, 5)

        assert coordinator.catchup_runner is None

        result = await coordinator.execute()

        # After transition, catchup runner should be available
        # (though not running anymore)
        assert result.success is True
        assert coordinator.catchup_runner is not None
