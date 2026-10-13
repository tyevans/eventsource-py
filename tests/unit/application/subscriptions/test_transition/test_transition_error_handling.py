"""Tests for error handling during transition."""

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
    TransitionPhase,
)
from tests.unit.application.subscriptions.test_transition._fixtures import (
    MockTransitionSubscriber,
    add_events_to_store,
)


class TestTransitionErrorHandling:
    """Tests for error handling during transition."""

    @pytest.mark.asyncio
    async def test_catchup_error_handled(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test error during catch-up is handled properly."""
        # Create subscriber that fails
        subscriber = MockTransitionSubscriber()
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=False,  # Stop on error
        )
        subscription = Subscription(
            name="ErrorTest",
            config=config,
            subscriber=subscriber,
        )

        # Add events that will fail
        await add_events_to_store(event_store, 3)

        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is False
        assert result.phase_reached == TransitionPhase.FAILED
        assert result.error is not None
        assert coordinator.phase == TransitionPhase.FAILED

    @pytest.mark.asyncio
    async def test_cleanup_on_failure(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test resources are cleaned up on failure."""
        subscriber = MockTransitionSubscriber()
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        config = SubscriptionConfig(continue_on_error=False)
        subscription = Subscription(
            name="CleanupTest",
            config=config,
            subscriber=subscriber,
        )

        await add_events_to_store(event_store, 3)

        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is False
        # Runners should be stopped after cleanup
        if coordinator.live_runner:
            assert not coordinator.live_runner.is_running
        if coordinator.catchup_runner:
            assert not coordinator.catchup_runner.is_running
