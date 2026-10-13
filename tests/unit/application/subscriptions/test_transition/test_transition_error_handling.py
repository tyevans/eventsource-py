"""Tests for error handling during transition."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners.live import LiveRunner
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
    TransitionPhase,
)
from tests.unit.application.subscriptions.test_transition._fixtures import (
    MockTransitionSubscriber,
    TransitionTestEvent,
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
        subscriber = MockTransitionSubscriber()
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=False,
        )
        subscription = Subscription(
            name="ErrorTest",
            config=config,
            subscriber=subscriber,
        )

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
        """Test resources are cleaned up on failure and lag is reconciled to 0."""
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
        assert subscription.lag == 0
        if coordinator.live_runner:
            assert not coordinator.live_runner.is_running
            assert coordinator.live_runner.buffer_size == 0
        if coordinator.catchup_runner:
            assert not coordinator.catchup_runner.is_running

    @pytest.mark.asyncio
    async def test_live_runner_stop_and_clear_buffer_reconciles_lag(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Stopping LiveRunner or TransitionCoordinator clears buffered wakes and reconciles lag to 0."""
        subscription = Subscription(
            name="LagReconcileSub",
            config=SubscriptionConfig(),
            subscriber=subscriber,
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start(buffer_events=True)
        await subscription.record_events_seen(5)
        await event_bus.publish([TransitionTestEvent(aggregate_id=uuid4(), data="wake")])

        assert runner.buffer_size == 1
        assert subscription.lag == 5

        await runner.stop()

        assert runner.buffer_size == 0
        assert runner.pause_buffer_size == 0
        assert subscription.lag == 0
