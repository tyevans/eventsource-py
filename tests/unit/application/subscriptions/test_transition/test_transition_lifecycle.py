"""Tests for TransitionCoordinator basic execution, phase tracking, already-caught-up, and stop."""

import asyncio

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
    TransitionPhase,
)
from eventsource.ports.positions import Position
from tests.unit.application.subscriptions.test_transition._fixtures import (
    MockTransitionSubscriber,
    add_events_to_store,
    current,
)


class TestTransitionCoordinatorBasic:
    """Basic tests for TransitionCoordinator."""

    @pytest.mark.asyncio
    async def test_coordinator_initialization(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscription: Subscription,
    ):
        """Test coordinator initializes correctly."""
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        assert coordinator.event_store is event_store
        assert coordinator.event_bus is event_bus
        assert coordinator.checkpoint_repo is checkpoint_repo
        assert coordinator.subscription is subscription
        assert coordinator.phase == TransitionPhase.NOT_STARTED
        assert coordinator.watermark is None
        assert coordinator.live_runner is None
        assert coordinator.catchup_runner is None

    @pytest.mark.asyncio
    async def test_transition_empty_store(
        self,
        coordinator: TransitionCoordinator,
    ):
        """Test transition with empty event store goes directly to live."""
        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 0
        assert result.buffer_events_processed == 0
        assert result.final_position is None
        assert result.phase_reached == TransitionPhase.LIVE
        assert coordinator.phase == TransitionPhase.LIVE

    @pytest.mark.asyncio
    async def test_transition_from_beginning(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscriber: MockTransitionSubscriber,
    ):
        """Test full transition from beginning to live."""
        # Add events to store
        await add_events_to_store(event_store, 10)

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 10
        assert result.final_position == await current(event_store)
        assert result.phase_reached == TransitionPhase.LIVE
        assert len(subscriber.handled_events) == 10

    @pytest.mark.asyncio
    async def test_subscription_transitions_to_live_state(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription state transitions to LIVE after successful transition."""
        await add_events_to_store(event_store, 5)

        assert subscription.state == SubscriptionState.STARTING

        result = await coordinator.execute()

        assert result.success is True
        assert subscription.state == SubscriptionState.LIVE


class TestTransitionAlreadyCaughtUp:
    """Tests for transition when already caught up."""

    @pytest.mark.asyncio
    async def test_skip_catchup_when_at_watermark(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test transition skips catch-up when position equals watermark."""
        # Add events and set subscription position to end
        await add_events_to_store(event_store, 10)
        watermark = await current(event_store)
        subscription.last_processed_position = watermark

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 0
        assert result.buffer_events_processed == 0
        assert result.final_position == watermark
        assert result.phase_reached == TransitionPhase.LIVE

    @pytest.mark.asyncio
    async def test_skip_catchup_when_past_watermark(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test transition skips catch-up when position past watermark."""
        # Add events but set subscription position beyond
        await add_events_to_store(event_store, 5)
        beyond = Position(store_id="memory", key=(100,))  # Way past watermark
        subscription.last_processed_position = beyond

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 0
        assert result.final_position == beyond


class TestTransitionStop:
    """Tests for graceful stop functionality."""

    @pytest.mark.asyncio
    async def test_stop_during_transition(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test stop() during transition cleans up resources."""
        # Create slow subscriber
        subscriber = MockTransitionSubscriber()
        subscriber.handle_delay = 0.1  # 100ms delay per event

        config = SubscriptionConfig(batch_size=1)
        subscription = Subscription(
            name="StopTest",
            config=config,
            subscriber=subscriber,
        )

        # Add many events
        await add_events_to_store(event_store, 10)

        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        # Start transition in background
        transition_task = asyncio.create_task(coordinator.execute())

        # Wait briefly then stop
        await asyncio.sleep(0.05)
        await coordinator.stop()

        # Wait for task to complete
        result = await transition_task

        # Should have stopped early
        assert result.catchup_events_processed < 10

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(
        self,
        coordinator: TransitionCoordinator,
    ):
        """Test calling stop() multiple times is safe."""
        # Call stop without starting - should be no-op
        await coordinator.stop()
        await coordinator.stop()

        assert coordinator.phase == TransitionPhase.NOT_STARTED


class TestTransitionPhaseTracking:
    """Tests for phase tracking during transition."""

    @pytest.mark.asyncio
    async def test_phase_transitions_correctly(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        """Test phases transition in correct order."""
        await add_events_to_store(event_store, 5)

        result = await coordinator.execute()

        # Final phase should be LIVE
        assert result.success is True
        assert coordinator.phase == TransitionPhase.LIVE

    @pytest.mark.asyncio
    async def test_watermark_captured(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        """Test watermark is captured correctly."""
        await add_events_to_store(event_store, 10)

        assert coordinator.watermark is None  # Not yet captured

        expected = await current(event_store)
        result = await coordinator.execute()

        assert result.success is True
        assert coordinator.watermark == expected  # Feed position at start
