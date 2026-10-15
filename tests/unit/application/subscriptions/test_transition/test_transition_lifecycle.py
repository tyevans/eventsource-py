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
    @pytest.mark.asyncio
    async def test_coordinator_initialization(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscription: Subscription,
    ):
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
    async def test_transition_empty_store(self, coordinator: TransitionCoordinator):
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
        await add_events_to_store(event_store, 5)
        assert subscription.state == SubscriptionState.STARTING
        result = await coordinator.execute()
        assert result.success is True
        assert subscription.state == SubscriptionState.LIVE


class TestTransitionAlreadyCaughtUp:
    @pytest.mark.asyncio
    async def test_skip_catchup_when_at_watermark(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
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
        await add_events_to_store(event_store, 5)
        beyond = Position(store_id="memory", key=(100,))
        subscription.last_processed_position = beyond

        result = await coordinator.execute()
        assert result.success is True
        assert result.catchup_events_processed == 0
        assert result.final_position == beyond


class TestTransitionStop:
    @pytest.mark.asyncio
    async def test_stop_during_transition(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        subscriber = MockTransitionSubscriber()
        subscriber.handle_delay = 0.1
        subscription = Subscription(
            name="StopTest",
            config=SubscriptionConfig(batch_size=1),
            subscriber=subscriber,
        )
        await add_events_to_store(event_store, 10)
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        transition_task = asyncio.create_task(coordinator.execute())
        await asyncio.sleep(0.05)
        await coordinator.stop()
        result = await transition_task
        assert result.catchup_events_processed < 10

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self, coordinator: TransitionCoordinator):
        await coordinator.stop()
        await coordinator.stop()
        assert coordinator.phase == TransitionPhase.NOT_STARTED


class TestTransitionPhaseTracking:
    @pytest.mark.asyncio
    async def test_phase_transitions_correctly(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        result = await coordinator.execute()
        assert result.success is True
        assert coordinator.phase == TransitionPhase.LIVE

    @pytest.mark.asyncio
    async def test_watermark_captured(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 10)
        assert coordinator.watermark is None
        expected = await current(event_store)
        result = await coordinator.execute()
        assert result.success is True
        assert coordinator.watermark == expected
