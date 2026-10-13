"""Tests for event buffering and live runner access during/after transition."""

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
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
    TransitionPhase,
)
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.subscriptions.test_transition._fixtures import (
    MockTransitionSubscriber,
    TransitionTestEvent,
    add_events_to_store,
)


class TestTransitionBufferMode:
    """Tests for event buffering during transition."""

    @pytest.mark.asyncio
    async def test_events_during_catchup_are_buffered(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test events published during catch-up are buffered."""
        # Add initial events
        await add_events_to_store(event_store, 5)

        # Create subscription with slow handler to simulate catch-up time
        subscriber.handle_delay = 0.01  # 10ms delay per event

        config = SubscriptionConfig(batch_size=10)
        subscription = Subscription(
            name="BufferTest",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        # Start transition in background
        transition_task = asyncio.create_task(coordinator.execute())

        # Wait a bit for live subscription to start buffering
        await asyncio.sleep(0.02)

        # Publish a live event while catch-up is in progress
        if coordinator.phase in (
            TransitionPhase.LIVE_SUBSCRIBED,
            TransitionPhase.FINAL_CATCHUP,
        ):
            live_event = TransitionTestEvent(aggregate_id=uuid4(), data="live_during_catchup")
            await event_bus.publish([live_event])

        # Wait for transition to complete
        result = await transition_task

        assert result.success is True

    @pytest.mark.asyncio
    async def test_duplicates_in_buffer_are_skipped(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test duplicate events in buffer are filtered."""
        # Add events to store
        await add_events_to_store(event_store, 5)

        config = SubscriptionConfig(batch_size=100)
        subscription = Subscription(
            name="DuplicateTest",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is True
        # All events processed via catch-up, none should be duplicated
        assert result.phase_reached == TransitionPhase.LIVE


class TestTransitionLiveRunnerAccess:
    """Tests for accessing live runner after transition."""

    @pytest.mark.asyncio
    async def test_live_runner_available_after_transition(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        """Test live_runner is available after successful transition."""
        await add_events_to_store(event_store, 5)

        result = await coordinator.execute()

        assert result.success is True
        assert coordinator.live_runner is not None
        assert coordinator.live_runner.is_running is True

    @pytest.mark.asyncio
    async def test_live_runner_receives_events_after_transition(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test live runner processes events after transition completes.

        Regression test for the live-checkpoint bug: the live runner used to
        read a `_position` attribute that nothing ever set, so it always
        delivered live events with `position=None` and never checkpointed
        past the catch-up watermark -- a live subscription would replay its
        entire live period from the watermark on every restart. Now the
        runner drains `GlobalEventFeed.read_all(from_position=...)` on each
        bus wake-up, so a live event's real feed position is always known
        and checkpointed.
        """
        # Add initial events
        await add_events_to_store(event_store, 5)

        config = SubscriptionConfig(
            batch_size=100,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        )
        subscription = Subscription(
            name="LiveAfterTransition",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()
        assert result.success is True
        assert len(subscriber.handled_events) == 5

        watermark = result.final_position
        assert watermark is not None

        # A real write: append to the store (the source of truth), then
        # publish to the bus purely as a wake-up notification -- the bus
        # payload itself is never delivered, per the store-owns-ordering
        # design (ADR 0047).
        live_event = TransitionTestEvent(aggregate_id=uuid4(), data="after_transition")
        append_result = await event_store.append(
            StreamId(aggregate_id=live_event.aggregate_id, category="TransitionAggregate"),
            [live_event],
            ExpectedVersion.no_stream(),
        )
        await event_bus.publish([live_event])

        # Wait for event processing
        await asyncio.sleep(0.01)

        # Should have received the live event
        assert len(subscriber.handled_events) == 6
        assert subscriber.handled_events[-1].data == "after_transition"

        # The checkpoint must have advanced past the catch-up watermark to
        # the live event's real position -- not stayed pinned at the
        # watermark the way the unfixed `_get_event_position` bug left it.
        checkpoint_position = await checkpoint_repo.get_position("LiveAfterTransition")
        assert checkpoint_position == append_result.position
        assert checkpoint_position is not None
        assert checkpoint_position > watermark
