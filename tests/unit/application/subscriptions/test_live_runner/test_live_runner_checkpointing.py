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
    """Tests for different checkpoint strategies."""

    @pytest.mark.asyncio
    async def test_every_event_strategy_checkpoints(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test EVERY_EVENT strategy saves checkpoint after each event."""
        config = SubscriptionConfig(
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        )
        subscription = Subscription(
            name="EveryEventLive",
            config=config,
            subscriber=subscriber,
        )
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

        saved = await checkpoint_repo.get_position("EveryEventLive")
        assert saved == position

    @pytest.mark.asyncio
    async def test_every_batch_strategy_checkpoints_live(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test EVERY_BATCH strategy checkpoints after each event in live mode."""
        config = SubscriptionConfig(
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(
            name="EveryBatchLive",
            config=config,
            subscriber=subscriber,
        )
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

        # In live mode, EVERY_BATCH behaves like EVERY_EVENT
        saved = await checkpoint_repo.get_position("EveryBatchLive")
        assert saved == position

    @pytest.mark.asyncio
    async def test_periodic_strategy_respects_interval(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test PERIODIC strategy only saves when interval has elapsed.

        The interval is advanced by rewinding the runner's last-checkpoint
        stamp rather than by sleeping. This test used to configure a 100ms
        interval, process an event ~10ms later, and assert no checkpoint had
        been written -- which is only true if fewer than 100ms of wall clock
        pass between `start()` and the event being processed. That holds on an
        idle machine and fails on a loaded one: the event loop is session-
        scoped and shared by the whole suite, so any scheduling stall longer
        than the interval makes the runner checkpoint on the *first* event and
        the assertion reads as a logic error. It failed exactly that way in CI
        on 2026-08-10 while passing locally.

        Rewinding `_last_checkpoint_time` tests the same contract -- PERIODIC
        checkpoints only once `checkpoint_interval_seconds` has elapsed since
        the last write -- without a race, and with an interval large enough
        that no plausible stall can satisfy it accidentally.
        """
        interval = 30.0
        config = SubscriptionConfig(
            checkpoint_strategy=CheckpointStrategy.PERIODIC,
            checkpoint_interval_seconds=interval,
        )
        subscription = Subscription(
            name="PeriodicLive",
            config=config,
            subscriber=subscriber,
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        # First event - must not checkpoint, the interval has not elapsed.
        event1 = LiveTestEvent(aggregate_id=uuid4(), data="first")
        await append_event(event_store, event1)
        await wake(event_bus)

        assert await checkpoint_repo.get_position("PeriodicLive") is None

        # Advance past the interval without waiting for it.
        runner._last_checkpoint_time -= interval * 2

        # Second event - the interval has now elapsed, so this one checkpoints.
        event2 = LiveTestEvent(aggregate_id=uuid4(), data="second")
        position2 = await append_event(event_store, event2)
        await wake(event_bus)

        saved = await checkpoint_repo.get_position("PeriodicLive")
        assert saved == position2


class TestLiveRunnerErrorHandling:
    """Tests for error handling behavior."""

    @pytest.mark.asyncio
    async def test_continue_on_error_true(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
    ):
        """Test continue_on_error=True continues after handler failure."""
        subscriber = MockLiveSubscriber()
        subscriber.fail_on_event_types.add("LiveTestEvent")

        config = SubscriptionConfig(continue_on_error=True)
        subscription = Subscription(
            name="ContinueOnError",
            config=config,
            subscriber=subscriber,
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="fail")
        await append_event(event_store, event)
        await wake(event_bus)
        await asyncio.sleep(0.01)

        # Should record failure but continue
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
        """Test continue_on_error=False propagates exception."""
        subscriber = MockLiveSubscriber()
        subscriber.fail_on_event_types.add("LiveTestEvent")

        config = SubscriptionConfig(continue_on_error=False)
        subscription = Subscription(
            name="StopOnError",
            config=config,
            subscriber=subscriber,
        )
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
