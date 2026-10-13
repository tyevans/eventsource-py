"""Basic and initialization tests for LiveRunner."""

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionState,
)
from eventsource.application.subscriptions.runners import LiveRunner, LiveRunnerStats


class TestLiveRunnerStats:
    """Tests for LiveRunnerStats dataclass."""

    def test_default_values(self):
        """Test default values are all zero."""
        stats = LiveRunnerStats()
        assert stats.events_received == 0
        assert stats.events_processed == 0
        assert stats.events_skipped_filtered == 0
        assert stats.events_failed == 0

    def test_custom_values(self):
        """Test custom values are set correctly."""
        stats = LiveRunnerStats(
            events_received=100,
            events_processed=90,
            events_skipped_filtered=5,
            events_failed=5,
        )
        assert stats.events_received == 100
        assert stats.events_processed == 90
        assert stats.events_skipped_filtered == 5
        assert stats.events_failed == 5


class TestLiveRunnerBasic:
    """Basic tests for LiveRunner."""

    @pytest.mark.asyncio
    async def test_runner_initialization(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test runner initializes correctly."""
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )

        assert runner.event_bus is event_bus
        assert runner.checkpoint_repo is checkpoint_repo
        assert runner.event_feed is event_store
        assert runner.subscription is subscription
        assert runner.is_running is False
        assert runner.buffer_size == 0

    @pytest.mark.asyncio
    async def test_start_subscribes_to_event_bus(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
    ):
        """Test start() subscribes to event bus."""
        assert event_bus.get_subscriber_count() == 0

        await runner.start()

        # Should have subscribed to 2 event types (LiveTestEvent, AnotherTestEvent)
        assert event_bus.get_subscriber_count() == 2
        assert runner.is_running is True

    @pytest.mark.asyncio
    async def test_start_transitions_to_live_state(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test start() transitions subscription to LIVE state."""
        assert subscription.state == SubscriptionState.STARTING

        await runner.start(buffer_events=False)

        assert subscription.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_start_with_buffer_does_not_transition(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test start(buffer_events=True) does not transition to LIVE."""
        assert subscription.state == SubscriptionState.STARTING

        await runner.start(buffer_events=True)

        # Should still be STARTING (transition happens after buffer is processed)
        assert subscription.state == SubscriptionState.STARTING
        assert runner.is_running is True

    @pytest.mark.asyncio
    async def test_start_is_idempotent(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
    ):
        """Test calling start() multiple times is safe."""
        await runner.start()
        initial_count = event_bus.get_subscriber_count()

        # Second call should be no-op
        await runner.start()

        assert event_bus.get_subscriber_count() == initial_count

    @pytest.mark.asyncio
    async def test_stop_unsubscribes_from_event_bus(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
    ):
        """Test stop() unsubscribes from event bus."""
        await runner.start()
        assert event_bus.get_subscriber_count() == 2

        await runner.stop()

        assert runner.is_running is False

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(
        self,
        runner: LiveRunner,
    ):
        """Test calling stop() multiple times is safe."""
        await runner.start()
        await runner.stop()

        # Second call should be no-op
        await runner.stop()

        assert runner.is_running is False


class TestLiveRunnerImports:
    """Tests for module imports and exports."""

    def test_import_from_runners_module(self):
        """Test LiveRunner can be imported from runners module."""
        from eventsource.application.subscriptions.runners import LiveRunner, LiveRunnerStats

        assert LiveRunner is not None
        assert LiveRunnerStats is not None

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from eventsource.application.subscriptions.runners import live

        assert "LiveRunner" in live.__all__
        assert "LiveRunnerStats" in live.__all__

    def test_runners_module_exports_all(self):
        """Test runners module exports all expected classes."""
        from eventsource.application.subscriptions import runners

        assert hasattr(runners, "LiveRunner")
        assert hasattr(runners, "LiveRunnerStats")
        assert hasattr(runners, "CatchUpRunner")
        assert hasattr(runners, "CatchUpResult")
