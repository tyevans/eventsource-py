"""Lifecycle, result, and stop tests for CatchUpRunner."""

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.application.subscriptions.runners import CatchUpResult, CatchUpRunner
from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import Position
from tests.unit.application.subscriptions.test_catchup_runner._fixtures import (
    MockSubscriber,
    add_events_to_store,
    current,
)


class TestCatchUpResult:
    """Tests for CatchUpResult dataclass."""

    def test_success_property_when_completed_no_error(self):
        """Test success returns True when completed without error."""
        result = CatchUpResult(
            events_processed=100,
            final_position=Position(store_id="test", key=(100,)),
            completed=True,
            error=None,
        )
        assert result.success is True

    def test_success_property_when_not_completed(self):
        """Test success returns False when not completed."""
        result = CatchUpResult(
            events_processed=50,
            final_position=Position(store_id="test", key=(50,)),
            completed=False,
            error=None,
        )
        assert result.success is False

    def test_success_property_when_error(self):
        """Test success returns False when error occurred."""
        result = CatchUpResult(
            events_processed=50,
            final_position=Position(store_id="test", key=(50,)),
            completed=True,
            error=ValueError("Test error"),
        )
        assert result.success is False

    def test_result_fields(self):
        """Test all result fields are accessible."""
        error = RuntimeError("test")
        result = CatchUpResult(
            events_processed=42,
            final_position=Position(store_id="test", key=(100,)),
            completed=True,
            error=error,
        )
        assert result.events_processed == 42
        assert result.final_position == Position(store_id="test", key=(100,))
        assert result.completed is True
        assert result.error is error


class TestCatchUpRunnerImports:
    """Tests for module imports and exports."""

    def test_import_from_runners_module(self):
        """Test CatchUpRunner can be imported from runners module."""
        from eventsource.application.subscriptions.runners import CatchUpResult, CatchUpRunner

        assert CatchUpRunner is not None
        assert CatchUpResult is not None

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from eventsource.application.subscriptions.runners import catchup

        assert "CatchUpRunner" in catchup.__all__
        assert "CatchUpResult" in catchup.__all__


class TestCatchUpRunnerLifecycle:
    """Lifecycle and state transition tests for CatchUpRunner."""

    @pytest.mark.asyncio
    async def test_runner_initialization(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscription: Subscription,
    ):
        """Test runner initializes correctly."""
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        assert runner.event_store is event_store
        assert runner.checkpoint_repo is checkpoint_repo
        assert runner.subscription is subscription
        assert runner.config is subscription.config
        assert runner.is_running is False
        assert runner.stop_requested is False

    @pytest.mark.asyncio
    async def test_subscription_state_transitions_to_catching_up(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription transitions to CATCHING_UP state."""
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        assert subscription.state == SubscriptionState.STARTING

        await runner.run_until_position(target_position=target)

        assert subscription.state == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_is_running_during_processing(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
    ):
        """Test is_running is True during processing."""
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        # Runner should not be running before start
        assert runner.is_running is False

        # Start and verify it completes (can't easily test during due to async)
        result = await runner.run_until_position(target_position=target)

        # Should be False after completion
        assert runner.is_running is False
        assert result.completed is True


class TestCatchUpRunnerStop:
    """Tests for graceful stop functionality."""

    @pytest.mark.asyncio
    async def test_stop_sets_flag(
        self,
        runner: CatchUpRunner,
    ):
        """Test stop() sets stop_requested flag."""
        assert runner.stop_requested is False

        await runner.stop()

        assert runner.stop_requested is True

    @pytest.mark.asyncio
    async def test_stop_halts_processing(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test stop() causes runner to halt processing."""
        # Add many events
        await add_events_to_store(event_store, 100)
        target = await current(event_store)

        config = SubscriptionConfig(batch_size=10)
        subscription = Subscription(
            name="StopTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        # Create a wrapper subscriber that stops after first event
        original_handle = subscriber.handle

        async def stopping_handle(event: DomainEvent) -> None:
            await original_handle(event)
            if len(subscriber.handled_events) == 1:
                await runner.stop()

        subscriber.handle = stopping_handle  # type: ignore[method-assign]

        result = await runner.run_until_position(target_position=target)

        # Should stop before processing all events
        assert result.completed is False
        assert result.events_processed < 100
        assert result.error is None

    @pytest.mark.asyncio
    async def test_lag_clears_after_stop_mid_batch_and_resume(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test lag doesn't accumulate phantom counts when a batch is
        abandoned mid-delivery and its tail is re-read on resume.

        Reproduces: record_events_seen(len(events)) fires before the
        delivery loop; if the loop stops early, the undelivered tail stays
        counted as seen. The next run re-reads and re-counts that tail,
        permanently inflating events_seen relative to events_delivered.
        """
        # All 10 events land in a single batch (batch_size == 10).
        await add_events_to_store(event_store, 10)
        target = await current(event_store)

        config = SubscriptionConfig(batch_size=10)
        subscription = Subscription(
            name="StopResumeTest",
            config=config,
            subscriber=subscriber,
        )
        first_runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        # Stop after 4 of 10 events have been delivered, mid-batch.
        original_handle = subscriber.handle

        async def stopping_handle(event: DomainEvent) -> None:
            await original_handle(event)
            if len(subscriber.handled_events) == 4:
                await first_runner.stop()

        subscriber.handle = stopping_handle  # type: ignore[method-assign]

        first_result = await first_runner.run_until_position(target_position=target)
        assert first_result.completed is False
        assert first_result.events_processed == 4

        # Simulate a quiescent state between runs (a real stop/resume cycle
        # would pause or restart the subscription rather than leave it
        # mid-CATCHING_UP), then resume with a fresh runner over the same
        # subscription and deliver the rest.
        await subscription.transition_to(SubscriptionState.PAUSED)
        subscriber.handle = original_handle  # type: ignore[method-assign]
        second_runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        second_result = await second_runner.run_until_position(target_position=target)

        assert second_result.completed is True
        assert len(subscriber.handled_events) == 10
        assert subscription.lag == 0
