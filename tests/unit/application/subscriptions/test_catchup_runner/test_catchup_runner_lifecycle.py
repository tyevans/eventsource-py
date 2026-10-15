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
        result = CatchUpResult(
            events_processed=100,
            final_position=Position(store_id="test", key=(100,)),
            completed=True,
            error=None,
        )
        assert result.success is True

    def test_success_property_when_not_completed(self):
        result = CatchUpResult(
            events_processed=50,
            final_position=Position(store_id="test", key=(50,)),
            completed=False,
            error=None,
        )
        assert result.success is False

    def test_success_property_when_error(self):
        result = CatchUpResult(
            events_processed=50,
            final_position=Position(store_id="test", key=(50,)),
            completed=True,
            error=ValueError("Test error"),
        )
        assert result.success is False

    def test_result_fields(self):
        error = RuntimeError("test")
        result = CatchUpResult(
            events_processed=42,
            final_position=Position(store_id="test", key=(100,)),
            completed=True,
            error=error,
        )
        assert result.events_processed == 42
        assert result.final_position == Position(store_id="test", key=(100,))
        assert result.completed is True and result.error is error


class TestCatchUpRunnerImports:
    """Tests for module imports and exports."""

    def test_import_from_runners_module(self):
        from eventsource.application.subscriptions.runners import CatchUpResult, CatchUpRunner

        assert CatchUpRunner is not None and CatchUpResult is not None

    def test_all_exports(self):
        from eventsource.application.subscriptions.runners import catchup

        assert "CatchUpRunner" in catchup.__all__ and "CatchUpResult" in catchup.__all__


class TestCatchUpRunnerLifecycle:
    """Lifecycle and state transition tests for CatchUpRunner."""

    @pytest.mark.asyncio
    async def test_runner_initialization(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscription: Subscription,
    ):
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        assert runner.event_store is event_store and runner.checkpoint_repo is checkpoint_repo
        assert runner.subscription is subscription and runner.config is subscription.config
        assert runner.is_running is False and runner.stop_requested is False

    @pytest.mark.asyncio
    async def test_subscription_state_transitions_to_catching_up(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
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
        await add_events_to_store(event_store, 5)
        target = await current(event_store)
        assert runner.is_running is False
        result = await runner.run_until_position(target_position=target)
        assert runner.is_running is False and result.completed is True


class TestCatchUpRunnerStop:
    """Tests for graceful stop functionality."""

    @pytest.mark.asyncio
    async def test_stop_sets_flag(self, runner: CatchUpRunner):
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
        await add_events_to_store(event_store, 100)
        target = await current(event_store)
        subscription = Subscription(
            name="StopTest", config=SubscriptionConfig(batch_size=10), subscriber=subscriber
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        original_handle = subscriber.handle

        async def stopping_handle(event: DomainEvent) -> None:
            await original_handle(event)
            if len(subscriber.handled_events) == 1:
                await runner.stop()

        subscriber.handle = stopping_handle  # type: ignore[method-assign]
        result = await runner.run_until_position(target_position=target)
        assert result.completed is False and result.events_processed < 100 and result.error is None

    @pytest.mark.asyncio
    async def test_lag_clears_after_stop_mid_batch_and_resume(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test lag doesn't accumulate phantom counts when a batch is abandoned mid-delivery."""
        await add_events_to_store(event_store, 10)
        target = await current(event_store)
        subscription = Subscription(
            name="StopResumeTest", config=SubscriptionConfig(batch_size=10), subscriber=subscriber
        )
        first_runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        original_handle = subscriber.handle

        async def stopping_handle(event: DomainEvent) -> None:
            await original_handle(event)
            if len(subscriber.handled_events) == 4:
                await first_runner.stop()

        subscriber.handle = stopping_handle  # type: ignore[method-assign]
        first_result = await first_runner.run_until_position(target_position=target)
        assert first_result.completed is False and first_result.events_processed == 4

        await subscription.transition_to(SubscriptionState.PAUSED)
        subscriber.handle = original_handle  # type: ignore[method-assign]
        second_runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        second_result = await second_runner.run_until_position(target_position=target)

        assert second_result.completed is True
        assert len(subscriber.handled_events) == 10 and subscription.lag == 0
