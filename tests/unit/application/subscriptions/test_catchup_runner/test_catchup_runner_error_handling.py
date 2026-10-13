"""Error handling tests for CatchUpRunner."""

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import CatchUpRunner
from tests.unit.application.subscriptions.test_catchup_runner._fixtures import (
    MockSubscriber,
    add_events_to_store,
    current,
)


class TestCatchUpRunnerErrorHandling:
    """Tests for error handling behavior."""

    @pytest.mark.asyncio
    async def test_continue_on_error_true(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test continue_on_error=True continues after handler failure."""
        # Add events - all will fail since they're SampleTestEvent
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        # Subscriber that fails on SampleTestEvent
        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("SampleTestEvent")

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=True,  # Should continue
        )
        subscription = Subscription(
            name="ContinueOnErrorTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        # Should complete despite errors
        assert result.completed is True
        assert result.events_processed == 3
        # Subscription should track failures
        assert subscription.events_failed == 3  # All fail since type is SampleTestEvent

    @pytest.mark.asyncio
    async def test_continue_on_error_false_raises(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test continue_on_error=False propagates handler failure."""
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        # Subscriber that fails
        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("SampleTestEvent")

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=False,  # Should stop on error
        )
        subscription = Subscription(
            name="StopOnErrorTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        # Should not complete and should have error
        assert result.completed is False
        assert result.error is not None
        assert isinstance(result.error, ValueError)
        assert subscription.events_failed == 1

    @pytest.mark.asyncio
    async def test_error_recorded_in_subscription(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test that errors are recorded in subscription statistics."""
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("SampleTestEvent")

        config = SubscriptionConfig(continue_on_error=True)
        subscription = Subscription(
            name="ErrorRecordTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        await runner.run_until_position(target_position=target)

        assert subscription.events_failed == 5
        assert subscription.last_error is not None
