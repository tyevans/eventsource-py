"""
Unit tests for the CatchUpRunner.

Tests cover:
- Basic catch-up operation
- Batch processing with configured batch sizes
- Checkpoint strategies (EVERY_EVENT, EVERY_BATCH, PERIODIC)
- Progress tracking and completion detection
- Error handling with continue_on_error
- Graceful stop functionality
- Edge cases (empty store, target already reached)
"""

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.application.subscriptions.runners import CatchUpResult, CatchUpRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import ExpectedVersion, Position

# --- Sample Event Classes ---


class SampleTestEvent(DomainEvent):
    """Simple test event."""

    aggregate_type: str = "SampleAggregate"
    data: str = "test"


class SampleFailingEvent(DomainEvent):
    """Event that should cause handler to fail."""

    aggregate_type: str = "SampleAggregate"
    should_fail: bool = True


# --- Mock Subscriber ---


class MockSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SampleTestEvent, SampleFailingEvent]

    async def handle(self, event: DomainEvent) -> None:
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


# --- Fixtures ---


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore (a real GlobalEventFeed)."""
    return InMemoryEventStore()


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def subscriber() -> MockSubscriber:
    """Create a test subscriber."""
    return MockSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )


@pytest.fixture
def subscription(subscriber: MockSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription."""
    return Subscription(
        name="TestSubscription",
        config=config,
        subscriber=subscriber,
    )


@pytest.fixture
def runner(
    event_store: InMemoryEventStore,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscription: Subscription,
) -> CatchUpRunner:
    """Create a CatchUpRunner."""
    return CatchUpRunner(event_store, checkpoint_repo, subscription)


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = SampleTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events


async def current(store: InMemoryEventStore) -> Position:
    """The feed's current position, asserted present (the store is non-empty)."""
    position = await store.current_position()
    assert position is not None
    return position


async def position_after(store: InMemoryEventStore, n: int) -> Position:
    """The token of the nth (1-based) event in the feed."""
    envelopes = [e async for e in store.read_all()]
    position = envelopes[n - 1].position
    assert position is not None
    return position


# --- CatchUpResult Tests ---


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


# --- Basic CatchUpRunner Tests ---


class TestCatchUpRunnerBasic:
    """Basic tests for CatchUpRunner."""

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
    async def test_empty_event_store_returns_completed(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
    ):
        """Test catch-up on empty store returns completed with 0 events.

        An empty feed has no current position, so the coordinator would
        skip catch-up entirely; a runner pointed at any target still
        completes on the first empty read.
        """
        assert await event_store.current_position() is None

        result = await runner.run_until_position(
            target_position=Position(store_id="memory", key=(1,))
        )

        assert result.completed is True
        assert result.events_processed == 0
        assert result.final_position is None
        assert result.error is None

    @pytest.mark.asyncio
    async def test_target_position_already_reached(
        self,
        runner: CatchUpRunner,
        subscription: Subscription,
    ):
        """Test catch-up returns immediately if already at target position."""
        # Set subscription position to target
        position = Position(store_id="memory", key=(100,))
        subscription.last_processed_position = position

        result = await runner.run_until_position(target_position=position)

        assert result.completed is True
        assert result.events_processed == 0
        assert result.final_position == position

    @pytest.mark.asyncio
    async def test_processes_all_events(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        """Test runner processes all events up to target position."""
        # Add 5 events
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 5
        assert result.final_position == target
        assert len(subscriber.handled_events) == 5

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


# --- Batch Processing Tests ---


class TestCatchUpRunnerBatching:
    """Tests for batch processing behavior."""

    @pytest.mark.asyncio
    async def test_respects_batch_size(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test runner processes events in batches of configured size."""
        # Add 25 events
        await add_events_to_store(event_store, 25)
        target = await current(event_store)

        # Create config with small batch size
        config = SubscriptionConfig(
            batch_size=5,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(
            name="BatchTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 25
        # With batch_size=5 and EVERY_BATCH strategy, we expect 5 checkpoints
        checkpoint = await checkpoint_repo.get_position("BatchTest")
        assert checkpoint == target

    @pytest.mark.asyncio
    async def test_batch_smaller_than_remaining(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test batch size is limited to remaining events to target."""
        # Add 3 events
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        # Config with large batch size
        config = SubscriptionConfig(batch_size=100)
        subscription = Subscription(
            name="SmallBatchTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 3


# --- Checkpoint Strategy Tests ---


class TestCatchUpRunnerCheckpointStrategies:
    """Tests for different checkpoint strategies."""

    @pytest.mark.asyncio
    async def test_every_event_strategy(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test EVERY_EVENT strategy saves checkpoint after each event."""
        # Add 5 events
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        )
        subscription = Subscription(
            name="EveryEventTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 5
        # Check final checkpoint
        checkpoint = await checkpoint_repo.get_position("EveryEventTest")
        assert checkpoint == target

    @pytest.mark.asyncio
    async def test_every_batch_strategy(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test EVERY_BATCH strategy saves checkpoint after each batch."""
        # Add 15 events
        await add_events_to_store(event_store, 15)
        target = await current(event_store)

        config = SubscriptionConfig(
            batch_size=5,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(
            name="EveryBatchTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 15
        checkpoint = await checkpoint_repo.get_position("EveryBatchTest")
        assert checkpoint == target

    @pytest.mark.asyncio
    async def test_periodic_strategy(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test PERIODIC strategy saves checkpoint on time intervals.

        Note: With PERIODIC strategy, checkpoints are only saved when the
        configured interval has elapsed. This test uses a very short interval
        and adds a small delay to ensure at least one checkpoint is saved.
        """
        # Add events
        await add_events_to_store(event_store, 10)
        target = await current(event_store)

        # Create a slow subscriber that delays slightly
        class SlowSubscriber:
            def __init__(self) -> None:
                self.handled_events: list[DomainEvent] = []

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [SampleTestEvent]

            async def handle(self, event: DomainEvent) -> None:
                await asyncio.sleep(0.001)  # Small delay
                self.handled_events.append(event)

        subscriber = SlowSubscriber()

        # Use very short interval for testing (must be positive)
        config = SubscriptionConfig(
            batch_size=100,
            checkpoint_strategy=CheckpointStrategy.PERIODIC,
            checkpoint_interval_seconds=0.005,  # 5ms interval
        )
        subscription = Subscription(
            name="PeriodicTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 10
        # With 10 events at 1ms each, and 5ms interval, we expect checkpoints
        # The exact number depends on timing, but should have at least one
        checkpoint = await checkpoint_repo.get_position("PeriodicTest")
        assert checkpoint is not None
        assert checkpoint <= target


# --- Error Handling Tests ---


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


# --- Stop Functionality Tests ---


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


# --- Position Tracking Tests ---


class TestCatchUpRunnerPositionTracking:
    """Tests for position tracking behavior."""

    @pytest.mark.asyncio
    async def test_subscription_position_updated(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription.last_processed_position is updated correctly."""
        await add_events_to_store(event_store, 10)
        target = await current(event_store)

        assert subscription.last_processed_position is None

        await runner.run_until_position(target_position=target)

        assert subscription.last_processed_position == target

    @pytest.mark.asyncio
    async def test_subscription_event_metadata_updated(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription tracks last event ID and type."""
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        await runner.run_until_position(target_position=target)

        assert subscription.last_event_id is not None
        assert subscription.last_event_type == "SampleTestEvent"
        assert subscription.events_processed == 3

    @pytest.mark.asyncio
    async def test_events_seen_tracked_for_lag(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription lag is tracked by counting events seen vs delivered."""
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        # Initial lag should be 0 (nothing seen yet)
        assert subscription.lag == 0

        await runner.run_until_position(target_position=target)

        # After catching up, all seen events have been delivered
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_checkpoint_persisted_correctly(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test checkpoint is persisted with correct data."""
        await add_events_to_store(event_store, 5)
        target = await current(event_store)

        await runner.run_until_position(target_position=target)

        checkpoint = await checkpoint_repo.get_position("TestSubscription")
        assert checkpoint == target


# --- Module Imports Tests ---


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


# --- Edge Cases Tests ---


class TestCatchUpRunnerEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_zero_batch_size_validation(self):
        """Test that batch_size=0 is rejected by config."""
        with pytest.raises(ValueError):
            SubscriptionConfig(batch_size=0)

    @pytest.mark.asyncio
    async def test_target_behind_the_feed_delivers_nothing(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        """A target behind every stored event stops before the first one.

        This replaces the old negative-target case: positions are opaque
        tokens, so "before the beginning" is expressed as a token that
        orders below the feed's first event, not as -1.
        """
        await add_events_to_store(event_store, 3)

        result = await runner.run_until_position(
            target_position=Position(store_id="memory", key=(0,))
        )

        assert result.completed is True
        assert result.events_processed == 0
        assert subscriber.handled_events == []

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test catch-up resumes from existing checkpoint position."""
        # Add 10 events
        await add_events_to_store(event_store, 10)
        target = await current(event_store)

        # Set subscription to start from position 5
        config = SubscriptionConfig(batch_size=100)
        subscription = Subscription(
            name="ResumeTest",
            config=config,
            subscriber=subscriber,
        )
        subscription.last_processed_position = await position_after(event_store, 5)

        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        result = await runner.run_until_position(target_position=target)

        # Should only process events 6-10
        assert result.completed is True
        assert result.events_processed == 5
        assert len(subscriber.handled_events) == 5

    @pytest.mark.asyncio
    async def test_single_event(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockSubscriber,
    ):
        """Test catch-up with exactly one event."""
        await add_events_to_store(event_store, 1)
        target = await current(event_store)

        config = SubscriptionConfig()
        subscription = Subscription(
            name="SingleEventTest",
            config=config,
            subscriber=subscriber,
        )
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(target_position=target)

        assert result.completed is True
        assert result.events_processed == 1
        assert len(subscriber.handled_events) == 1

    @pytest.mark.asyncio
    async def test_last_processed_at_updated(
        self,
        runner: CatchUpRunner,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test last_processed_at timestamp is updated."""
        await add_events_to_store(event_store, 3)
        target = await current(event_store)

        assert subscription.last_processed_at is None

        await runner.run_until_position(target_position=target)

        assert subscription.last_processed_at is not None
        assert isinstance(subscription.last_processed_at, datetime)
