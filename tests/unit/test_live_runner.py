"""
Unit tests for the LiveRunner.

Tests cover:
- Basic live event processing
- Event bus subscription management
- Duplicate detection by position
- Buffer mode for catch-up to live transition
- Checkpoint strategies (EVERY_EVENT, EVERY_BATCH, PERIODIC)
- Error handling with continue_on_error
- Graceful stop functionality
- Statistics tracking
"""

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.subscriptions.runners import LiveRunner, LiveRunnerStats

# --- Sample Event Classes ---


class LiveTestEvent(DomainEvent):
    """Simple test event for live processing."""

    event_type: str = "LiveTestEvent"
    aggregate_type: str = "LiveAggregate"
    data: str = "test"


class AnotherTestEvent(DomainEvent):
    """Another test event type."""

    event_type: str = "AnotherTestEvent"
    aggregate_type: str = "LiveAggregate"
    value: int = 0


# --- Mock Subscriber ---


class MockLiveSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()
        self.handle_delay: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveTestEvent, AnotherTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.handle_delay > 0:
            await asyncio.sleep(self.handle_delay)
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


# --- Fixtures ---


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh InMemoryEventBus."""
    return InMemoryEventBus(enable_tracing=False)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def subscriber() -> MockLiveSubscriber:
    """Create a test subscriber."""
    return MockLiveSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )


@pytest.fixture
def subscription(subscriber: MockLiveSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription."""
    return Subscription(
        name="LiveTestSubscription",
        config=config,
        subscriber=subscriber,
    )


@pytest.fixture
def runner(
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscription: Subscription,
) -> LiveRunner:
    """Create a LiveRunner."""
    return LiveRunner(
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        subscription=subscription,
    )


# --- LiveRunnerStats Tests ---


class TestLiveRunnerStats:
    """Tests for LiveRunnerStats dataclass."""

    def test_default_values(self):
        """Test default values are all zero."""
        stats = LiveRunnerStats()
        assert stats.events_received == 0
        assert stats.events_processed == 0
        assert stats.events_skipped_duplicate == 0
        assert stats.events_failed == 0

    def test_custom_values(self):
        """Test custom values are set correctly."""
        stats = LiveRunnerStats(
            events_received=100,
            events_processed=90,
            events_skipped_duplicate=5,
            events_failed=5,
        )
        assert stats.events_received == 100
        assert stats.events_processed == 90
        assert stats.events_skipped_duplicate == 5
        assert stats.events_failed == 5


# --- Basic LiveRunner Tests ---


class TestLiveRunnerBasic:
    """Basic tests for LiveRunner."""

    @pytest.mark.asyncio
    async def test_runner_initialization(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscription: Subscription,
    ):
        """Test runner initializes correctly."""
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            subscription=subscription,
        )

        assert runner.event_bus is event_bus
        assert runner.checkpoint_repo is checkpoint_repo
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


# --- Event Processing Tests ---


class TestLiveRunnerEventProcessing:
    """Tests for event processing behavior."""

    @pytest.mark.asyncio
    async def test_processes_published_events(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        subscriber: MockLiveSubscriber,
    ):
        """Test runner processes events published to the bus."""
        await runner.start()

        # Publish an event
        event = LiveTestEvent(aggregate_id=uuid4(), data="test_data")
        await event_bus.publish([event])

        # Wait briefly for async processing
        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 1
        assert subscriber.handled_events[0].data == "test_data"
        assert runner.stats.events_received == 1
        assert runner.stats.events_processed == 1

    @pytest.mark.asyncio
    async def test_processes_multiple_event_types(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        subscriber: MockLiveSubscriber,
    ):
        """Test runner processes multiple event types."""
        await runner.start()

        # Publish different event types
        event1 = LiveTestEvent(aggregate_id=uuid4(), data="first")
        event2 = AnotherTestEvent(aggregate_id=uuid4(), value=42)
        await event_bus.publish([event1, event2])

        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 2
        assert runner.stats.events_processed == 2

    @pytest.mark.asyncio
    async def test_statistics_updated_correctly(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
    ):
        """Test statistics are updated correctly."""
        await runner.start()

        # Publish 5 events
        for i in range(5):
            event = LiveTestEvent(aggregate_id=uuid4(), data=f"event_{i}")
            await event_bus.publish([event])

        await asyncio.sleep(0.01)

        stats = runner.stats
        assert stats.events_received == 5
        assert stats.events_processed == 5
        assert stats.events_failed == 0


# --- Buffer Mode Tests ---


class TestLiveRunnerBufferMode:
    """Tests for buffer mode during catch-up to live transition."""

    @pytest.mark.asyncio
    async def test_buffers_events_when_enabled(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        subscriber: MockLiveSubscriber,
    ):
        """Test events are buffered when buffer_events=True."""
        await runner.start(buffer_events=True)

        # Publish events
        for i in range(3):
            event = LiveTestEvent(aggregate_id=uuid4(), data=f"buffered_{i}")
            await event_bus.publish([event])

        await asyncio.sleep(0.01)

        # Events should be buffered, not processed
        assert len(subscriber.handled_events) == 0
        assert runner.buffer_size == 3
        assert runner.stats.events_received == 3
        assert runner.stats.events_processed == 0

    @pytest.mark.asyncio
    async def test_process_buffer_delivers_buffered_events(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        subscriber: MockLiveSubscriber,
    ):
        """Test process_buffer() delivers buffered events."""
        await runner.start(buffer_events=True)

        # Buffer some events
        for i in range(3):
            event = LiveTestEvent(aggregate_id=uuid4(), data=f"buffered_{i}")
            await event_bus.publish([event])

        await asyncio.sleep(0.01)
        assert runner.buffer_size == 3

        # Process the buffer
        processed = await runner.process_buffer()

        assert processed == 3
        assert len(subscriber.handled_events) == 3
        assert runner.buffer_size == 0

    @pytest.mark.asyncio
    async def test_disable_buffer_transitions_to_live(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test disable_buffer() transitions to LIVE state."""
        await runner.start(buffer_events=True)
        assert subscription.state == SubscriptionState.STARTING

        await runner.disable_buffer()

        assert subscription.state == SubscriptionState.LIVE
        # Note: buffer is disabled but not cleared
        assert runner._buffer_enabled is False

    @pytest.mark.asyncio
    async def test_events_processed_directly_after_buffer_disabled(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        subscriber: MockLiveSubscriber,
    ):
        """Test events are processed directly after buffer is disabled."""
        await runner.start(buffer_events=True)

        # Disable buffer
        await runner.disable_buffer()

        # Now events should be processed directly
        event = LiveTestEvent(aggregate_id=uuid4(), data="direct")
        await event_bus.publish([event])

        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 1


# --- Duplicate Detection Tests ---


class TestLiveRunnerDuplicateDetection:
    """Tests for duplicate event detection."""

    @pytest.mark.asyncio
    async def test_skips_duplicate_by_position(
        self,
        runner: LiveRunner,
        subscription: Subscription,
        subscriber: MockLiveSubscriber,
    ):
        """Test events with position <= last_processed_position are skipped."""
        # Set last processed position
        subscription.last_processed_position = 100

        await runner.start()

        # Create an event with position attached (simulating position metadata)
        event = LiveTestEvent(aggregate_id=uuid4(), data="old_event")
        event._global_position = 50  # Position before last processed

        # Process directly through internal method
        await runner._handle_live_event(event)

        assert len(subscriber.handled_events) == 0
        assert runner.stats.events_skipped_duplicate == 1

    @pytest.mark.asyncio
    async def test_processes_event_with_higher_position(
        self,
        runner: LiveRunner,
        subscription: Subscription,
        subscriber: MockLiveSubscriber,
    ):
        """Test events with position > last_processed_position are processed."""
        subscription.last_processed_position = 100

        await runner.start()

        # Create an event with position after last processed
        event = LiveTestEvent(aggregate_id=uuid4(), data="new_event")
        event._global_position = 150

        await runner._handle_live_event(event)

        assert len(subscriber.handled_events) == 1
        assert runner.stats.events_skipped_duplicate == 0

    @pytest.mark.asyncio
    async def test_processes_event_without_position(
        self,
        runner: LiveRunner,
        subscriber: MockLiveSubscriber,
    ):
        """Test events without position metadata are processed."""
        await runner.start()

        # Event without position (normal case for live events)
        event = LiveTestEvent(aggregate_id=uuid4(), data="no_position")
        await runner._handle_live_event(event)

        assert len(subscriber.handled_events) == 1


# --- Checkpoint Strategy Tests ---


class TestLiveRunnerCheckpointStrategies:
    """Tests for different checkpoint strategies."""

    @pytest.mark.asyncio
    async def test_every_event_strategy_checkpoints(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
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
            subscription=subscription,
        )
        await runner.start()

        # Create event with position
        event = LiveTestEvent(aggregate_id=uuid4(), data="checkpoint_test")
        event._global_position = 42

        await runner._handle_live_event(event)

        # Check checkpoint was saved
        position = await checkpoint_repo.get_position("EveryEventLive")
        assert position == 42

    @pytest.mark.asyncio
    async def test_every_batch_strategy_checkpoints_live(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
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
            subscription=subscription,
        )
        await runner.start()

        # Create event with position
        event = LiveTestEvent(aggregate_id=uuid4(), data="batch_test")
        event._global_position = 55

        await runner._handle_live_event(event)

        # In live mode, EVERY_BATCH behaves like EVERY_EVENT
        position = await checkpoint_repo.get_position("EveryBatchLive")
        assert position == 55

    @pytest.mark.asyncio
    async def test_periodic_strategy_respects_interval(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockLiveSubscriber,
    ):
        """Test PERIODIC strategy only saves when interval has elapsed."""
        config = SubscriptionConfig(
            checkpoint_strategy=CheckpointStrategy.PERIODIC,
            checkpoint_interval_seconds=0.1,  # 100ms interval
        )
        subscription = Subscription(
            name="PeriodicLive",
            config=config,
            subscriber=subscriber,
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            subscription=subscription,
        )
        await runner.start()

        # First event - should not checkpoint (interval not elapsed)
        event1 = LiveTestEvent(aggregate_id=uuid4(), data="first")
        event1._global_position = 10
        await runner._handle_live_event(event1)

        position = await checkpoint_repo.get_position("PeriodicLive")
        assert position is None  # No checkpoint yet

        # Wait for interval to elapse
        await asyncio.sleep(0.15)

        # Second event - should checkpoint now
        event2 = LiveTestEvent(aggregate_id=uuid4(), data="second")
        event2._global_position = 20
        await runner._handle_live_event(event2)

        position = await checkpoint_repo.get_position("PeriodicLive")
        assert position == 20

    @pytest.mark.asyncio
    async def test_no_checkpoint_without_position(
        self,
        runner: LiveRunner,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test no checkpoint is saved when event has no position."""
        await runner.start()

        # Event without position
        event = LiveTestEvent(aggregate_id=uuid4(), data="no_pos")
        await runner._handle_live_event(event)

        # No checkpoint should be saved
        position = await checkpoint_repo.get_position("LiveTestSubscription")
        assert position is None


# --- Error Handling Tests ---


class TestLiveRunnerErrorHandling:
    """Tests for error handling behavior."""

    @pytest.mark.asyncio
    async def test_continue_on_error_true(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
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
            subscription=subscription,
        )
        await runner.start()

        # Publish failing event
        event = LiveTestEvent(aggregate_id=uuid4(), data="fail")
        await runner._handle_live_event(event)

        # Should record failure but continue
        assert runner.stats.events_failed == 1
        assert subscription.events_failed == 1
        assert subscription.last_error is not None

    @pytest.mark.asyncio
    async def test_continue_on_error_false_raises(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
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
            subscription=subscription,
        )
        await runner.start()

        # Should raise the exception
        event = LiveTestEvent(aggregate_id=uuid4(), data="fail")
        with pytest.raises(ValueError):
            await runner._handle_live_event(event)

        assert runner.stats.events_failed == 1


# --- Module Imports Tests ---


class TestLiveRunnerImports:
    """Tests for module imports and exports."""

    def test_import_from_runners_module(self):
        """Test LiveRunner can be imported from runners module."""
        from eventsource.subscriptions.runners import LiveRunner, LiveRunnerStats

        assert LiveRunner is not None
        assert LiveRunnerStats is not None

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from eventsource.subscriptions.runners import live

        assert "LiveRunner" in live.__all__
        assert "LiveRunnerStats" in live.__all__

    def test_runners_module_exports_all(self):
        """Test runners module exports all expected classes."""
        from eventsource.subscriptions import runners

        assert hasattr(runners, "LiveRunner")
        assert hasattr(runners, "LiveRunnerStats")
        assert hasattr(runners, "CatchUpRunner")
        assert hasattr(runners, "CatchUpResult")


# --- Edge Cases Tests ---


class TestLiveRunnerEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_buffer_process(
        self,
        runner: LiveRunner,
    ):
        """Test process_buffer() with empty buffer."""
        await runner.start(buffer_events=True)

        # Process empty buffer
        processed = await runner.process_buffer()

        assert processed == 0

    @pytest.mark.asyncio
    async def test_subscription_position_updated_with_position(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test subscription position is updated when event has position."""
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="positioned")
        event._global_position = 999

        await runner._handle_live_event(event)

        assert subscription.last_processed_position == 999
        assert subscription.last_event_id == event.event_id
        assert subscription.last_event_type == "LiveTestEvent"

    @pytest.mark.asyncio
    async def test_events_processed_counter_updated(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test events_processed is incremented."""
        await runner.start()

        for i in range(3):
            event = LiveTestEvent(aggregate_id=uuid4(), data=f"event_{i}")
            await runner._handle_live_event(event)

        assert subscription.events_processed == 3

    @pytest.mark.asyncio
    async def test_last_processed_at_updated(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test last_processed_at timestamp is updated."""
        await runner.start()
        assert subscription.last_processed_at is None

        event = LiveTestEvent(aggregate_id=uuid4(), data="timestamp_test")
        await runner._handle_live_event(event)

        assert subscription.last_processed_at is not None
        assert isinstance(subscription.last_processed_at, datetime)

    @pytest.mark.asyncio
    async def test_buffer_queue_is_asyncio_safe(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
    ):
        """Test buffer queue handles concurrent access safely."""
        await runner.start(buffer_events=True)

        # Publish many events concurrently
        async def publish_event(i: int) -> None:
            event = LiveTestEvent(aggregate_id=uuid4(), data=f"concurrent_{i}")
            await event_bus.publish([event])

        await asyncio.gather(*[publish_event(i) for i in range(10)])
        await asyncio.sleep(0.05)

        assert runner.buffer_size == 10
        assert runner.stats.events_received == 10

    @pytest.mark.asyncio
    async def test_properties_accessible_after_stop(
        self,
        runner: LiveRunner,
    ):
        """Test properties are still accessible after stop."""
        await runner.start()
        await runner.stop()

        # Should be able to access these properties
        assert runner.is_running is False
        assert runner.buffer_size == 0
        assert runner.stats is not None

    @pytest.mark.asyncio
    async def test_multiple_position_updates(
        self,
        runner: LiveRunner,
        subscription: Subscription,
    ):
        """Test position is updated correctly with multiple events."""
        await runner.start()

        positions = [10, 20, 30]
        for pos in positions:
            event = LiveTestEvent(aggregate_id=uuid4(), data=f"pos_{pos}")
            event._global_position = pos
            await runner._handle_live_event(event)

        assert subscription.last_processed_position == 30

    @pytest.mark.asyncio
    async def test_config_accessible_on_runner(
        self,
        runner: LiveRunner,
        config: SubscriptionConfig,
    ):
        """Test config is accessible on runner."""
        assert runner.config is config
        assert runner.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT
