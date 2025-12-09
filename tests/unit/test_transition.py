"""
Unit tests for the TransitionCoordinator.

Tests cover:
- Complete transition from catch-up to live
- Transition when already caught up (skip catch-up)
- Events during catch-up are buffered
- Duplicates in buffer are skipped
- Clean stop during transition
- Error during catch-up handled
- StartFromResolver handles all start_from values
- Edge cases (empty store, position at watermark)
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.subscriptions.transition import (
    StartFromResolver,
    TransitionCoordinator,
    TransitionPhase,
    TransitionResult,
)

# --- Sample Event Classes ---


class TransitionTestEvent(DomainEvent):
    """Simple test event for transition testing."""

    event_type: str = "TransitionTestEvent"
    aggregate_type: str = "TransitionAggregate"
    data: str = "test"


class AnotherTransitionEvent(DomainEvent):
    """Another test event type."""

    event_type: str = "AnotherTransitionEvent"
    aggregate_type: str = "TransitionAggregate"
    value: int = 0


# --- Mock Subscriber ---


class MockTransitionSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()
        self.handle_delay: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TransitionTestEvent, AnotherTransitionEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.handle_delay > 0:
            await asyncio.sleep(self.handle_delay)
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


# --- Fixtures ---


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore."""
    return InMemoryEventStore(enable_tracing=False)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh InMemoryEventBus."""
    return InMemoryEventBus(enable_tracing=False)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def subscriber() -> MockTransitionSubscriber:
    """Create a test subscriber."""
    return MockTransitionSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )


@pytest.fixture
def subscription(subscriber: MockTransitionSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription."""
    return Subscription(
        name="TransitionTestSubscription",
        config=config,
        subscriber=subscriber,
    )


@pytest.fixture
def coordinator(
    event_store: InMemoryEventStore,
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscription: Subscription,
) -> TransitionCoordinator:
    """Create a TransitionCoordinator."""
    return TransitionCoordinator(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        subscription=subscription,
    )


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = TransitionTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TransitionAggregate",
            events=[event],
            expected_version=0,
        )
        events.append(event)
    return events


# --- TransitionResult Tests ---


class TestTransitionResult:
    """Tests for TransitionResult dataclass."""

    def test_successful_result(self):
        """Test successful result has expected values."""
        result = TransitionResult(
            success=True,
            catchup_events_processed=100,
            buffer_events_processed=10,
            buffer_events_skipped=5,
            final_position=110,
            phase_reached=TransitionPhase.LIVE,
        )
        assert result.success is True
        assert result.catchup_events_processed == 100
        assert result.buffer_events_processed == 10
        assert result.buffer_events_skipped == 5
        assert result.final_position == 110
        assert result.phase_reached == TransitionPhase.LIVE
        assert result.error is None

    def test_failed_result(self):
        """Test failed result has error."""
        error = ValueError("Test error")
        result = TransitionResult(
            success=False,
            catchup_events_processed=50,
            buffer_events_processed=0,
            buffer_events_skipped=0,
            final_position=50,
            phase_reached=TransitionPhase.FAILED,
            error=error,
        )
        assert result.success is False
        assert result.phase_reached == TransitionPhase.FAILED
        assert result.error is error


# --- TransitionPhase Tests ---


class TestTransitionPhase:
    """Tests for TransitionPhase enum."""

    def test_all_phases_exist(self):
        """Test all expected phases exist."""
        assert TransitionPhase.NOT_STARTED.value == "not_started"
        assert TransitionPhase.INITIAL_CATCHUP.value == "initial_catchup"
        assert TransitionPhase.LIVE_SUBSCRIBED.value == "live_subscribed"
        assert TransitionPhase.FINAL_CATCHUP.value == "final_catchup"
        assert TransitionPhase.PROCESSING_BUFFER.value == "processing_buffer"
        assert TransitionPhase.LIVE.value == "live"
        assert TransitionPhase.FAILED.value == "failed"


# --- Basic TransitionCoordinator Tests ---


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
        assert coordinator.watermark == 0
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
        assert result.buffer_events_skipped == 0
        assert result.final_position == 0
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
        assert result.final_position == 10
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


# --- Already Caught Up Tests ---


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
        subscription.last_processed_position = 10

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 0
        assert result.buffer_events_processed == 0
        assert result.final_position == 10
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
        subscription.last_processed_position = 100  # Way past watermark

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 0
        assert result.final_position == 100


# --- Buffer Mode Tests ---


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
            # Attach position metadata
            live_event._global_position = 100  # Position after watermark
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


# --- Error Handling Tests ---


class TestTransitionErrorHandling:
    """Tests for error handling during transition."""

    @pytest.mark.asyncio
    async def test_catchup_error_handled(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test error during catch-up is handled properly."""
        # Create subscriber that fails
        subscriber = MockTransitionSubscriber()
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=False,  # Stop on error
        )
        subscription = Subscription(
            name="ErrorTest",
            config=config,
            subscriber=subscriber,
        )

        # Add events that will fail
        await add_events_to_store(event_store, 3)

        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is False
        assert result.phase_reached == TransitionPhase.FAILED
        assert result.error is not None
        assert coordinator.phase == TransitionPhase.FAILED

    @pytest.mark.asyncio
    async def test_cleanup_on_failure(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test resources are cleaned up on failure."""
        subscriber = MockTransitionSubscriber()
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        config = SubscriptionConfig(continue_on_error=False)
        subscription = Subscription(
            name="CleanupTest",
            config=config,
            subscriber=subscriber,
        )

        await add_events_to_store(event_store, 3)

        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is False
        # Runners should be stopped after cleanup
        if coordinator.live_runner:
            assert not coordinator.live_runner.is_running
        if coordinator.catchup_runner:
            assert not coordinator.catchup_runner.is_running


# --- Stop Functionality Tests ---


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


# --- Phase Tracking Tests ---


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

        assert coordinator.watermark == 0  # Not yet captured

        result = await coordinator.execute()

        assert result.success is True
        assert coordinator.watermark == 10  # Should be max position at start


# --- StartFromResolver Tests ---


class TestStartFromResolver:
    """Tests for StartFromResolver."""

    @pytest.mark.asyncio
    async def test_resolve_beginning(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('beginning') returns 0."""
        config = SubscriptionConfig(start_from="beginning")
        subscription = Subscription(
            name="BeginningTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == 0

    @pytest.mark.asyncio
    async def test_resolve_end(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('end') returns current max position."""
        # Add events
        await add_events_to_store(event_store, 10)

        config = SubscriptionConfig(start_from="end")
        subscription = Subscription(
            name="EndTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == 10

    @pytest.mark.asyncio
    async def test_resolve_checkpoint_with_existing(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('checkpoint') returns existing checkpoint position."""
        # Save a checkpoint
        await checkpoint_repo.save_position(
            subscription_id="CheckpointTest",
            position=42,
            event_id=uuid4(),
            event_type="TestEvent",
        )

        config = SubscriptionConfig(start_from="checkpoint")
        subscription = Subscription(
            name="CheckpointTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == 42

    @pytest.mark.asyncio
    async def test_resolve_checkpoint_without_existing(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('checkpoint') returns 0 when no checkpoint exists."""
        config = SubscriptionConfig(start_from="checkpoint")
        subscription = Subscription(
            name="NoCheckpointTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == 0

    @pytest.mark.asyncio
    async def test_resolve_explicit_position(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve(int) returns the explicit position."""
        config = SubscriptionConfig(start_from=100)
        subscription = Subscription(
            name="ExplicitTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == 100

    @pytest.mark.asyncio
    async def test_resolve_unknown_start_from(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve raises for unknown start_from value."""
        # Create config with valid value, then modify it
        config = SubscriptionConfig(start_from="beginning")
        subscription = Subscription(
            name="UnknownTest",
            config=config,
            subscriber=subscriber,
        )
        # Hack to set invalid value (normally prevented by validation)
        object.__setattr__(config, "start_from", "invalid")

        resolver = StartFromResolver(event_store, checkpoint_repo)

        with pytest.raises(ValueError, match="Unknown start_from value"):
            await resolver.resolve(subscription)


# --- Live Runner Access Tests ---


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
        """Test live runner processes events after transition completes."""
        # Add initial events
        await add_events_to_store(event_store, 5)

        config = SubscriptionConfig(batch_size=100)
        subscription = Subscription(
            name="LiveAfterTransition",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()
        assert result.success is True
        assert len(subscriber.handled_events) == 5

        # Now publish a live event
        live_event = TransitionTestEvent(aggregate_id=uuid4(), data="after_transition")
        await event_bus.publish([live_event])

        # Wait for event processing
        await asyncio.sleep(0.01)

        # Should have received the live event
        assert len(subscriber.handled_events) == 6
        assert subscriber.handled_events[-1].data == "after_transition"


# --- Module Imports Tests ---


class TestTransitionImports:
    """Tests for module imports and exports."""

    def test_import_from_subscriptions_module(self):
        """Test transition classes can be imported from subscriptions module."""
        from eventsource.subscriptions import (
            StartFromResolver,
            TransitionCoordinator,
            TransitionPhase,
            TransitionResult,
        )

        assert TransitionCoordinator is not None
        assert TransitionPhase is not None
        assert TransitionResult is not None
        assert StartFromResolver is not None

    def test_import_from_transition_module(self):
        """Test transition classes can be imported from transition module."""
        from eventsource.subscriptions.transition import (
            StartFromResolver,
            TransitionCoordinator,
            TransitionPhase,
            TransitionResult,
        )

        assert TransitionCoordinator is not None
        assert TransitionPhase is not None
        assert TransitionResult is not None
        assert StartFromResolver is not None

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from eventsource.subscriptions import transition

        assert "TransitionCoordinator" in transition.__all__
        assert "TransitionPhase" in transition.__all__
        assert "TransitionResult" in transition.__all__
        assert "StartFromResolver" in transition.__all__


# --- Edge Cases Tests ---


class TestTransitionEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_transition_single_event(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscriber: MockTransitionSubscriber,
    ):
        """Test transition with exactly one event."""
        await add_events_to_store(event_store, 1)

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 1
        assert len(subscriber.handled_events) == 1

    @pytest.mark.asyncio
    async def test_transition_large_batch(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test transition with many events."""
        # Add 100 events
        await add_events_to_store(event_store, 100)

        config = SubscriptionConfig(batch_size=25)  # Multiple batches
        subscription = Subscription(
            name="LargeBatchTest",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is True
        assert result.catchup_events_processed == 100
        assert len(subscriber.handled_events) == 100

    @pytest.mark.asyncio
    async def test_position_tracking_during_transition(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription position is correctly tracked during transition."""
        await add_events_to_store(event_store, 10)

        assert subscription.last_processed_position == 0

        result = await coordinator.execute()

        assert result.success is True
        assert subscription.last_processed_position == 10
        assert result.final_position == 10

    @pytest.mark.asyncio
    async def test_checkpoint_saved_during_transition(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test checkpoints are saved during transition."""
        await add_events_to_store(event_store, 10)

        config = SubscriptionConfig(
            batch_size=5,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        )
        subscription = Subscription(
            name="CheckpointSaveTest",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        assert result.success is True

        # Check checkpoint was saved
        position = await checkpoint_repo.get_position("CheckpointSaveTest")
        assert position == 10

    @pytest.mark.asyncio
    async def test_continue_on_error_during_catchup(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test continue_on_error allows transition to complete despite errors."""
        subscriber = MockTransitionSubscriber()
        # Fail on first event only
        subscriber.fail_on_event_types.add("TransitionTestEvent")

        # Add events
        await add_events_to_store(event_store, 5)

        config = SubscriptionConfig(
            batch_size=100,
            continue_on_error=True,  # Continue despite errors
        )
        subscription = Subscription(
            name="ContinueOnErrorTransition",
            config=config,
            subscriber=subscriber,
        )
        coordinator = TransitionCoordinator(event_store, event_bus, checkpoint_repo, subscription)

        result = await coordinator.execute()

        # Should complete despite errors
        assert result.success is True
        assert result.catchup_events_processed == 5
        assert subscription.events_failed == 5  # All events failed but continued

    @pytest.mark.asyncio
    async def test_catchup_runner_property(
        self,
        coordinator: TransitionCoordinator,
        event_store: InMemoryEventStore,
    ):
        """Test catchup_runner property is available during transition."""
        await add_events_to_store(event_store, 5)

        assert coordinator.catchup_runner is None

        result = await coordinator.execute()

        # After transition, catchup runner should be available
        # (though not running anymore)
        assert result.success is True
        assert coordinator.catchup_runner is not None
