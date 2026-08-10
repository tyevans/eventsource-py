"""
Unit tests for the LiveRunner.

Tests cover:
- Basic live event processing
- Event bus subscription management (wake-up signal, not delivery)
- Feed-driven ordering (the global feed, not the bus payload, is authoritative)
- Buffer mode for catch-up to live transition
- Checkpoint strategies (EVERY_EVENT, EVERY_BATCH, PERIODIC)
- Error handling with continue_on_error
- Graceful stop functionality
- Statistics tracking

The bus is a wake-up signal only: `LiveRunner` drains
`GlobalEventFeed.read_all(from_position=...)` on every notification, so
tests seed events into a real `InMemoryEventStore` (a `GlobalEventFeed`)
before publishing a wake-up on the bus, mirroring `test_catchup_runner.py`'s
idiom.
"""

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.application.subscriptions.runners import LiveRunner, LiveRunnerStats
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

# --- Sample Event Classes ---


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class LiveTestEvent(DomainEvent):
    """Simple test event for live processing."""

    aggregate_type: str = "LiveAggregate"
    data: str = "test"


@register_event(registry=_REGISTRY)
class AnotherTestEvent(DomainEvent):
    """Another test event type."""

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
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore (a real GlobalEventFeed)."""
    return InMemoryEventStore(event_registry=_REGISTRY)


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
    event_store: InMemoryEventStore,
    subscription: Subscription,
) -> LiveRunner:
    """Create a LiveRunner."""
    return LiveRunner(
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        event_feed=event_store,
        subscription=subscription,
    )


async def append_event(store: InMemoryEventStore, event: DomainEvent) -> Position:
    """Append one event directly to the store, returning its feed position."""
    result = await store.append(
        StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
        [event],
        ExpectedVersion.no_stream(),
    )
    assert result.position is not None
    return result.position


async def wake(bus: InMemoryEventBus) -> None:
    """Publish a bus notification to wake a live runner into draining the feed.

    The published event's content is never delivered -- only events already
    committed to the store (via `append_event`) are; this call exists purely
    to trigger `_handle_live_event`.
    """
    await bus.publish([LiveTestEvent(aggregate_id=uuid4(), data="__wake__")])


# --- LiveRunnerStats Tests ---


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


# --- Basic LiveRunner Tests ---


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


# --- Event Processing Tests ---


class TestLiveRunnerEventProcessing:
    """Tests for event processing behavior."""

    @pytest.mark.asyncio
    async def test_processes_published_events(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test runner drains an event committed to the store on wake-up."""
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="test_data")
        await append_event(event_store, event)
        await wake(event_bus)

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
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test runner processes multiple event types in one drain."""
        await runner.start()

        event1 = LiveTestEvent(aggregate_id=uuid4(), data="first")
        event2 = AnotherTestEvent(aggregate_id=uuid4(), value=42)
        await append_event(event_store, event1)
        await append_event(event_store, event2)
        await wake(event_bus)

        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 2
        assert runner.stats.events_processed == 2

    @pytest.mark.asyncio
    async def test_statistics_updated_correctly(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
    ):
        """Test statistics are updated correctly."""
        await runner.start()

        for i in range(5):
            await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"event_{i}"))
        await wake(event_bus)

        await asyncio.sleep(0.01)

        stats = runner.stats
        assert stats.events_received == 5
        assert stats.events_processed == 5
        assert stats.events_failed == 0


# --- Buffer Mode Tests ---


class TestLiveRunnerBufferMode:
    """Tests for buffer mode during catch-up to live transition."""

    @pytest.mark.asyncio
    async def test_buffers_wake_ups_when_enabled(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test wake-ups are buffered (not drained) when buffer_events=True."""
        await runner.start(buffer_events=True)

        for i in range(3):
            await append_event(
                event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"buffered_{i}")
            )
            await wake(event_bus)

        await asyncio.sleep(0.01)

        # Nothing drained yet -- feed has events, but buffering held them off
        assert len(subscriber.handled_events) == 0
        assert runner.buffer_size == 3
        assert runner.stats.events_received == 0
        assert runner.stats.events_processed == 0

    @pytest.mark.asyncio
    async def test_process_buffer_drains_the_feed(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test process_buffer() drains everything committed since the checkpoint."""
        await runner.start(buffer_events=True)

        for i in range(3):
            await append_event(
                event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"buffered_{i}")
            )
        await wake(event_bus)

        await asyncio.sleep(0.01)
        assert runner.buffer_size == 1

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
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test events are drained directly after buffer is disabled."""
        await runner.start(buffer_events=True)
        await runner.disable_buffer()

        event = LiveTestEvent(aggregate_id=uuid4(), data="direct")
        await append_event(event_store, event)
        await wake(event_bus)

        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 1


# --- Feed Ordering Tests ---
#
# The catch-up-to-live duplicate suppression that used to live in
# `_process_live_event` is gone: the global feed is the single ordered
# source (ADR 0047), and `read_all(from_position=...)` never returns a
# position at or before the checkpoint, so there is nothing to deduplicate.


class TestLiveRunnerFeedOrdering:
    """Tests that draining only ever moves forward from the checkpoint."""

    @pytest.mark.asyncio
    async def test_drains_only_events_past_the_checkpoint(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
        subscriber: MockLiveSubscriber,
    ):
        """Test events at/before the checkpoint are never re-delivered."""
        events = [LiveTestEvent(aggregate_id=uuid4(), data=f"e{i}") for i in range(3)]
        positions = [await append_event(event_store, e) for e in events]
        subscription.last_processed_position = positions[0]

        await runner.start()
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert [e.data for e in subscriber.handled_events] == ["e1", "e2"]

    @pytest.mark.asyncio
    async def test_position_always_present_from_the_feed(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test the runner checkpoints the feed-assigned position."""
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="checkpointed")
        position = await append_event(event_store, event)
        await wake(event_bus)

        await asyncio.sleep(0.01)

        saved = await checkpoint_repo.get_position("LiveTestSubscription")
        assert saved == position


# --- Checkpoint Strategy Tests ---


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


# --- Error Handling Tests ---


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


# --- Module Imports Tests ---


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


# --- Edge Cases Tests ---


class TestLiveRunnerEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_buffer_process(
        self,
        runner: LiveRunner,
    ):
        """Test process_buffer() with nothing buffered or on the feed."""
        await runner.start(buffer_events=True)

        processed = await runner.process_buffer()

        assert processed == 0

    @pytest.mark.asyncio
    async def test_subscription_position_updated_with_position(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test subscription position is updated to the feed's position."""
        await runner.start()

        event = LiveTestEvent(aggregate_id=uuid4(), data="positioned")
        position = await append_event(event_store, event)
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert subscription.last_processed_position == position
        assert subscription.last_event_id == event.event_id
        assert subscription.last_event_type == "LiveTestEvent"

    @pytest.mark.asyncio
    async def test_events_processed_counter_updated(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test events_processed is incremented."""
        await runner.start()

        for i in range(3):
            await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"event_{i}"))
            await wake(event_bus)
            await asyncio.sleep(0.01)

        assert subscription.events_processed == 3

    @pytest.mark.asyncio
    async def test_last_processed_at_updated(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test last_processed_at timestamp is updated."""
        await runner.start()
        assert subscription.last_processed_at is None

        await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data="timestamp_test"))
        await wake(event_bus)
        await asyncio.sleep(0.01)

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

        async def publish_wake() -> None:
            await wake(event_bus)

        await asyncio.gather(*[publish_wake() for _ in range(10)])
        await asyncio.sleep(0.05)

        assert runner.buffer_size == 10
        assert runner.stats.events_received == 0

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
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
    ):
        """Test position is updated correctly with multiple events."""
        await runner.start()

        last_position = None
        for n in range(3):
            last_position = await append_event(
                event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"pos_{n}")
            )
            await wake(event_bus)
            await asyncio.sleep(0.01)

        assert subscription.last_processed_position == last_position

    @pytest.mark.asyncio
    async def test_config_accessible_on_runner(
        self,
        runner: LiveRunner,
        config: SubscriptionConfig,
    ):
        """Test config is accessible on runner."""
        assert runner.config is config
        assert runner.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT


# --- Bounded Drain / Stop-Pause Responsiveness Tests ---
#
# Regression coverage for two bugs: the live drain read the feed with no
# `FeedReadOptions.limit` (unbounded materialization, unlike catch-up which
# passes `config.batch_size`), and neither `stop()` nor `pause()` had any
# effect on a drain already in progress -- `_drain_feed` never consulted
# `_running` or `subscription.wait_if_paused()` between envelopes.


class _RecordingFeed:
    """Wraps a GlobalEventFeed and records the options passed to read_all."""

    def __init__(self, inner: InMemoryEventStore) -> None:
        self._inner = inner
        self.read_all_calls: list[tuple[Position | None, object]] = []

    async def read_all(self, from_position=None, options=None):
        self.read_all_calls.append((from_position, options))
        async for envelope in self._inner.read_all(from_position, options):
            yield envelope

    async def current_position(self):
        return await self._inner.current_position()


class TestLiveRunnerBoundedDrain:
    """Test the live drain bounds each read instead of materializing the feed."""

    @pytest.mark.asyncio
    async def test_drain_passes_a_limit_from_batch_size(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscription: Subscription,
        config: SubscriptionConfig,
    ):
        """Test _drain_feed reads the feed with a bounded limit, not an unbounded read."""
        recording_feed = _RecordingFeed(event_store)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=recording_feed,
            subscription=subscription,
        )
        await runner.start()

        await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data="bounded"))
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert recording_feed.read_all_calls, "read_all was never called"
        for _from_position, options in recording_feed.read_all_calls:
            assert options is not None, "read_all called with no options -- unbounded read"
            assert options.limit == config.batch_size


class TestLiveRunnerStopPauseResponsiveness:
    """Test that stop() and pause() take effect on an in-flight drain."""

    @pytest.mark.asyncio
    async def test_stop_halts_an_in_flight_drain(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test stop() during a drain stops delivery before the whole feed is processed."""
        subscriber.handle_delay = 0.05
        await runner.start()

        total_events = 6
        for i in range(total_events):
            await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"e{i}"))

        drain_task = asyncio.create_task(wake(event_bus))
        # Let the drain begin and process roughly one event before stopping.
        await asyncio.sleep(0.06)
        await runner.stop()

        await asyncio.wait_for(drain_task, timeout=2.0)

        assert len(subscriber.handled_events) < total_events

    @pytest.mark.asyncio
    async def test_pause_takes_effect_mid_drain(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
        subscriber: MockLiveSubscriber,
    ):
        """Test pause() during a drain blocks further delivery until resume()."""
        subscriber.handle_delay = 0.02
        await runner.start()

        total_events = 6
        for i in range(total_events):
            await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"e{i}"))

        drain_task = asyncio.create_task(wake(event_bus))
        # Let the drain begin and process roughly one event before pausing.
        await asyncio.sleep(0.03)
        await subscription.pause()

        # Wait well past the time an un-paused drain would need to finish
        # all `total_events` (6 * 0.02s = 0.12s) to prove pause(), not mere
        # delay, is what is holding the rest of the batch back.
        await asyncio.sleep(0.3)
        processed_while_paused = len(subscriber.handled_events)
        assert processed_while_paused < total_events, (
            "pause() had no effect -- drain ran to completion while paused"
        )
        assert not drain_task.done(), "drain completed instead of blocking on pause"

        await subscription.resume()
        await asyncio.wait_for(drain_task, timeout=2.0)

        assert len(subscriber.handled_events) == total_events


# --- Tenant Isolation Tests ---
#
# `SubscriptionConfig.tenant_id` documents that, when set, "only events
# belonging to the specified tenant are processed". Catch-up honors this via
# `FeedReadOptions(tenant_id=self.config.tenant_id, ...)` (catchup.py:438).
# The live drain must agree -- a tenant-scoped subscription that behaves
# correctly during catch-up and then reads every tenant's events once live
# is a data-isolation breach, not a latency issue.


class TestLiveRunnerTenantIsolation:
    """Test a tenant-scoped LiveRunner never delivers another tenant's events."""

    @pytest.mark.asyncio
    async def test_tenant_scoped_drain_excludes_other_tenants(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test a tenant-scoped subscription only receives its own tenant's events."""
        own_tenant = uuid4()
        other_tenant = uuid4()

        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
            tenant_id=own_tenant,
        )
        subscription = Subscription(
            name="TenantScopedLive",
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

        own_event = LiveTestEvent(aggregate_id=uuid4(), data="mine", tenant_id=own_tenant)
        other_event = LiveTestEvent(aggregate_id=uuid4(), data="not-mine", tenant_id=other_tenant)
        await append_event(event_store, own_event)
        await append_event(event_store, other_event)
        await wake(event_bus)

        await asyncio.sleep(0.01)

        handled_data = [e.data for e in subscriber.handled_events]
        assert "not-mine" not in handled_data, (
            "tenant-scoped LiveRunner delivered another tenant's event"
        )
        assert handled_data == ["mine"]

    @pytest.mark.asyncio
    async def test_drain_pushes_tenant_id_into_the_feed_read(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test the bounded feed read itself is tenant-scoped, not just the consumer-side filter.

        `EventFilter` already drops another tenant's events before delivery
        (that is what the previous test observes), which means a delivery-only
        assertion cannot tell an indexed, tenant-scoped read apart from an
        unscoped read that happens to get filtered afterwards. Catch-up pushes
        the filter into the read itself (`FeedReadOptions(tenant_id=...)`,
        catchup.py:438) rather than relying on the consumer -- the live drain
        must match, both so the two runners agree (recurring-defects #1) and
        so backends can use the indexed predicate instead of over-fetching
        every tenant's rows.
        """
        own_tenant = uuid4()
        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
            tenant_id=own_tenant,
        )
        subscription = Subscription(
            name="TenantScopedLiveSpy",
            config=config,
            subscriber=subscriber,
        )
        recording_feed = _RecordingFeed(event_store)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=recording_feed,
            subscription=subscription,
        )
        await runner.start()

        await append_event(
            event_store, LiveTestEvent(aggregate_id=uuid4(), data="mine", tenant_id=own_tenant)
        )
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert recording_feed.read_all_calls, "read_all was never called"
        for _from_position, options in recording_feed.read_all_calls:
            assert options is not None
            assert options.tenant_id == own_tenant, (
                "read_all called without the subscription's tenant_id -- "
                "the live drain reads every tenant's events from the feed"
            )
