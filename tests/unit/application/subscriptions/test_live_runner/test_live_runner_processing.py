"""Event processing, buffer mode, feed ordering, and bounded drain tests for LiveRunner."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
)
from eventsource.application.subscriptions.runners import LiveRunner
from tests.unit.application.subscriptions.test_live_runner._fixtures import (
    AnotherTestEvent,
    LiveTestEvent,
    MockLiveSubscriber,
    _RecordingFeed,
    append_event,
    wake,
)


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


# --- Bounded Drain Tests ---


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
