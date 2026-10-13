"""Edge cases and boundary condition tests for LiveRunner."""

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import LiveRunner
from tests.unit.application.subscriptions.test_live_runner._fixtures import (
    LiveTestEvent,
    append_event,
    wake,
)


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
