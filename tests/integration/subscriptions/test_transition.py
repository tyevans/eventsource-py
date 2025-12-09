"""
Integration tests for catch-up to live transition.

Tests cover:
- Seamless transition from catch-up to live
- No events lost during transition (gap-free)
- No duplicate events during transition
- Concurrent event publishing during transition
- Buffer processing during transition
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.subscriptions import SubscriptionConfig

from .conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    populate_event_store,
    publish_live_event,
)

pytestmark = pytest.mark.asyncio


class TestSeamlessTransition:
    """Tests for seamless catch-up to live transition."""

    async def test_seamless_transition(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test seamless transition from catch-up to live."""
        # Arrange: Populate store with 50 events
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        # Act: Subscribe and start
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for catch-up
        success = await projection.wait_for_events(50)
        assert success, f"Expected 50 events, got {projection.event_count}"

        # Publish live events
        for i in range(50, 60):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:05d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )
            await in_memory_event_bus.publish([event])

        # Assert: All events received (catch-up + live)
        success = await projection.wait_for_events(60)
        assert success, f"Expected 60 events, got {projection.event_count}"
        assert projection.event_count == 60

    async def test_transition_maintains_order(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test event order is maintained across transition."""
        # Arrange
        await populate_event_store(in_memory_event_store, 30)

        projection = CollectingProjection()

        # Act
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(30)

        # Publish live events
        for i in range(30, 40):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:05d}",
            )

        await projection.wait_for_events(40)

        # Assert order
        for i in range(40):
            assert projection.events[i].order_number == f"ORD-{i:05d}"


class TestNoDuplicatesDuringTransition:
    """Tests ensuring no duplicate events during transition."""

    async def test_no_duplicate_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test no events are duplicated during transition."""
        # Arrange: Populate store with events
        await populate_event_store(in_memory_event_store, 100)

        projection = CollectingProjection()

        # Act: Subscribe and start
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for completion
        success = await projection.wait_for_events(100)
        assert success

        # Assert: Exactly 100 events, no duplicates
        assert projection.event_count == 100
        assert not projection.has_duplicates()

        event_ids = [str(e.event_id) for e in projection.events]
        assert len(event_ids) == len(set(event_ids))  # All unique

    async def test_no_duplicates_with_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test no duplicates when live events arrive during catch-up."""
        # Arrange
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        # Act
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for catch-up
        await projection.wait_for_events(50)

        # Publish live events
        live_events = []
        for i in range(50, 60):
            event = await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:05d}",
            )
            live_events.append(event)

        await projection.wait_for_events(60)

        # Assert
        assert projection.event_count == 60
        assert not projection.has_duplicates()


class TestNoGapDuringTransition:
    """Tests ensuring no events are lost during transition."""

    async def test_no_gap_during_transition(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test no events are lost during transition."""
        # Arrange: Start with 50 events
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        # Subscribe but don't start yet
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)

        # Start manager in background
        start_task = asyncio.create_task(subscription_manager.start())

        # Publish events concurrently during startup
        for i in range(50, 75):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:05d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )
            await in_memory_event_bus.publish([event])
            await asyncio.sleep(0.01)  # Small delay

        await start_task

        # Wait for events
        success = await projection.wait_for_events(75, timeout=10.0)
        assert success, f"Expected 75 events, got {projection.event_count}"

        # Assert: All events received
        assert projection.event_count >= 75

        # Verify order numbers are sequential (no gaps)
        order_numbers = sorted([e.order_number for e in projection.events])
        for i, order_num in enumerate(order_numbers):
            expected = f"ORD-{i:05d}"
            assert order_num == expected, (
                f"Gap at position {i}: expected {expected}, got {order_num}"
            )

    async def test_continuous_event_stream_during_transition(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling continuous event stream during transition."""
        # Arrange
        await populate_event_store(in_memory_event_store, 100)

        projection = CollectingProjection()

        # Act: Start manager
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)

        # Start background task that continuously publishes events
        async def publish_continuously():
            for i in range(100, 150):
                event = SubTestOrderCreated(
                    aggregate_id=uuid4(),
                    order_number=f"ORD-{i:05d}",
                    amount=100.0 + i,
                )
                await in_memory_event_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type="SubTestOrder",
                    events=[event],
                    expected_version=0,
                )
                await in_memory_event_bus.publish([event])
                await asyncio.sleep(0.01)

        publish_task = asyncio.create_task(publish_continuously())

        # Start manager while publishing
        await subscription_manager.start()
        await publish_task

        # Wait for all events
        success = await projection.wait_for_events(150, timeout=15.0)
        assert success, f"Expected 150 events, got {projection.event_count}"

        # Verify no gaps
        order_numbers = sorted([e.order_number for e in projection.events])
        for i, order_num in enumerate(order_numbers):
            expected = f"ORD-{i:05d}"
            assert order_num == expected


class TestTransitionWithMultipleSubscribers:
    """Tests for transition with multiple subscribers."""

    async def test_multiple_subscribers_receive_all_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test multiple subscribers each receive all events."""
        # Arrange
        await populate_event_store(in_memory_event_store, 50)

        projection1 = CollectingProjection(name="Projection1")
        projection2 = CollectingProjection(name="Projection2")

        # Act
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection1, config, name="Projection1")
        await subscription_manager.subscribe(projection2, config, name="Projection2")
        await subscription_manager.start()

        # Wait for both
        success1 = await projection1.wait_for_events(50)
        success2 = await projection2.wait_for_events(50)

        assert success1 and success2

        # Publish live events
        for i in range(50, 60):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:05d}",
            )

        await projection1.wait_for_events(60)
        await projection2.wait_for_events(60)

        # Assert
        assert projection1.event_count == 60
        assert projection2.event_count == 60
        assert not projection1.has_duplicates()
        assert not projection2.has_duplicates()


class TestTransitionPhases:
    """Tests for transition phase handling."""

    async def test_subscription_state_after_transition(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription is in LIVE state after successful transition."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(50)

        subscription = subscription_manager.get_subscription("CollectingProjection")
        assert subscription.state.value == "live"

    async def test_position_tracking_during_transition(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test position is tracked correctly during transition."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(50)

        subscription = subscription_manager.get_subscription("CollectingProjection")
        # Position should be at or past 50
        assert subscription.last_processed_position >= 50
