"""
Integration tests for live event processing.

Tests cover:
- Live event delivery after transition
- Multiple event types in live mode
- Real-time event handling
- Event ordering in live mode
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.subscriptions import SubscriptionConfig

from .conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    SubTestOrderShipped,
    populate_event_store,
    publish_live_event,
)

pytestmark = pytest.mark.asyncio


class TestLiveEventDelivery:
    """Tests for live event delivery after transition."""

    async def test_receives_live_events_after_catchup(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription receives live events after catch-up."""
        # Arrange
        await populate_event_store(in_memory_event_store, 20)

        projection = CollectingProjection()

        # Act
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(20)

        # Publish live events
        for i in range(5):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"LIVE-{i:03d}",
            )

        # Assert
        success = await projection.wait_for_events(25)
        assert success
        assert projection.event_count == 25

    async def test_live_only_subscription(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test live-only subscription ignores historical events."""
        # Arrange: Populate store with historical events
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        # Act: Subscribe with start_from="end"
        config = SubscriptionConfig(start_from="end")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait a moment to ensure we're in live mode
        await asyncio.sleep(0.1)

        # Historical events should not be received
        assert projection.event_count == 0

        # Publish live events
        for i in range(10):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"LIVE-{i:03d}",
            )

        # Assert: Only live events received
        success = await projection.wait_for_events(10)
        assert success
        assert projection.event_count == 10

        # All events should be live events
        for event in projection.events:
            assert event.order_number.startswith("LIVE-")

    async def test_live_events_with_different_types(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test live subscription handles different event types."""
        projection = CollectingProjection()

        # Start with empty store
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Publish different event types
        order_id = uuid4()

        # Created event
        created = SubTestOrderCreated(
            aggregate_id=order_id,
            order_number="ORD-001",
            amount=100.0,
        )
        await in_memory_event_store.append_events(
            aggregate_id=order_id,
            aggregate_type="SubTestOrder",
            events=[created],
            expected_version=0,
        )
        await in_memory_event_bus.publish([created])

        # Shipped event
        shipped = SubTestOrderShipped(
            aggregate_id=order_id,
            tracking_number="TRK-001",
        )
        await in_memory_event_store.append_events(
            aggregate_id=order_id,
            aggregate_type="SubTestOrder",
            events=[shipped],
            expected_version=1,
        )
        await in_memory_event_bus.publish([shipped])

        # Assert
        success = await projection.wait_for_events(2)
        assert success
        assert projection.event_count == 2
        assert isinstance(projection.events[0], SubTestOrderCreated)
        assert isinstance(projection.events[1], SubTestOrderShipped)


class TestLiveEventOrdering:
    """Tests for event ordering in live mode."""

    async def test_live_events_maintain_order(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test live events are delivered in order."""
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Publish events in sequence
        for i in range(20):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:03d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )
            await in_memory_event_bus.publish([event])

        success = await projection.wait_for_events(20)
        assert success

        # Verify order
        for i, event in enumerate(projection.events):
            assert event.order_number == f"ORD-{i:03d}"

    async def test_rapid_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling rapid succession of live events."""
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Publish many events rapidly
        events_published = []
        for i in range(100):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:03d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )
            await in_memory_event_bus.publish([event])
            events_published.append(event)

        success = await projection.wait_for_events(100, timeout=10.0)
        assert success
        assert projection.event_count == 100
        assert not projection.has_duplicates()


class TestLiveSubscriptionHealth:
    """Tests for live subscription health and state."""

    async def test_live_subscription_state(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription is in live state after transition."""
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Give it a moment to transition
        await asyncio.sleep(0.1)

        subscription = subscription_manager.get_subscription("CollectingProjection")
        assert subscription is not None
        assert subscription.state.value == "live"

    async def test_live_subscription_stats(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test live subscription tracks statistics."""
        await populate_event_store(in_memory_event_store, 30)

        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(30)

        subscription = subscription_manager.get_subscription("CollectingProjection")
        assert subscription.events_processed >= 30
        assert subscription.events_failed == 0


class TestLiveSubscriptionRecovery:
    """Tests for live subscription handling edge cases."""

    async def test_multiple_live_subscriptions(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test multiple live subscriptions receive events independently."""
        projection1 = CollectingProjection(name="Projection1")
        projection2 = CollectingProjection(name="Projection2")

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection1, config, name="Projection1")
        await subscription_manager.subscribe(projection2, config, name="Projection2")
        await subscription_manager.start()

        # Publish events
        for i in range(10):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:03d}",
            )

        await projection1.wait_for_events(10)
        await projection2.wait_for_events(10)

        # Both should have received all events
        assert projection1.event_count == 10
        assert projection2.event_count == 10

    async def test_live_subscription_continues_after_stop_start(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test live subscription resumes correctly after manager restart."""
        from eventsource.subscriptions import SubscriptionManager

        # First: Populate store with initial events (not live)
        for i in range(20):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:03d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )

        # First manager instance - catches up from beginning
        projection1 = CollectingProjection()
        manager1 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager1.subscribe(projection1, config, name="TestProjection")
        await manager1.start()

        # Wait for catch-up to complete
        await projection1.wait_for_events(20)
        await manager1.stop()

        # Verify checkpoint was saved (catch-up saves checkpoints)
        checkpoint_pos = await in_memory_checkpoint_repo.get_position("TestProjection")
        assert checkpoint_pos is not None, "Checkpoint should be saved after catch-up"
        assert checkpoint_pos >= 20

        # Add more events while stopped
        for i in range(20, 30):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:03d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )

        # Second manager instance
        projection2 = CollectingProjection()
        manager2 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        # Resume from checkpoint
        resume_config = SubscriptionConfig(start_from="checkpoint")
        await manager2.subscribe(projection2, resume_config, name="TestProjection")
        await manager2.start()

        # Should catch up events 20-29 (10 new events after checkpoint)
        success = await projection2.wait_for_events(10, timeout=10.0)
        assert success, f"Expected 10 events, got {projection2.event_count}"
        assert projection2.event_count == 10

        # First event should be ORD-020
        assert projection2.events[0].order_number == "ORD-020"

        await manager2.stop()
