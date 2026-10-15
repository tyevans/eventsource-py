"""Integration tests for catch-up to live transition."""

import asyncio

import pytest

from eventsource.application.subscriptions import SubscriptionConfig

from .conftest import (
    CollectingProjection,
    populate_event_store,
    publish_live_event,
)

pytestmark = pytest.mark.asyncio


async def position_after(store, n: int):
    """The token of the nth (1-based) event in the feed."""
    envelopes = [e async for e in store.read_all()]
    position = envelopes[n - 1].position
    assert position is not None
    return position


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
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        success = await projection.wait_for_events(50)
        assert success, f"Expected 50 events, got {projection.event_count}"

        for i in range(50, 60):
            await publish_live_event(
                in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}", amount=100.0 + i
            )

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
        await populate_event_store(in_memory_event_store, 30)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(30)

        for i in range(30, 40):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}")

        await projection.wait_for_events(40)
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
        await populate_event_store(in_memory_event_store, 100)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        success = await projection.wait_for_events(100)
        assert success
        assert projection.event_count == 100
        assert not projection.has_duplicates()

        event_ids = [str(e.event_id) for e in projection.events]
        assert len(event_ids) == len(set(event_ids))

    async def test_no_duplicates_with_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test no duplicates when live events arrive during catch-up."""
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(50)

        live_events = []
        for i in range(50, 60):
            event = await publish_live_event(
                in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}"
            )
            live_events.append(event)

        await projection.wait_for_events(60)
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
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        start_task = asyncio.create_task(subscription_manager.start())

        for i in range(50, 75):
            await publish_live_event(
                in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}", amount=100.0 + i
            )
            await asyncio.sleep(0.01)

        await start_task
        success = await projection.wait_for_events(75, timeout=10.0)
        assert success, f"Expected 75 events, got {projection.event_count}"
        assert projection.event_count >= 75

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
        await populate_event_store(in_memory_event_store, 100)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)

        async def publish_continuously():
            for i in range(100, 150):
                await publish_live_event(
                    in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}", amount=100.0 + i
                )
                await asyncio.sleep(0.01)

        publish_task = asyncio.create_task(publish_continuously())
        await subscription_manager.start()
        await publish_task

        success = await projection.wait_for_events(150, timeout=15.0)
        assert success, f"Expected 150 events, got {projection.event_count}"

        order_numbers = sorted([e.order_number for e in projection.events])
        for i, order_num in enumerate(order_numbers):
            assert order_num == f"ORD-{i:05d}"


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
        await populate_event_store(in_memory_event_store, 50)
        projection1 = CollectingProjection(name="Projection1")
        projection2 = CollectingProjection(name="Projection2")

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection1, config, name="Projection1")
        await subscription_manager.subscribe(projection2, config, name="Projection2")
        await subscription_manager.start()

        success1 = await projection1.wait_for_events(50)
        success2 = await projection2.wait_for_events(50)
        assert success1 and success2

        for i in range(50, 60):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}")

        await projection1.wait_for_events(60)
        await projection2.wait_for_events(60)

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
        assert subscription.last_processed_position >= await position_after(
            in_memory_event_store, 50
        )
