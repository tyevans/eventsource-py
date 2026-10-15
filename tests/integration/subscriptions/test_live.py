"""Integration tests for live event processing."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion

from .conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    SubTestOrderShipped,
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


class TestLiveEventDelivery:
    """Tests for live event delivery after transition."""

    async def test_receives_live_events_after_catchup(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 20)
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(20)
        for i in range(5):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"LIVE-{i:03d}")
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
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="end")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await asyncio.sleep(0.1)
        assert projection.event_count == 0
        for i in range(10):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"LIVE-{i:03d}")
        success = await projection.wait_for_events(10)
        assert success
        assert projection.event_count == 10
        for event in projection.events:
            assert event.order_number.startswith("LIVE-")

    async def test_live_events_with_different_types(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        order_id = uuid4()
        stream_id = StreamId(aggregate_id=order_id, category="SubTestOrder")
        created = SubTestOrderCreated(aggregate_id=order_id, order_number="ORD-001", amount=100.0)
        await in_memory_event_store.append(stream_id, [created], ExpectedVersion.no_stream())
        await in_memory_event_bus.publish([created])
        shipped = SubTestOrderShipped(aggregate_id=order_id, tracking_number="TRK-001")
        await in_memory_event_store.append(stream_id, [shipped], ExpectedVersion.exact(1))
        await in_memory_event_bus.publish([shipped])
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
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        for i in range(20):
            await publish_live_event(
                in_memory_event_store, in_memory_event_bus, f"ORD-{i:03d}", amount=100.0 + i
            )
        success = await projection.wait_for_events(20)
        assert success
        for i, event in enumerate(projection.events):
            assert event.order_number == f"ORD-{i:03d}"

    async def test_rapid_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        events_published = []
        for i in range(100):
            event = await publish_live_event(
                in_memory_event_store, in_memory_event_bus, f"ORD-{i:03d}", amount=100.0 + i
            )
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
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
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
        projection1 = CollectingProjection(name="Projection1")
        projection2 = CollectingProjection(name="Projection2")
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection1, config, name="Projection1")
        await subscription_manager.subscribe(projection2, config, name="Projection2")
        await subscription_manager.start()
        for i in range(10):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"ORD-{i:03d}")
        await projection1.wait_for_events(10)
        await projection2.wait_for_events(10)
        assert projection1.event_count == 10
        assert projection2.event_count == 10

    async def test_live_subscription_continues_after_stop_start(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        for i in range(20):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(), order_number=f"ORD-{i:03d}", amount=100.0 + i
            )
            await in_memory_event_store.append(
                StreamId(aggregate_id=event.aggregate_id, category="SubTestOrder"),
                [event],
                ExpectedVersion.no_stream(),
            )
        projection1 = CollectingProjection()
        manager1 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager1.subscribe(
            projection1, SubscriptionConfig(start_from="beginning"), name="TestProjection"
        )
        await manager1.start()
        await projection1.wait_for_events(20)
        await manager1.stop()
        checkpoint_pos = await in_memory_checkpoint_repo.get_position("TestProjection")
        assert checkpoint_pos is not None, "Checkpoint should be saved after catch-up"
        assert checkpoint_pos >= await position_after(in_memory_event_store, 20)
        for i in range(20, 30):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(), order_number=f"ORD-{i:03d}", amount=100.0 + i
            )
            await in_memory_event_store.append(
                StreamId(aggregate_id=event.aggregate_id, category="SubTestOrder"),
                [event],
                ExpectedVersion.no_stream(),
            )
        projection2 = CollectingProjection()
        manager2 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager2.subscribe(
            projection2, SubscriptionConfig(start_from="checkpoint"), name="TestProjection"
        )
        await manager2.start()
        success = await projection2.wait_for_events(10, timeout=10.0)
        assert success, f"Expected 10 events, got {projection2.event_count}"
        assert projection2.event_count == 10
        assert projection2.events[0].order_number == "ORD-020"
        await manager2.stop()
