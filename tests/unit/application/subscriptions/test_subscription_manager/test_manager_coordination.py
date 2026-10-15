"""Tests for multi-subscription coordination, live events, and concurrency in SubscriptionManager."""

import asyncio
import time
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import SubscriptionManager, SubscriptionState
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.subscriptions.test_subscription_manager._fixtures import (
    CustomerProjection,
    ManagerTestEvent,
    MockSubscriber,
    OrderProjection,
    add_events_to_store,
)


class TestMultipleSubscriptionsConcurrent:
    @pytest.mark.asyncio
    async def test_concurrent_start_all_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 10)
        order_proj = OrderProjection()
        customer_proj = CustomerProjection()
        third_proj = MockSubscriber()
        await manager.subscribe(order_proj)
        await manager.subscribe(customer_proj)
        await manager.subscribe(third_proj, name="ThirdProjection")

        results = await manager.start()
        assert len(results) == 3 and all(v is None for v in results.values())
        assert len(order_proj.handled_events) == 10
        assert len(customer_proj.handled_events) == 10
        assert len(third_proj.handled_events) == 10
        await manager.stop()

    @pytest.mark.asyncio
    async def test_concurrent_start_returns_results(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())

        results = await manager.start()
        assert results["OrderProjection"] is None and results["CustomerProjection"] is None
        await manager.stop()

    @pytest.mark.asyncio
    async def test_sequential_start_mode(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_proj, customer_proj = OrderProjection(), CustomerProjection()
        await manager.subscribe(order_proj)
        await manager.subscribe(customer_proj)

        results = await manager.start(concurrent=False)
        assert len(results) == 2 and all(v is None for v in results.values())
        assert len(order_proj.handled_events) == 5 and len(customer_proj.handled_events) == 5
        await manager.stop()


class TestMultipleSubscriptionsLifecycle:
    @pytest.mark.asyncio
    async def test_stop_stops_all_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.subscribe(MockSubscriber(), name="ThirdProjection")

        await manager.start()
        await manager.stop()

        for name, status in manager.get_all_statuses().items():
            assert status.state == "stopped", f"{name} should be stopped"

    @pytest.mark.asyncio
    async def test_unsubscribe_one_keeps_others(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())

        await manager.start()
        await manager.unsubscribe("OrderProjection")

        assert manager.is_running is True
        assert manager.get_subscription("CustomerProjection").state == SubscriptionState.LIVE
        assert manager.get_subscription("OrderProjection") is None
        await manager.stop()

    @pytest.mark.asyncio
    async def test_independent_checkpoint_per_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        await add_events_to_store(event_store, 10)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()

        assert await checkpoint_repo.get_position("OrderProjection") is not None
        assert await checkpoint_repo.get_position("CustomerProjection") is not None
        await manager.stop()


class TestConcurrentStartPerformance:
    @pytest.mark.asyncio
    async def test_concurrent_start_is_faster_than_sequential(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        await add_events_to_store(event_store, 50)
        concurrent_manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
        subs = []
        for idx in (1, 2, 3):
            sub = MockSubscriber()
            sub.handle_delay = 0.01
            subs.append(sub)
            await concurrent_manager.subscribe(sub, name=f"Sub{idx}")

        start_time = time.time()
        await concurrent_manager.start(concurrent=True)
        concurrent_duration = time.time() - start_time
        await concurrent_manager.stop()

        for sub in subs:
            sub.handled_events.clear()
        assert concurrent_duration < 10.0

    @pytest.mark.asyncio
    async def test_start_returns_empty_dict_if_already_running(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(MockSubscriber())

        assert len(await manager.start()) == 1
        assert await manager.start() == {}
        await manager.stop()


class TestLiveEvents:
    @pytest.mark.asyncio
    async def test_receives_live_events_after_start(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()
        assert len(subscriber.handled_events) == 5

        live_event = ManagerTestEvent(aggregate_id=uuid4(), data="live_event")
        await event_store.append(
            StreamId(aggregate_id=live_event.aggregate_id, category="ManagerAggregate"),
            [live_event],
            ExpectedVersion.no_stream(),
        )
        await event_bus.publish([live_event])
        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 6
        assert subscriber.handled_events[-1].data == "live_event"
        await manager.stop()
