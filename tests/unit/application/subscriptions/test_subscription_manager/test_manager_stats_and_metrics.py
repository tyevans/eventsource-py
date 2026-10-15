"""Tests for health status, statistics, statuses, and module exports in SubscriptionManager."""

import contextlib

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionStatus,
)
from tests.unit.application.subscriptions.test_subscription_manager._fixtures import (
    CustomerProjection,
    MockSubscriber,
    OrderProjection,
    add_events_to_store,
)


class TestHealthStatus:
    @pytest.mark.asyncio
    async def test_health_when_stopped(
        self, manager: SubscriptionManager, subscriber: MockSubscriber
    ):
        await manager.subscribe(subscriber)
        health = manager.get_health()
        assert health["status"] == "stopped"
        assert health["running"] is False
        assert health["subscription_count"] == 1
        assert len(health["subscriptions"]) == 1

    @pytest.mark.asyncio
    async def test_health_when_running(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(subscriber)
        await manager.start()
        health = manager.get_health()
        assert health["status"] == "healthy"
        assert health["running"] is True
        assert health["subscription_count"] == 1
        assert len(health["subscriptions"]) == 1
        assert health["subscriptions"][0]["state"] == "live"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_health_with_error_subscription(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("ManagerTestEvent")
        config = SubscriptionConfig(continue_on_error=False)
        await manager.subscribe(subscriber, config=config)
        with contextlib.suppress(Exception):
            await manager.start()
        health = manager.get_health()
        assert health["status"] == "unhealthy"
        assert health["subscriptions"][0]["state"] == "error"

    @pytest.mark.asyncio
    async def test_health_subscription_details(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(subscriber)
        await manager.start()
        sub_status = manager.get_health()["subscriptions"][0]
        assert "name" in sub_status
        assert "state" in sub_status
        assert "position" in sub_status
        assert "events_processed" in sub_status
        await manager.stop()


class TestMultipleSubscriptionsHealth:
    @pytest.mark.asyncio
    async def test_health_status_all_healthy(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()
        health = manager.get_health()
        assert health["status"] == "healthy"
        assert health["subscription_count"] == 2
        assert len(health["subscriptions"]) == 2
        await manager.stop()

    @pytest.mark.asyncio
    async def test_health_status_with_mixed_states(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        working = OrderProjection()
        failing = MockSubscriber()
        failing.fail_on_event_types.add("ManagerTestEvent")
        await manager.subscribe(working)
        await manager.subscribe(
            failing,
            config=SubscriptionConfig(continue_on_error=False),
            name="FailingSubscriber",
        )
        await manager.start()
        health = manager.get_health()
        assert health["status"] == "unhealthy"
        sub_states = {s["name"]: s["state"] for s in health["subscriptions"]}
        assert sub_states["OrderProjection"] == "live"
        assert sub_states["FailingSubscriber"] == "error"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_is_healthy_property_with_multiple_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()
        assert manager.is_healthy is True
        await manager.stop()


class TestGetAllStatuses:
    @pytest.mark.asyncio
    async def test_get_all_statuses_returns_dict(self, manager: SubscriptionManager):
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        statuses = manager.get_all_statuses()
        assert isinstance(statuses, dict)
        assert "OrderProjection" in statuses
        assert "CustomerProjection" in statuses

    @pytest.mark.asyncio
    async def test_get_all_statuses_returns_subscription_status_objects(
        self, manager: SubscriptionManager
    ):
        await manager.subscribe(MockSubscriber())
        statuses = manager.get_all_statuses()
        assert "MockSubscriber" in statuses
        status = statuses["MockSubscriber"]
        assert isinstance(status, SubscriptionStatus)
        assert status.name == "MockSubscriber"
        assert status.state == "starting"

    @pytest.mark.asyncio
    async def test_get_all_statuses_shows_different_states(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start(subscription_names=["OrderProjection"])
        statuses = manager.get_all_statuses()
        assert statuses["OrderProjection"].state == "live"
        assert statuses["CustomerProjection"].state == "starting"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_get_all_statuses_empty_manager(self, manager: SubscriptionManager):
        assert manager.get_all_statuses() == {}

    @pytest.mark.asyncio
    async def test_get_all_statuses_includes_statistics(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 10)
        await manager.subscribe(MockSubscriber())
        await manager.start()
        status = manager.get_all_statuses()["MockSubscriber"]
        assert status.events_processed == 10
        assert status.position is not None
        assert status.uptime_seconds >= 0
        await manager.stop()


class TestModuleImports:
    def test_import_from_subscriptions_module(self):
        from eventsource.application.subscriptions import SubscriptionManager as SubMgr

        assert SubMgr is SubscriptionManager

    def test_all_exports(self):
        from eventsource.application.subscriptions import manager

        assert "SubscriptionManager" in manager.__all__
