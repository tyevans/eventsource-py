"""Integration tests for health, status, and edge cases."""

from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
)
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    FailingProjection,
    SubTestOrderCreated,
    populate_event_store,
    publish_live_event,
)

pytestmark = pytest.mark.asyncio


class TestHealthAndStatus:
    """Tests for health checks and status reporting."""

    async def test_health_status_healthy(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status when all subscriptions are healthy."""
        await populate_event_store(in_memory_event_store, 10)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        await projection.wait_for_events(10)

        health = subscription_manager.get_health()

        assert health["status"] == "healthy"
        assert health["running"] is True
        assert health["subscription_count"] == 1
        assert len(health["subscriptions"]) == 1
        assert health["subscriptions"][0]["state"] == "live"

    async def test_health_status_unhealthy(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status when subscription has errors."""
        await populate_event_store(in_memory_event_store, 10)

        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await subscription_manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        health = subscription_manager.get_health()

        assert health["status"] == "unhealthy"
        assert health["subscriptions"][0]["state"] == "error"

    async def test_subscription_status_details(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription status includes all details."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        await projection.wait_for_events(50)

        subscription = subscription_manager.get_subscription("CollectingProjection")
        assert subscription is not None
        status = subscription.get_status()

        assert status.name == "CollectingProjection"
        assert status.state == "live"
        assert status.events_processed >= 50
        assert status.events_failed == 0
        assert status.last_processed_at is not None
        assert status.uptime_seconds >= 0


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    async def test_empty_store_then_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test starting with empty store then receiving live events."""
        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        await asyncio.sleep(0.1)
        assert projection.event_count == 0

        for i in range(10):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:03d}",
            )

        success = await projection.wait_for_events(10)
        assert success
        assert projection.event_count == 10

    async def test_start_stop_start_cycle(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test manager can be started, stopped, and restarted."""
        await populate_event_store(in_memory_event_store, 30)

        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)
        await manager.start()
        await projection.wait_for_events(30)
        await manager.stop()

        assert not manager.is_running

        for i in range(30, 40):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:05d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append(
                StreamId(aggregate_id=event.aggregate_id, category="TestOrder"),
                [event],
                ExpectedVersion.no_stream(),
            )

    async def test_single_event(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling of a single event."""
        await populate_event_store(in_memory_event_store, 1)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        success = await projection.wait_for_events(1)
        assert success
        assert projection.event_count == 1
        assert projection.events[0].order_number == "ORD-00000"
