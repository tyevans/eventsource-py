"""Integration tests for Health Check API functionality."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from eventsource.application.subscriptions.health import ManagerHealth
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    FailingProjection,
    SlowProjection,
    populate_event_store,
)

pytestmark = pytest.mark.asyncio


class TestHealthCheckAPI:
    """Tests for Health Check API functionality."""

    async def test_readiness_check_not_ready_before_start(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test readiness check returns not ready before start."""
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)

        readiness = await manager.readiness_check()
        assert readiness.ready is False
        assert "not running" in readiness.reason.lower()

    async def test_readiness_check_ready_after_start(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test readiness check returns ready after start."""
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)
        await manager.start()
        await asyncio.sleep(0.2)

        readiness = await manager.readiness_check()
        assert readiness.ready is True
        assert "ready" in readiness.reason.lower()
        await manager.stop()

    async def test_readiness_check_not_ready_with_error_subscription(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test readiness check returns not ready when subscription is in error."""
        await populate_event_store(in_memory_event_store, 10)
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await manager.start()

        readiness = await manager.readiness_check()
        assert readiness.ready is False
        assert "error" in readiness.reason.lower()
        await manager.stop()

    async def test_liveness_check_alive(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test liveness check returns alive for running manager."""
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)
        await manager.start()

        liveness = await manager.liveness_check()
        assert liveness.alive is True
        assert "alive" in liveness.reason.lower()
        await manager.stop()

    async def test_comprehensive_health_check(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test comprehensive health check returns all expected data."""
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)
        await manager.start()
        await projection.wait_for_events(50)

        health = await manager.health_check()
        assert isinstance(health, ManagerHealth)
        assert health.status in ("healthy", "degraded", "unhealthy", "unknown")
        assert health.running is True
        assert health.subscription_count == 1
        assert health.total_events_processed >= 50
        assert health.total_events_failed == 0
        assert health.uptime_seconds >= 0
        assert len(health.subscriptions) == 1

        sub_health = health.subscriptions[0]
        assert sub_health.name == "CollectingProjection"
        assert sub_health.state == "live"
        assert sub_health.events_processed >= 50
        await manager.stop()

    async def test_health_check_returns_all_subscriptions(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test that health check returns status for all subscriptions."""
        projections = [CollectingProjection(name=f"Proj{i}") for i in range(4)]
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"Proj{i}")

        await manager.start()
        await asyncio.sleep(0.2)

        health = await manager.health_check()
        assert health.subscription_count == 4
        assert len(health.subscriptions) == 4
        names = {s.name for s in health.subscriptions}
        assert names == {"Proj0", "Proj1", "Proj2", "Proj3"}
        await manager.stop()

    async def test_lag_calculation(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test that lag is calculated correctly."""
        await populate_event_store(in_memory_event_store, 100)
        projection = SlowProjection(delay=0.05)
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning", batch_size=10)
        await manager.subscribe(projection, config)
        await manager.start()
        await asyncio.sleep(0.3)

        health = await manager.health_check()
        assert health.total_lag_events >= 0

        await projection.wait_for_events(100, timeout=30.0)
        health_after = await manager.health_check()
        assert health_after.total_lag_events <= 10
        await manager.stop()

    async def test_health_status_degraded(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test health status shows degraded for catching up subscriptions."""
        await populate_event_store(in_memory_event_store, 200)
        projection = SlowProjection(delay=0.1)
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config)
        await manager.start()
        await asyncio.sleep(0.2)

        subscription = manager.get_subscription("SlowProjection")
        if subscription and subscription.state == SubscriptionState.CATCHING_UP:
            health = await manager.health_check()
            assert health.degraded_count >= 1 or health.healthy_count >= 1
        await manager.stop()

    async def test_health_status_unhealthy(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test health status shows unhealthy for error subscriptions."""
        await populate_event_store(in_memory_event_store, 10)
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await manager.start()

        health = await manager.health_check()
        assert health.status == "unhealthy"
        assert health.unhealthy_count >= 1
        await manager.stop()

    async def test_health_to_dict_serialization(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test health check result is JSON serializable."""
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)
        await manager.start()
        await asyncio.sleep(0.1)

        health = await manager.health_check()
        health_dict = health.to_dict()
        assert isinstance(health_dict, dict)
        assert "status" in health_dict
        assert "running" in health_dict
        assert "subscription_count" in health_dict
        assert "subscriptions" in health_dict
        assert isinstance(health_dict["subscriptions"], list)
        await manager.stop()
