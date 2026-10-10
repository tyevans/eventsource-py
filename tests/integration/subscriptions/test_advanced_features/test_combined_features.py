"""Integration tests for combined advanced features working together."""

from __future__ import annotations

import asyncio

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
)
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    FailingProjection,
    SubTestOrderCancelled,
    SubTestOrderCreated,
    SubTestOrderShipped,
    populate_event_store,
    populate_event_store_with_types,
)

from .helpers import FilteredProjection

pytestmark = pytest.mark.asyncio


class TestCombinedAdvancedFeatures:
    """Tests for combined advanced features working together."""

    async def test_filtering_with_pause_resume(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test event filtering works correctly with pause/resume."""
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=30,
            shipped_count=20,
            cancelled_count=10,
        )

        projection = FilteredProjection(event_types=[SubTestOrderCreated, SubTestOrderShipped])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="FilterPauseProj")
        await manager.start()

        await projection.wait_for_events(25, timeout=10.0)

        await manager.pause_subscription("FilterPauseProj")
        await manager.resume_subscription("FilterPauseProj")

        await projection.wait_for_events(50, timeout=10.0)

        await manager.stop()

        assert projection.event_count == 50
        for event in projection.events:
            assert not isinstance(event, SubTestOrderCancelled)

    async def test_multiple_subscriptions_with_health_check(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test health check reflects status of multiple subscriptions."""
        await populate_event_store(in_memory_event_store, 50)

        healthy_projection = CollectingProjection(name="Healthy")
        failing_projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config_healthy = SubscriptionConfig(start_from="beginning")
        config_failing = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )

        await manager.subscribe(healthy_projection, config_healthy, name="HealthyProj")
        await manager.subscribe(failing_projection, config_failing, name="FailingProj")

        await manager.start()
        await healthy_projection.wait_for_events(50, timeout=10.0)

        health = await manager.health_check()

        assert health.status == "unhealthy"
        assert health.unhealthy_count >= 1
        assert health.subscription_count == 2

        assert len(health.subscriptions) == 2

        healthy_sub = next(s for s in health.subscriptions if s.name == "HealthyProj")
        assert healthy_sub.state == "live"

        failing_sub = next(s for s in health.subscriptions if s.name == "FailingProj")
        assert failing_sub.state == "error"

        await manager.stop()

    async def test_pause_affects_health_status(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that paused subscriptions affect health status."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="PauseHealthProj")
        await manager.start()
        await asyncio.sleep(0.2)

        health_before = await manager.health_check()
        assert health_before.status == "healthy"

        await manager.pause_subscription("PauseHealthProj")

        health_after = await manager.health_check()
        assert health_after.status in ("degraded", "unhealthy")
        assert health_after.subscriptions[0].state == "paused"

        await manager.stop()

    async def test_comprehensive_health_with_all_features(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test comprehensive health report with all advanced features."""
        await populate_event_store(in_memory_event_store, 100)

        healthy_proj = CollectingProjection(name="Healthy")
        filtered_proj = FilteredProjection(event_types=[SubTestOrderCreated])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(healthy_proj, name="HealthyProj")
        await manager.subscribe(filtered_proj, name="FilteredProj")

        await manager.start()
        await healthy_proj.wait_for_events(100, timeout=10.0)
        await filtered_proj.wait_for_events(100, timeout=10.0)

        comprehensive = manager.get_comprehensive_health()

        assert "status" in comprehensive
        assert "subscriptions" in comprehensive
        assert "error_stats" in comprehensive
        assert "recent_errors" in comprehensive
        assert "dlq_status" in comprehensive

        await manager.stop()
