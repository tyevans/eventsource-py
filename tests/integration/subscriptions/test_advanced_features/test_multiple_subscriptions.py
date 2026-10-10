"""Integration tests for multiple concurrent subscriptions."""

from __future__ import annotations

import asyncio

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    FailingProjection,
    populate_event_store,
    publish_live_event,
)

from .helpers import position_after

pytestmark = pytest.mark.asyncio


class TestMultipleSubscriptions:
    """Tests for multiple concurrent subscriptions."""

    async def test_multiple_subscriptions_concurrent_start(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that multiple subscriptions start concurrently."""
        await populate_event_store(in_memory_event_store, 100)

        projections = [CollectingProjection(name=f"Projection{i}") for i in range(5)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"Projection{i}")

        results = await manager.start()

        assert all(result is None for result in results.values())
        assert len(results) == 5

        for projection in projections:
            await projection.wait_for_events(100, timeout=10.0)

        await manager.stop()

        for projection in projections:
            assert projection.event_count == 100

    async def test_multiple_subscriptions_independent_checkpoints(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that each subscription has independent checkpoints."""
        events = await populate_event_store(in_memory_event_store, 100)

        await in_memory_checkpoint_repo.save_position(
            subscription_id="ProjectionA",
            position=await position_after(in_memory_event_store, 25),
            event_id=events[24].event_id,
            event_type="SubTestOrderCreated",
        )
        await in_memory_checkpoint_repo.save_position(
            subscription_id="ProjectionB",
            position=await position_after(in_memory_event_store, 50),
            event_id=events[49].event_id,
            event_type="SubTestOrderCreated",
        )

        projection_a = CollectingProjection(name="A")
        projection_b = CollectingProjection(name="B")
        projection_c = CollectingProjection(name="C")

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config_checkpoint = SubscriptionConfig(start_from="checkpoint")
        config_beginning = SubscriptionConfig(start_from="beginning")

        await manager.subscribe(projection_a, config_checkpoint, name="ProjectionA")
        await manager.subscribe(projection_b, config_checkpoint, name="ProjectionB")
        await manager.subscribe(projection_c, config_beginning, name="ProjectionC")

        await manager.start()

        await projection_a.wait_for_events(75, timeout=10.0)
        await projection_b.wait_for_events(50, timeout=10.0)
        await projection_c.wait_for_events(100, timeout=10.0)

        await manager.stop()

        assert projection_a.event_count == 75
        assert projection_b.event_count == 50
        assert projection_c.event_count == 100

    async def test_one_failure_doesnt_stop_others(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that one subscription failure doesn't stop other subscriptions."""
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

        success = await healthy_projection.wait_for_events(50, timeout=10.0)
        assert success

        await manager.stop()

        assert healthy_projection.event_count == 50

        failing_sub = manager.get_subscription("FailingProj")
        assert failing_sub is not None
        assert failing_sub.state == SubscriptionState.ERROR

    async def test_concurrent_live_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test multiple subscriptions processing live events concurrently."""
        projections = [CollectingProjection(name=f"LiveProj{i}") for i in range(3)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"LiveProj{i}")

        await manager.start()

        await asyncio.sleep(0.2)

        for i in range(20):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"LIVE-CONCURRENT-{i:03d}",
            )

        for projection in projections:
            await projection.wait_for_events(20, timeout=10.0)

        await manager.stop()

        for projection in projections:
            assert projection.event_count == 20
