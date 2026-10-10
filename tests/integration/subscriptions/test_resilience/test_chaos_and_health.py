"""Integration tests for chaos engineering and health check resilience."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
)
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    FailingProjection,
    SlowProjection,
    populate_event_store,
    publish_live_event,
)

from .helpers import ChaosProjection, VariableLatencyProjection

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


class TestChaos:
    """Chaos testing scenarios."""

    async def test_random_failures(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling of random failures during processing."""
        await populate_event_store(in_memory_event_store, 100)

        # 10% random failure rate
        projection = ChaosProjection(failure_rate=0.1)

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for processing
        await asyncio.sleep(3.0)
        await subscription_manager.stop()

        # Should have processed most events despite failures
        assert projection.event_count >= 70
        assert projection.total_failures > 0

    async def test_variable_latency(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling of variable processing latency."""
        await populate_event_store(in_memory_event_store, 50)

        projection = VariableLatencyProjection(
            min_delay=0.01,
            max_delay=0.1,
        )

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        success = await projection.wait_for_events(50, timeout=30.0)
        await subscription_manager.stop()

        assert success
        assert projection.event_count == 50

        if len(projection.processing_times) > 1:
            times = projection.processing_times
            assert max(times) > min(times)

    async def test_concurrent_events_during_shutdown(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test events arriving during shutdown."""
        await populate_event_store(in_memory_event_store, 20)

        projection = SlowProjection(delay=0.05)

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        async def publish_events():
            for i in range(20, 30):
                await publish_live_event(
                    in_memory_event_store,
                    in_memory_event_bus,
                    f"ORD-{i:05d}",
                )
                await asyncio.sleep(0.02)

        publish_task = asyncio.create_task(publish_events())

        await asyncio.sleep(0.3)
        await subscription_manager.stop(timeout=5.0)

        if not publish_task.done():
            publish_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await publish_task

        assert projection.event_count > 0


class TestHealthCheckIntegration:
    """Tests for health check integration with resilience features."""

    async def test_health_status_reflects_errors(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status reflects error state."""
        await populate_event_store(in_memory_event_store, 10)

        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )
        await subscription_manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        health = subscription_manager.get_health()
        assert health["status"] == "unhealthy"

    async def test_health_status_healthy_after_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status is healthy after successful processing."""
        await populate_event_store(in_memory_event_store, 20)

        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(20)

        health = subscription_manager.get_health()
        assert health["status"] == "healthy"

        await subscription_manager.stop()

    async def test_comprehensive_health_includes_error_stats(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test comprehensive health report includes error statistics."""
        await populate_event_store(in_memory_event_store, 20)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()
        await projection.wait_for_events(20)

        health = subscription_manager.get_comprehensive_health()

        assert "error_stats" in health
        assert "recent_errors" in health
        assert "dlq_status" in health
        assert health["error_stats"]["total_errors"] == 0

        await subscription_manager.stop()
