"""Unit tests for FlowController drain edge cases and concurrency."""

import asyncio

import pytest

from eventsource.application.subscriptions.flow_control import FlowController


class TestFlowControllerDrainEdgeCases:
    """Edge case tests for FlowController drain functionality."""

    @pytest.mark.asyncio
    async def test_drain_event_cleared_on_first_acquire(self):
        """Test drain event is cleared on first acquire."""
        controller = FlowController()
        assert controller._drain_event.is_set()

        ctx = await controller.acquire()
        assert not controller._drain_event.is_set()

        await ctx.__aexit__(None, None, None)
        assert controller._drain_event.is_set()

    @pytest.mark.asyncio
    async def test_drain_event_stays_cleared_with_multiple_in_flight(self):
        """Test drain event stays cleared with multiple in-flight events."""
        controller = FlowController()

        ctx1 = await controller.acquire()
        ctx2 = await controller.acquire()
        assert not controller._drain_event.is_set()

        # Release one - should still be cleared
        await ctx1.__aexit__(None, None, None)
        assert not controller._drain_event.is_set()

        # Release second - should now be set
        await ctx2.__aexit__(None, None, None)
        assert controller._drain_event.is_set()

    @pytest.mark.asyncio
    async def test_drain_with_max_capacity_acquired(self):
        """Test drain when at max capacity."""
        controller = FlowController()

        # Acquire all slots
        contexts = [await controller.acquire() for _ in range(3)]
        assert controller.in_flight == 3

        async def release_all():
            await asyncio.sleep(0.05)
            for ctx in contexts:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_all())
        remaining = await controller.wait_for_drain(timeout=5.0)

        await release_task
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_drain_concurrent_wait_calls(self):
        """Test multiple concurrent wait_for_drain calls."""
        controller = FlowController()

        # Acquire some slots
        contexts = [await controller.acquire() for _ in range(5)]

        async def release_after_delay():
            await asyncio.sleep(0.1)
            for ctx in contexts:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_after_delay())

        # Start multiple drain waits concurrently
        drain_tasks = [
            asyncio.create_task(controller.wait_for_drain(timeout=5.0)) for _ in range(3)
        ]

        results = await asyncio.gather(*drain_tasks)
        await release_task

        # All should return 0 (fully drained)
        assert all(r == 0 for r in results)

    @pytest.mark.asyncio
    async def test_drain_with_negative_timeout_treated_as_zero(self):
        """Test wait_for_drain with negative timeout is treated as immediate timeout."""
        controller = FlowController()

        ctx = await controller.acquire()
        assert controller.in_flight == 1

        # Negative timeout should behave like zero/immediate timeout
        # asyncio.wait_for with negative timeout will raise TimeoutError immediately
        remaining = await controller.wait_for_drain(timeout=-1.0)
        assert remaining == 1

        await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_stats_updated_during_drain(self):
        """Test stats are properly updated during drain operations."""
        controller = FlowController()

        # Initial stats
        assert controller.stats.events_in_flight == 0
        assert controller.stats.total_acquisitions == 0
        assert controller.stats.total_releases == 0

        # Acquire slots
        contexts = [await controller.acquire() for _ in range(3)]
        assert controller.stats.total_acquisitions == 3
        assert controller.stats.events_in_flight == 3

        # Release slots
        for ctx in contexts:
            await ctx.__aexit__(None, None, None)

        assert controller.stats.total_releases == 3
        assert controller.stats.events_in_flight == 0

        # Drain should return immediately
        remaining = await controller.wait_for_drain(timeout=1.0)
        assert remaining == 0
