"""Unit tests for FlowController.wait_for_drain() basic functionality."""

import asyncio

import pytest

from eventsource.application.subscriptions.flow_control import FlowController


class TestFlowControllerWaitForDrain:
    """Tests for FlowController.wait_for_drain() method."""

    @pytest.mark.asyncio
    async def test_wait_for_drain_returns_immediately_when_empty(self):
        controller = FlowController()
        remaining = await controller.wait_for_drain(timeout=1.0)
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_waits_for_single_event(self):
        controller = FlowController()
        context = await controller.acquire()

        async def release_after_delay():
            await asyncio.sleep(0.1)
            await context.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_after_delay())
        remaining = await controller.wait_for_drain(timeout=5.0)
        await release_task
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_waits_for_multiple_events(self):
        controller = FlowController()
        contexts = [await controller.acquire() for _ in range(5)]
        assert controller.in_flight == 5

        async def release_all():
            await asyncio.sleep(0.1)
            for ctx in contexts:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_all())
        remaining = await controller.wait_for_drain(timeout=5.0)
        await release_task
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_times_out_with_pending_events(self):
        controller = FlowController()
        contexts = [await controller.acquire() for _ in range(3)]
        assert controller.in_flight == 3
        remaining = await controller.wait_for_drain(timeout=0.1)
        assert remaining == 3
        for ctx in contexts:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_partial_completion(self):
        controller = FlowController()
        contexts = [await controller.acquire() for _ in range(5)]

        async def release_some():
            await asyncio.sleep(0.05)
            for ctx in contexts[:3]:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_some())
        remaining = await controller.wait_for_drain(timeout=0.1)
        await release_task
        assert remaining == 2
        for ctx in contexts[3:]:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_concurrent_acquire_release(self):
        controller = FlowController()

        async def worker(worker_id: int):
            for _ in range(10):
                async with await controller.acquire():
                    await asyncio.sleep(0.01)

        workers = [asyncio.create_task(worker(i)) for i in range(5)]
        await asyncio.sleep(0.05)
        remaining = await controller.wait_for_drain(timeout=5.0)
        await asyncio.gather(*workers)
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_drain_event_state_transitions(self):
        controller = FlowController()
        assert controller._drain_event.is_set()
        ctx = await controller.acquire()
        assert not controller._drain_event.is_set()
        await ctx.__aexit__(None, None, None)
        assert controller._drain_event.is_set()

    @pytest.mark.asyncio
    async def test_wait_for_drain_with_zero_timeout(self):
        controller = FlowController()
        ctx = await controller.acquire()
        assert controller.in_flight == 1
        remaining = await controller.wait_for_drain(timeout=0.0)
        assert remaining == 1
        await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_with_very_short_timeout(self):
        controller = FlowController()
        contexts = [await controller.acquire() for _ in range(2)]
        remaining = await controller.wait_for_drain(timeout=0.001)
        assert remaining == 2
        for ctx in contexts:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_multiple_calls(self):
        controller = FlowController()
        assert await controller.wait_for_drain(timeout=1.0) == 0
        contexts = [await controller.acquire() for _ in range(3)]

        async def release_all():
            await asyncio.sleep(0.05)
            for ctx in contexts:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_all())
        assert await controller.wait_for_drain(timeout=5.0) == 0
        await release_task
        assert await controller.wait_for_drain(timeout=1.0) == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_interleaved_acquire_release(self):
        controller = FlowController()
        events_started = 0
        events_completed = 0

        async def worker():
            nonlocal events_started, events_completed
            for _ in range(5):
                ctx = await controller.acquire()
                events_started += 1
                await asyncio.sleep(0.02)
                await ctx.__aexit__(None, None, None)
                events_completed += 1

        workers = [asyncio.create_task(worker()) for _ in range(3)]
        await asyncio.sleep(0.03)
        remaining = await controller.wait_for_drain(timeout=5.0)
        await asyncio.gather(*workers)
        assert remaining == 0
        assert events_completed == 15
