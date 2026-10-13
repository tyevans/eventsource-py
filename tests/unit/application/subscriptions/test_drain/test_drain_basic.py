"""Unit tests for FlowController.wait_for_drain() basic functionality."""

import asyncio

import pytest

from eventsource.application.subscriptions.flow_control import FlowController


class TestFlowControllerWaitForDrain:
    """Tests for FlowController.wait_for_drain() method."""

    @pytest.mark.asyncio
    async def test_wait_for_drain_returns_immediately_when_empty(self):
        """Test that wait_for_drain returns 0 immediately when no events in flight."""
        controller = FlowController()

        remaining = await controller.wait_for_drain(timeout=1.0)

        assert remaining == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_waits_for_single_event(self):
        """Test that wait_for_drain waits for a single in-flight event."""
        controller = FlowController()

        # Acquire slot and start drain wait
        context = await controller.acquire()

        async def release_after_delay():
            await asyncio.sleep(0.1)
            await context.__aexit__(None, None, None)

        # Start release task and wait for drain
        release_task = asyncio.create_task(release_after_delay())
        remaining = await controller.wait_for_drain(timeout=5.0)

        await release_task
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_waits_for_multiple_events(self):
        """Test that wait_for_drain waits for multiple in-flight events."""
        controller = FlowController()

        # Acquire multiple slots
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
        """Test that wait_for_drain returns remaining count on timeout."""
        controller = FlowController()

        # Acquire slots but don't release
        contexts = [await controller.acquire() for _ in range(3)]
        assert controller.in_flight == 3

        # Wait with short timeout
        remaining = await controller.wait_for_drain(timeout=0.1)

        assert remaining == 3

        # Cleanup
        for ctx in contexts:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_partial_completion(self):
        """Test wait_for_drain when some events complete before timeout."""
        controller = FlowController()

        # Acquire 5 slots
        contexts = [await controller.acquire() for _ in range(5)]

        async def release_some():
            await asyncio.sleep(0.05)
            # Release only 3
            for ctx in contexts[:3]:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_some())

        # Wait with timeout that allows some but not all to complete
        remaining = await controller.wait_for_drain(timeout=0.1)

        await release_task
        # 2 events should remain (5 - 3 released)
        assert remaining == 2

        # Cleanup remaining
        for ctx in contexts[3:]:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_concurrent_acquire_release(self):
        """Test wait_for_drain with concurrent acquire/release operations."""
        controller = FlowController()

        async def worker(worker_id: int):
            for _ in range(10):
                async with await controller.acquire():
                    await asyncio.sleep(0.01)

        # Start workers
        workers = [asyncio.create_task(worker(i)) for i in range(5)]

        # Wait briefly for workers to start
        await asyncio.sleep(0.05)

        # Workers are still running, some events in flight
        # Wait for all to complete
        remaining = await controller.wait_for_drain(timeout=5.0)

        await asyncio.gather(*workers)
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_drain_event_state_transitions(self):
        """Test that drain event is properly set/cleared."""
        controller = FlowController()

        # Initially drain event should be set (no events)
        assert controller._drain_event.is_set()

        # After acquire, should be cleared
        ctx = await controller.acquire()
        assert not controller._drain_event.is_set()

        # After release back to 0, should be set
        await ctx.__aexit__(None, None, None)
        assert controller._drain_event.is_set()

    @pytest.mark.asyncio
    async def test_wait_for_drain_with_zero_timeout(self):
        """Test wait_for_drain with zero timeout returns immediately."""
        controller = FlowController()

        # Acquire a slot
        ctx = await controller.acquire()
        assert controller.in_flight == 1

        # Zero timeout should return immediately with in-flight count
        remaining = await controller.wait_for_drain(timeout=0.0)
        assert remaining == 1

        # Cleanup
        await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_with_very_short_timeout(self):
        """Test wait_for_drain with very short timeout."""
        controller = FlowController()

        # Acquire slots
        contexts = [await controller.acquire() for _ in range(2)]

        # Very short timeout - should timeout before events drain
        remaining = await controller.wait_for_drain(timeout=0.001)
        assert remaining == 2

        # Cleanup
        for ctx in contexts:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_wait_for_drain_multiple_calls(self):
        """Test multiple sequential wait_for_drain calls."""
        controller = FlowController()

        # First call with no events
        remaining1 = await controller.wait_for_drain(timeout=1.0)
        assert remaining1 == 0

        # Acquire some slots
        contexts = [await controller.acquire() for _ in range(3)]

        # Start release in background
        async def release_all():
            await asyncio.sleep(0.05)
            for ctx in contexts:
                await ctx.__aexit__(None, None, None)

        release_task = asyncio.create_task(release_all())

        # Second call should wait and complete
        remaining2 = await controller.wait_for_drain(timeout=5.0)
        assert remaining2 == 0

        await release_task

        # Third call after all drained
        remaining3 = await controller.wait_for_drain(timeout=1.0)
        assert remaining3 == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_interleaved_acquire_release(self):
        """Test wait_for_drain with interleaved acquire/release operations."""
        controller = FlowController()

        # Track events during test
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

        # Start multiple workers
        workers = [asyncio.create_task(worker()) for _ in range(3)]

        # Wait briefly for some events to start
        await asyncio.sleep(0.03)

        # Wait for drain - all workers should complete
        remaining = await controller.wait_for_drain(timeout=5.0)

        await asyncio.gather(*workers)
        assert remaining == 0
        assert events_completed == 15  # 3 workers * 5 events each
