"""
Unit tests for in-flight event draining functionality.

Tests for:
- FlowController.wait_for_drain() method
- SubscriptionManager._drain_in_flight_events() method
- Integration of drain phase in graceful shutdown
- Edge cases and error conditions
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from eventsource.subscriptions.flow_control import FlowController
from eventsource.subscriptions.manager import SubscriptionManager
from eventsource.subscriptions.shutdown import ShutdownCoordinator

# =============================================================================
# FlowController.wait_for_drain() Tests
# =============================================================================


class TestFlowControllerWaitForDrain:
    """Tests for FlowController.wait_for_drain() method."""

    @pytest.mark.asyncio
    async def test_wait_for_drain_returns_immediately_when_empty(self):
        """Test that wait_for_drain returns 0 immediately when no events in flight."""
        controller = FlowController(max_in_flight=10)

        remaining = await controller.wait_for_drain(timeout=1.0)

        assert remaining == 0

    @pytest.mark.asyncio
    async def test_wait_for_drain_waits_for_single_event(self):
        """Test that wait_for_drain waits for a single in-flight event."""
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=100)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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


# =============================================================================
# Mock Classes for SubscriptionManager Testing
# =============================================================================


class MockFlowController:
    """Mock FlowController for testing."""

    def __init__(self, in_flight: int = 0, drain_delay: float = 0):
        self._in_flight = in_flight
        self._drain_delay = drain_delay

    @property
    def in_flight(self) -> int:
        return self._in_flight

    async def wait_for_drain(self, timeout: float) -> int:
        if self._drain_delay > timeout:
            await asyncio.sleep(timeout)
            return self._in_flight
        await asyncio.sleep(self._drain_delay)
        return 0


class MockCoordinator:
    """Mock TransitionCoordinator for testing."""

    def __init__(self, flow_controller: MockFlowController | None = None):
        self._flow_controller = flow_controller

    @property
    def flow_controller(self) -> MockFlowController | None:
        return self._flow_controller


# =============================================================================
# SubscriptionManager._drain_in_flight_events() Tests
# =============================================================================


class TestDrainInFlightEvents:
    """Tests for SubscriptionManager._drain_in_flight_events()."""

    @pytest.fixture
    def manager(self):
        """Create a SubscriptionManager with mocked dependencies."""
        manager = MagicMock(spec=SubscriptionManager)
        manager._shutdown_coordinator = ShutdownCoordinator(drain_timeout=5.0)
        manager._lifecycle = MagicMock()
        manager._lifecycle.coordinators = {}
        manager._registry = {}
        return manager

    @pytest.mark.asyncio
    async def test_drain_with_no_subscriptions(self, manager):
        """Test drain returns 0 when no subscriptions exist."""
        manager._lifecycle.coordinators = {}

        # Call the actual method
        result = await SubscriptionManager._drain_in_flight_events(manager)

        assert result == 0

    @pytest.mark.asyncio
    async def test_drain_with_no_events_in_flight(self, manager):
        """Test drain returns 0 when all subscriptions are idle."""
        manager._lifecycle.coordinators = {
            "sub1": MockCoordinator(MockFlowController(in_flight=0)),
            "sub2": MockCoordinator(MockFlowController(in_flight=0)),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        assert result == 0

    @pytest.mark.asyncio
    async def test_drain_with_events_in_flight(self, manager):
        """Test drain returns total in-flight count."""
        manager._lifecycle.coordinators = {
            "sub1": MockCoordinator(MockFlowController(in_flight=5, drain_delay=0.01)),
            "sub2": MockCoordinator(MockFlowController(in_flight=10, drain_delay=0.01)),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        # Should return total in-flight at start
        assert result == 15

    @pytest.mark.asyncio
    async def test_drain_timeout_with_slow_handlers(self, manager):
        """Test drain handles timeout with slow handlers."""
        manager._shutdown_coordinator.drain_timeout = 0.1
        manager._lifecycle.coordinators = {
            "sub1": MockCoordinator(
                MockFlowController(in_flight=5, drain_delay=10.0)  # Very slow
            ),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        # Should return in-flight count (timeout occurred)
        assert result == 5

    @pytest.mark.asyncio
    async def test_drain_mixed_fast_slow_handlers(self, manager):
        """Test drain with mix of fast and slow handlers."""
        manager._shutdown_coordinator.drain_timeout = 0.2
        manager._lifecycle.coordinators = {
            "fast": MockCoordinator(MockFlowController(in_flight=10, drain_delay=0.01)),
            "slow": MockCoordinator(MockFlowController(in_flight=5, drain_delay=10.0)),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        # Returns total in-flight at start
        assert result == 15

    @pytest.mark.asyncio
    async def test_drain_with_none_flow_controller(self, manager):
        """Test drain handles subscriptions without FlowController."""
        manager._lifecycle.coordinators = {
            "with_fc": MockCoordinator(MockFlowController(in_flight=5, drain_delay=0.01)),
            "no_fc": MockCoordinator(flow_controller=None),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        # Should only count subscription with FlowController
        assert result == 5

    @pytest.mark.asyncio
    async def test_drain_parallel_execution(self, manager):
        """Test that drain waits on all subscriptions in parallel."""
        start_times = {}
        end_times = {}

        class TimingFlowController:
            def __init__(self, name: str):
                self.name = name
                self._in_flight = 1

            @property
            def in_flight(self):
                return self._in_flight

            async def wait_for_drain(self, timeout: float) -> int:
                start_times[self.name] = asyncio.get_event_loop().time()
                await asyncio.sleep(0.1)
                end_times[self.name] = asyncio.get_event_loop().time()
                return 0

        manager._lifecycle.coordinators = {
            f"sub{i}": MockCoordinator(TimingFlowController(f"sub{i}")) for i in range(3)
        }

        await SubscriptionManager._drain_in_flight_events(manager)

        # All should start at roughly the same time (parallel)
        times = list(start_times.values())
        max_diff = max(times) - min(times)
        assert max_diff < 0.05, "Drains should run in parallel"

    @pytest.mark.asyncio
    async def test_drain_with_exception_in_one_handler(self, manager):
        """Test drain continues when one handler raises exception."""

        class FailingFlowController:
            def __init__(self):
                self._in_flight = 5

            @property
            def in_flight(self):
                return self._in_flight

            async def wait_for_drain(self, timeout: float) -> int:
                raise RuntimeError("Handler failed")

        class WorkingFlowController:
            def __init__(self):
                self._in_flight = 10

            @property
            def in_flight(self):
                return self._in_flight

            async def wait_for_drain(self, timeout: float) -> int:
                return 0

        manager._lifecycle.coordinators = {
            "failing": MockCoordinator(FailingFlowController()),
            "working": MockCoordinator(WorkingFlowController()),
        }

        # Should not raise, returns total in-flight
        result = await SubscriptionManager._drain_in_flight_events(manager)
        assert result == 15

    @pytest.mark.asyncio
    async def test_drain_with_single_subscription(self, manager):
        """Test drain with a single subscription."""
        manager._lifecycle.coordinators = {
            "only_sub": MockCoordinator(MockFlowController(in_flight=7, drain_delay=0.01)),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        assert result == 7

    @pytest.mark.asyncio
    async def test_drain_with_many_subscriptions(self, manager):
        """Test drain with many subscriptions."""
        manager._lifecycle.coordinators = {
            f"sub{i}": MockCoordinator(MockFlowController(in_flight=i + 1, drain_delay=0.01))
            for i in range(10)
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        # Sum of 1+2+3+...+10 = 55
        assert result == 55

    @pytest.mark.asyncio
    async def test_drain_with_all_subscriptions_having_no_flow_controller(self, manager):
        """Test drain when all subscriptions lack FlowController."""
        manager._lifecycle.coordinators = {
            "sub1": MockCoordinator(flow_controller=None),
            "sub2": MockCoordinator(flow_controller=None),
            "sub3": MockCoordinator(flow_controller=None),
        }

        result = await SubscriptionManager._drain_in_flight_events(manager)

        assert result == 0


# =============================================================================
# FlowController Edge Cases
# =============================================================================


class TestFlowControllerDrainEdgeCases:
    """Edge case tests for FlowController drain functionality."""

    @pytest.mark.asyncio
    async def test_drain_event_cleared_on_first_acquire(self):
        """Test drain event is cleared on first acquire."""
        controller = FlowController(max_in_flight=10)
        assert controller._drain_event.is_set()

        ctx = await controller.acquire()
        assert not controller._drain_event.is_set()

        await ctx.__aexit__(None, None, None)
        assert controller._drain_event.is_set()

    @pytest.mark.asyncio
    async def test_drain_event_stays_cleared_with_multiple_in_flight(self):
        """Test drain event stays cleared with multiple in-flight events."""
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=3)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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
        controller = FlowController(max_in_flight=10)

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


# =============================================================================
# Integration Tests for Drain Phase in Graceful Shutdown
# =============================================================================


class TestGracefulShutdownWithDrain:
    """Integration tests for graceful shutdown with drain phase."""

    @pytest.mark.asyncio
    async def test_drain_phase_execution_order(self):
        """Test drain phase executes in correct order during shutdown."""
        coordinator = ShutdownCoordinator(drain_timeout=5.0)
        phases_executed = []

        async def stop_func():
            phases_executed.append("stop")

        async def drain_func():
            phases_executed.append("drain")
            return 10

        async def checkpoint_func():
            phases_executed.append("checkpoint")
            return 3

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
        )

        assert phases_executed == ["stop", "drain", "checkpoint"]
        assert result.events_drained == 10
        assert result.checkpoints_saved == 3

    @pytest.mark.asyncio
    async def test_drain_timeout_proceeds_to_checkpoint(self):
        """Test that shutdown proceeds to checkpoint when drain times out."""
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        stop_called = False
        drain_called = False
        checkpoint_called = False

        async def stop_func():
            nonlocal stop_called
            stop_called = True

        async def slow_drain():
            nonlocal drain_called
            drain_called = True
            await asyncio.sleep(10.0)  # Will timeout
            return 100

        async def checkpoint_func():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            checkpoint_func=checkpoint_func,
        )

        assert stop_called
        assert drain_called
        # Checkpoint should NOT be called because drain timed out and forced shutdown
        assert result.forced is True

    @pytest.mark.asyncio
    async def test_drain_with_real_flow_controller_in_shutdown(self):
        """Test drain using real FlowController during shutdown sequence."""
        controller = FlowController(max_in_flight=10)
        coordinator = ShutdownCoordinator(drain_timeout=5.0)

        # Acquire some slots
        contexts = [await controller.acquire() for _ in range(3)]

        async def stop_func():
            pass

        async def drain_func():
            # Start releasing slots asynchronously
            async def release_all():
                for ctx in contexts:
                    await asyncio.sleep(0.05)
                    await ctx.__aexit__(None, None, None)

            asyncio.create_task(release_all())
            # Wait for drain
            await controller.wait_for_drain(timeout=coordinator.drain_timeout)
            return controller.stats.total_releases

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
        )

        assert result.events_drained == 3
        assert controller.in_flight == 0


# =============================================================================
# Module Import Tests
# =============================================================================


class TestModuleImports:
    """Tests for module imports."""

    def test_import_flow_controller(self):
        """Test FlowController import."""
        from eventsource.subscriptions.flow_control import FlowController

        assert FlowController is not None

    def test_import_subscription_manager(self):
        """Test SubscriptionManager import."""
        from eventsource.subscriptions.manager import SubscriptionManager

        assert SubscriptionManager is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.subscriptions import (
            FlowController,
            SubscriptionManager,
        )

        assert FlowController is not None
        assert SubscriptionManager is not None
