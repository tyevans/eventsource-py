"""Unit tests for SubscriptionManager._drain_in_flight_events()."""

import asyncio

import pytest

from eventsource.application.subscriptions.manager import SubscriptionManager
from tests.unit.application.subscriptions.test_drain._fixtures import (
    MockCoordinator,
    MockFlowController,
)


class TestDrainInFlightEvents:
    """Tests for SubscriptionManager._drain_in_flight_events()."""

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
