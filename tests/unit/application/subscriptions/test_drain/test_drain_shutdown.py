"""Integration tests for drain phase in graceful shutdown and module imports."""

import asyncio

import pytest

from eventsource.application.subscriptions.flow_control import FlowController
from eventsource.application.subscriptions.shutdown import ShutdownCoordinator


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
        controller = FlowController()
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


class TestModuleImports:
    """Tests for module imports."""

    def test_import_flow_controller(self):
        """Test FlowController import."""
        from eventsource.application.subscriptions.flow_control import FlowController

        assert FlowController is not None

    def test_import_subscription_manager(self):
        """Test SubscriptionManager import."""
        from eventsource.application.subscriptions.manager import SubscriptionManager

        assert SubscriptionManager is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.application.subscriptions import (
            FlowController,
            SubscriptionManager,
        )

        assert FlowController is not None
        assert SubscriptionManager is not None
