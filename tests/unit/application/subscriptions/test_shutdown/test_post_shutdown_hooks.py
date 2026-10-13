"""Unit tests for post-shutdown hook functionality and imports."""

import asyncio

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)


class TestPostShutdownHooks:
    """Tests for post-shutdown hook functionality."""

    def test_register_hook(self):
        """Test registering a post-shutdown hook."""
        coordinator = ShutdownCoordinator()

        async def hook(result: ShutdownResult):
            pass

        coordinator.on_post_shutdown(hook)
        assert len(coordinator._post_shutdown_hooks) == 1

    def test_register_hook_validates_async(self):
        """Test that non-async hooks are rejected."""
        coordinator = ShutdownCoordinator()

        def sync_hook(result):
            pass

        with pytest.raises(TypeError):
            coordinator.on_post_shutdown(sync_hook)

    def test_register_multiple_hooks(self):
        """Test registering multiple hooks."""
        coordinator = ShutdownCoordinator()

        async def hook1(result):
            pass

        async def hook2(result):
            pass

        coordinator.on_post_shutdown(hook1)
        coordinator.on_post_shutdown(hook2)
        assert len(coordinator._post_shutdown_hooks) == 2

    @pytest.mark.asyncio
    async def test_hooks_receive_result(self):
        """Test hooks receive ShutdownResult."""
        coordinator = ShutdownCoordinator()
        received_result = None

        async def capture_result(result: ShutdownResult):
            nonlocal received_result
            received_result = result

        coordinator.on_post_shutdown(capture_result)

        test_result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(test_result)

        assert received_result is test_result

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        """Test hooks execute in registration order."""
        coordinator = ShutdownCoordinator()
        execution_order = []

        async def hook1(result):
            execution_order.append(1)

        async def hook2(result):
            execution_order.append(2)

        coordinator.on_post_shutdown(hook1)
        coordinator.on_post_shutdown(hook2)

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert execution_order == [1, 2]

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        """Test that hook exception doesn't stop other hooks."""
        coordinator = ShutdownCoordinator()
        hook2_executed = False

        async def failing_hook(result):
            raise RuntimeError("Hook failed")

        async def success_hook(result):
            nonlocal hook2_executed
            hook2_executed = True

        coordinator.on_post_shutdown(failing_hook)
        coordinator.on_post_shutdown(success_hook)

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert hook2_executed

    @pytest.mark.asyncio
    async def test_hook_timeout_continues_execution(self):
        """Test that hook timeout doesn't stop other hooks."""
        coordinator = ShutdownCoordinator()
        hook2_executed = False

        async def slow_hook(result):
            await asyncio.sleep(10.0)  # Will timeout (5s limit)

        async def success_hook(result):
            nonlocal hook2_executed
            hook2_executed = True

        coordinator.on_post_shutdown(slow_hook)
        coordinator.on_post_shutdown(success_hook)

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert hook2_executed

    @pytest.mark.asyncio
    async def test_hooks_execute_after_forced_shutdown(self):
        """Test hooks run even after forced shutdown."""
        coordinator = ShutdownCoordinator()
        hook_executed = False
        received_forced = None

        async def check_forced(result: ShutdownResult):
            nonlocal hook_executed, received_forced
            hook_executed = True
            received_forced = result.forced

        coordinator.on_post_shutdown(check_forced)

        result = ShutdownResult(
            phase=ShutdownPhase.FORCED,
            duration_seconds=10.0,
            subscriptions_stopped=1,
            events_drained=0,
            forced=True,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert hook_executed
        assert received_forced is True

    @pytest.mark.asyncio
    async def test_hooks_execute_via_shutdown(self):
        """Test hooks are called from shutdown() method."""
        coordinator = ShutdownCoordinator()
        hook_called = False
        received_result = None

        async def track_call(result: ShutdownResult):
            nonlocal hook_called, received_result
            hook_called = True
            received_result = result

        coordinator.on_post_shutdown(track_call)

        async def stop_func():
            pass

        result = await coordinator.shutdown(stop_func=stop_func)

        assert hook_called
        assert received_result is result

    @pytest.mark.asyncio
    async def test_hooks_receive_full_result_info(self):
        """Test hooks receive complete ShutdownResult info."""
        coordinator = ShutdownCoordinator()
        received_result = None

        async def capture_result(result: ShutdownResult):
            nonlocal received_result
            received_result = result

        coordinator.on_post_shutdown(capture_result)

        async def stop_func():
            pass

        async def drain_func():
            return 42

        async def checkpoint_func():
            return 5

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
        )

        assert received_result is not None
        assert received_result.events_drained == 42
        assert received_result.checkpoints_saved == 5
        assert received_result.phase == ShutdownPhase.STOPPED

    def test_reset_clears_hooks(self):
        """Test reset clears registered hooks."""
        coordinator = ShutdownCoordinator()

        async def hook(result):
            pass

        coordinator.on_post_shutdown(hook)
        assert len(coordinator._post_shutdown_hooks) == 1

        coordinator.reset()
        assert len(coordinator._post_shutdown_hooks) == 0

    @pytest.mark.asyncio
    async def test_no_hooks_execution_is_safe(self):
        """Test that executing with no hooks registered is safe."""
        coordinator = ShutdownCoordinator()

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        # Should not raise
        await coordinator._execute_post_shutdown_hooks(result)

    @pytest.mark.asyncio
    async def test_hooks_execute_after_all_phases(self):
        """Test hooks execute after all shutdown phases complete."""
        coordinator = ShutdownCoordinator()
        phase_order = []

        async def stop_func():
            phase_order.append("stop")

        async def drain_func():
            phase_order.append("drain")
            return 0

        async def checkpoint_func():
            phase_order.append("checkpoint")
            return 0

        async def close_func():
            phase_order.append("close")

        async def post_hook(result):
            phase_order.append("post_hook")

        coordinator.on_post_shutdown(post_hook)

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
            close_func=close_func,
        )

        # Post-shutdown hooks should be last
        assert phase_order == ["stop", "drain", "checkpoint", "close", "post_hook"]

    @pytest.mark.asyncio
    async def test_hooks_can_access_result_reason(self):
        """Test hooks can access ShutdownResult.reason."""
        coordinator = ShutdownCoordinator()
        received_reason = None

        async def check_reason(result: ShutdownResult):
            nonlocal received_reason
            received_reason = result.reason

        coordinator.on_post_shutdown(check_reason)
        coordinator.request_shutdown(ShutdownReason.PROGRAMMATIC)

        async def stop_func():
            pass

        await coordinator.shutdown(stop_func=stop_func)

        assert received_reason == ShutdownReason.PROGRAMMATIC

    @pytest.mark.asyncio
    async def test_hooks_execute_even_when_shutdown_errors(self):
        """Test hooks execute even when shutdown has errors."""
        coordinator = ShutdownCoordinator()
        hook_executed = False

        async def track_hook(result: ShutdownResult):
            nonlocal hook_executed
            hook_executed = True

        coordinator.on_post_shutdown(track_hook)

        async def failing_stop():
            raise RuntimeError("Stop failed")

        result = await coordinator.shutdown(stop_func=failing_stop)

        assert hook_executed
        assert result.forced
        assert result.error is not None


class TestPostShutdownHookImports:
    """Tests for post-shutdown hook type imports."""

    def test_import_post_shutdown_hook_type(self):
        """Test PostShutdownHook type alias import."""
        from eventsource.application.subscriptions.shutdown import PostShutdownHook

        assert PostShutdownHook is not None

    def test_post_shutdown_hook_in_all(self):
        """Test PostShutdownHook is in __all__."""
        from eventsource.application.subscriptions.shutdown import __all__

        assert "PostShutdownHook" in __all__
