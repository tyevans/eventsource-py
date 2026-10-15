"""Unit tests for post-shutdown hook functionality and imports."""

import asyncio

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)


def _stopped_result() -> ShutdownResult:
    return ShutdownResult(
        phase=ShutdownPhase.STOPPED,
        duration_seconds=5.0,
        subscriptions_stopped=1,
        events_drained=10,
    )


async def _noop_hook(result: ShutdownResult) -> None:
    pass


async def _noop_stop() -> None:
    pass


class TestPostShutdownHooks:
    """Tests for post-shutdown hook functionality."""

    def test_register_hook(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_post_shutdown(_noop_hook)
        assert len(coordinator._post_shutdown_hooks) == 1

    def test_register_hook_validates_async(self):
        coordinator = ShutdownCoordinator()

        def sync_hook(result):
            pass

        with pytest.raises(TypeError):
            coordinator.on_post_shutdown(sync_hook)

    def test_register_multiple_hooks(self):
        coordinator = ShutdownCoordinator()

        async def hook2(result):
            pass

        coordinator.on_post_shutdown(_noop_hook)
        coordinator.on_post_shutdown(hook2)
        assert len(coordinator._post_shutdown_hooks) == 2

    @pytest.mark.asyncio
    async def test_hooks_receive_result(self):
        coordinator = ShutdownCoordinator()
        received_result = None

        async def capture_result(result: ShutdownResult):
            nonlocal received_result
            received_result = result

        coordinator.on_post_shutdown(capture_result)
        test_result = _stopped_result()
        await coordinator._execute_post_shutdown_hooks(test_result)
        assert received_result is test_result

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        coordinator = ShutdownCoordinator()
        execution_order = []

        async def hook1(result):
            execution_order.append(1)

        async def hook2(result):
            execution_order.append(2)

        coordinator.on_post_shutdown(hook1)
        coordinator.on_post_shutdown(hook2)
        await coordinator._execute_post_shutdown_hooks(_stopped_result())
        assert execution_order == [1, 2]

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        coordinator = ShutdownCoordinator()
        hook2_executed = False

        async def failing_hook(result):
            raise RuntimeError("Hook failed")

        async def success_hook(result):
            nonlocal hook2_executed
            hook2_executed = True

        coordinator.on_post_shutdown(failing_hook)
        coordinator.on_post_shutdown(success_hook)
        await coordinator._execute_post_shutdown_hooks(_stopped_result())
        assert hook2_executed

    @pytest.mark.asyncio
    async def test_hook_timeout_continues_execution(self):
        coordinator = ShutdownCoordinator()
        hook2_executed = False

        async def slow_hook(result):
            await asyncio.sleep(10.0)

        async def success_hook(result):
            nonlocal hook2_executed
            hook2_executed = True

        coordinator.on_post_shutdown(slow_hook)
        coordinator.on_post_shutdown(success_hook)
        await coordinator._execute_post_shutdown_hooks(_stopped_result())
        assert hook2_executed

    @pytest.mark.asyncio
    async def test_hooks_execute_after_forced_shutdown(self):
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
        coordinator = ShutdownCoordinator()
        hook_called = False
        received_result = None

        async def track_call(result: ShutdownResult):
            nonlocal hook_called, received_result
            hook_called = True
            received_result = result

        coordinator.on_post_shutdown(track_call)
        result = await coordinator.shutdown(stop_func=_noop_stop)
        assert hook_called
        assert received_result is result

    @pytest.mark.asyncio
    async def test_hooks_receive_full_result_info(self):
        coordinator = ShutdownCoordinator()
        received_result = None

        async def capture_result(result: ShutdownResult):
            nonlocal received_result
            received_result = result

        async def drain_func():
            return 42

        async def checkpoint_func():
            return 5

        coordinator.on_post_shutdown(capture_result)
        await coordinator.shutdown(
            stop_func=_noop_stop, drain_func=drain_func, checkpoint_func=checkpoint_func
        )
        assert received_result is not None
        assert received_result.events_drained == 42
        assert received_result.checkpoints_saved == 5
        assert received_result.phase == ShutdownPhase.STOPPED

    def test_reset_clears_hooks(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_post_shutdown(_noop_hook)
        assert len(coordinator._post_shutdown_hooks) == 1
        coordinator.reset()
        assert len(coordinator._post_shutdown_hooks) == 0

    @pytest.mark.asyncio
    async def test_no_hooks_execution_is_safe(self):
        coordinator = ShutdownCoordinator()
        await coordinator._execute_post_shutdown_hooks(_stopped_result())

    @pytest.mark.asyncio
    async def test_hooks_execute_after_all_phases(self):
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
        assert phase_order == ["stop", "drain", "checkpoint", "close", "post_hook"]

    @pytest.mark.asyncio
    async def test_hooks_can_access_result_reason(self):
        coordinator = ShutdownCoordinator()
        received_reason = None

        async def check_reason(result: ShutdownResult):
            nonlocal received_reason
            received_reason = result.reason

        coordinator.on_post_shutdown(check_reason)
        coordinator.request_shutdown(ShutdownReason.PROGRAMMATIC)
        await coordinator.shutdown(stop_func=_noop_stop)
        assert received_reason == ShutdownReason.PROGRAMMATIC

    @pytest.mark.asyncio
    async def test_hooks_execute_even_when_shutdown_errors(self):
        coordinator = ShutdownCoordinator()
        hook_executed = False

        async def track_hook(result: ShutdownResult):
            nonlocal hook_executed
            hook_executed = True

        async def failing_stop():
            raise RuntimeError("Stop failed")

        coordinator.on_post_shutdown(track_hook)
        result = await coordinator.shutdown(stop_func=failing_stop)
        assert hook_executed
        assert result.forced
        assert result.error is not None


class TestPostShutdownHookImports:
    """Tests for post-shutdown hook type imports."""

    def test_import_post_shutdown_hook_type(self):
        from eventsource.application.subscriptions.shutdown import PostShutdownHook

        assert PostShutdownHook is not None

    def test_post_shutdown_hook_in_all(self):
        from eventsource.application.subscriptions.shutdown import __all__

        assert "PostShutdownHook" in __all__
