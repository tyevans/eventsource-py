"""Unit tests for post-shutdown hook functionality and imports."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    PostShutdownHook,
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)
from eventsource.application.subscriptions.shutdown import (
    __all__ as shutdown_all,
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


async def _failing_hook(result: ShutdownResult) -> None:
    raise RuntimeError("Hook failed")


async def _slow_hook(result: ShutdownResult) -> None:
    await asyncio.sleep(10.0)


class TestPostShutdownHooks:
    """Tests for post-shutdown hook functionality."""

    def test_register_hook(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_post_shutdown(_noop_hook)
        assert len(coordinator._post_shutdown_hooks) == 1

    def test_register_hook_validates_async(self):
        def sync_hook(result):
            pass

        with pytest.raises(TypeError):
            ShutdownCoordinator().on_post_shutdown(sync_hook)

    def test_register_multiple_hooks(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_post_shutdown(_noop_hook)
        coordinator.on_post_shutdown(AsyncMock())
        assert len(coordinator._post_shutdown_hooks) == 2

    @pytest.mark.asyncio
    async def test_hooks_receive_result(self):
        coordinator = ShutdownCoordinator()
        hook = AsyncMock()
        coordinator.on_post_shutdown(hook)
        test_result = _stopped_result()
        await coordinator._execute_post_shutdown_hooks(test_result)
        hook.assert_awaited_once_with(test_result)

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        coordinator = ShutdownCoordinator()
        order: list[int] = []

        async def hook1(result):
            order.append(1)

        async def hook2(result):
            order.append(2)

        coordinator.on_post_shutdown(hook1)
        coordinator.on_post_shutdown(hook2)
        await coordinator._execute_post_shutdown_hooks(_stopped_result())
        assert order == [1, 2]

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        coordinator = ShutdownCoordinator()
        success_hook = AsyncMock()
        coordinator.on_post_shutdown(_failing_hook)
        coordinator.on_post_shutdown(success_hook)
        await coordinator._execute_post_shutdown_hooks(_stopped_result())
        success_hook.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hook_timeout_continues_execution(self):
        coordinator = ShutdownCoordinator()
        success_hook = AsyncMock()
        coordinator.on_post_shutdown(_slow_hook)
        coordinator.on_post_shutdown(success_hook)
        await coordinator._execute_post_shutdown_hooks(_stopped_result())
        success_hook.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hooks_execute_after_forced_shutdown(self):
        coordinator = ShutdownCoordinator()
        received: list[ShutdownResult] = []

        async def check_forced(result: ShutdownResult):
            received.append(result)

        coordinator.on_post_shutdown(check_forced)
        result = ShutdownResult(
            phase=ShutdownPhase.FORCED,
            duration_seconds=10.0,
            subscriptions_stopped=1,
            events_drained=0,
            forced=True,
        )
        await coordinator._execute_post_shutdown_hooks(result)
        assert len(received) == 1 and received[0].forced is True

    @pytest.mark.asyncio
    async def test_hooks_execute_via_shutdown(self):
        coordinator = ShutdownCoordinator()
        hook = AsyncMock()
        coordinator.on_post_shutdown(hook)
        result = await coordinator.shutdown(stop_func=_noop_stop)
        hook.assert_awaited_once_with(result)

    @pytest.mark.asyncio
    async def test_hooks_receive_full_result_info(self):
        coordinator = ShutdownCoordinator()
        hook = AsyncMock()
        coordinator.on_post_shutdown(hook)
        await coordinator.shutdown(
            stop_func=_noop_stop,
            drain_func=AsyncMock(return_value=42),
            checkpoint_func=AsyncMock(return_value=5),
        )
        received_result = hook.call_args[0][0]
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
        await ShutdownCoordinator()._execute_post_shutdown_hooks(_stopped_result())

    @pytest.mark.asyncio
    async def test_hooks_execute_after_all_phases(self):
        coordinator = ShutdownCoordinator()
        phase_order: list[str] = []

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
        hook = AsyncMock()
        coordinator.on_post_shutdown(hook)
        coordinator.request_shutdown(ShutdownReason.PROGRAMMATIC)
        await coordinator.shutdown(stop_func=_noop_stop)
        assert hook.call_args[0][0].reason == ShutdownReason.PROGRAMMATIC

    @pytest.mark.asyncio
    async def test_hooks_execute_even_when_shutdown_errors(self):
        coordinator = ShutdownCoordinator()
        hook = AsyncMock()

        async def failing_stop():
            raise RuntimeError("Stop failed")

        coordinator.on_post_shutdown(hook)
        result = await coordinator.shutdown(stop_func=failing_stop)
        hook.assert_awaited_once()
        assert result.forced and result.error is not None


class TestPostShutdownHookImports:
    def test_import_post_shutdown_hook_type(self):
        assert PostShutdownHook is not None

    def test_post_shutdown_hook_in_all(self):
        assert "PostShutdownHook" in shutdown_all
