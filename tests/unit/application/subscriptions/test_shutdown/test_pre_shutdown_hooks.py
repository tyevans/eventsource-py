"""Unit tests for pre-shutdown hook functionality and imports."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    PreShutdownHook,
    ShutdownCoordinator,
    ShutdownPhase,
)
from eventsource.application.subscriptions.shutdown import (
    __all__ as shutdown_all,
)


async def _noop_hook() -> None:
    pass


async def _failing_hook() -> None:
    raise RuntimeError("Hook failed")


async def _slow_hook() -> None:
    await asyncio.sleep(10.0)


def _make_timed_hook(sink: list, value, delay: float = 0.0):
    async def _hook():
        if delay:
            await asyncio.sleep(delay)
        sink.append(value)

    return _hook


class TestPreShutdownHooks:
    def test_register_hook(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_pre_shutdown(_noop_hook, timeout=5.0)
        assert len(coordinator._pre_shutdown_hooks) == 1

    def test_register_hook_default_timeout(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_pre_shutdown(_noop_hook)
        assert coordinator._pre_shutdown_hooks == [(_noop_hook, 5.0)]

    def test_register_hook_validates_async(self):
        def sync_hook():
            pass

        with pytest.raises(TypeError, match="must be async function"):
            ShutdownCoordinator().on_pre_shutdown(sync_hook)  # type: ignore[arg-type]

    def test_register_hook_validates_timeout_zero(self):
        with pytest.raises(ValueError, match="must be positive"):
            ShutdownCoordinator().on_pre_shutdown(_noop_hook, timeout=0)

    def test_register_hook_validates_timeout_negative(self):
        with pytest.raises(ValueError, match="must be positive"):
            ShutdownCoordinator().on_pre_shutdown(_noop_hook, timeout=-1)

    def test_register_multiple_hooks(self):
        coordinator = ShutdownCoordinator()
        h1, h2, h3 = AsyncMock(), AsyncMock(), AsyncMock()
        for h, t in ((h1, 5.0), (h2, 10.0), (h3, 2.0)):
            coordinator.on_pre_shutdown(h, timeout=t)
        assert coordinator._pre_shutdown_hooks == [(h1, 5.0), (h2, 10.0), (h3, 2.0)]

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        coordinator, order = ShutdownCoordinator(), []
        for idx in (1, 2, 3):
            coordinator.on_pre_shutdown(_make_timed_hook(order, idx))
        await coordinator._execute_pre_shutdown_hooks()
        assert order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_hook_timeout(self):
        coordinator, completed = ShutdownCoordinator(), []
        coordinator.on_pre_shutdown(_make_timed_hook(completed, True, 10.0), timeout=0.1)
        start = asyncio.get_event_loop().time()
        await coordinator._execute_pre_shutdown_hooks()
        assert not completed and asyncio.get_event_loop().time() - start < 0.5

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        coordinator, success = ShutdownCoordinator(), AsyncMock()
        coordinator.on_pre_shutdown(_failing_hook)
        coordinator.on_pre_shutdown(success)
        await coordinator._execute_pre_shutdown_hooks()
        success.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hooks_execute_before_stopping_phase(self):
        coordinator, phases = ShutdownCoordinator(), []

        async def capture_phase():
            phases.append(coordinator.phase)

        coordinator.on_pre_shutdown(capture_phase)
        await coordinator.shutdown(stop_func=_noop_hook)
        assert phases == [ShutdownPhase.RUNNING]

    def test_reset_clears_hooks(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_pre_shutdown(_noop_hook)
        assert len(coordinator._pre_shutdown_hooks) == 1
        coordinator.reset()
        assert len(coordinator._pre_shutdown_hooks) == 0

    @pytest.mark.asyncio
    async def test_no_hooks_execution_is_safe(self):
        await ShutdownCoordinator()._execute_pre_shutdown_hooks()

    @pytest.mark.asyncio
    async def test_hooks_run_during_shutdown_sequence(self):
        coordinator, pre_hook = ShutdownCoordinator(), AsyncMock()
        coordinator.on_pre_shutdown(pre_hook)
        await coordinator.shutdown(stop_func=_noop_hook)
        pre_hook.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hook_failure_does_not_prevent_shutdown(self):
        coordinator, stop_func = ShutdownCoordinator(), AsyncMock()
        coordinator.on_pre_shutdown(_failing_hook)
        result = await coordinator.shutdown(stop_func=stop_func)
        stop_func.assert_awaited_once()
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_hook_timeout_does_not_prevent_shutdown(self):
        coordinator, stop_func = ShutdownCoordinator(), AsyncMock()
        coordinator.on_pre_shutdown(_slow_hook, timeout=0.1)
        result = await coordinator.shutdown(stop_func=stop_func)
        stop_func.assert_awaited_once()
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_multiple_hooks_with_different_timeouts(self):
        coordinator, results = ShutdownCoordinator(), []
        specs = (("fast", 0.0, 1.0), ("slow", 10.0, 0.1), ("medium", 0.05, 1.0))
        for label, delay, timeout in specs:
            coordinator.on_pre_shutdown(_make_timed_hook(results, label, delay), timeout=timeout)
        await coordinator._execute_pre_shutdown_hooks()
        assert results == ["fast", "medium"]

    @pytest.mark.asyncio
    async def test_hooks_execute_before_drain_and_checkpoint(self):
        coordinator, times = ShutdownCoordinator(), {}

        async def pre_hook():
            times["hook"] = asyncio.get_event_loop().time()

        async def drain_func():
            times["drain"] = asyncio.get_event_loop().time()
            return 0

        async def checkpoint_func():
            times["checkpoint"] = asyncio.get_event_loop().time()
            return 0

        coordinator.on_pre_shutdown(pre_hook)
        await coordinator.shutdown(
            stop_func=_noop_hook, drain_func=drain_func, checkpoint_func=checkpoint_func
        )
        assert times["hook"] < times["drain"] < times["checkpoint"]


class TestPreShutdownHookImports:
    def test_import_pre_shutdown_hook_type(self):
        assert PreShutdownHook is not None

    def test_pre_shutdown_hook_in_all(self):
        assert "PreShutdownHook" in shutdown_all
