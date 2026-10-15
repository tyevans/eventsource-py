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


class TestPreShutdownHooks:
    """Tests for pre-shutdown hook functionality."""

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
        coordinator.on_pre_shutdown(h1, timeout=5.0)
        coordinator.on_pre_shutdown(h2, timeout=10.0)
        coordinator.on_pre_shutdown(h3, timeout=2.0)
        assert coordinator._pre_shutdown_hooks == [(h1, 5.0), (h2, 10.0), (h3, 2.0)]

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        coordinator = ShutdownCoordinator()
        order: list[int] = []

        async def h1():
            order.append(1)

        async def h2():
            order.append(2)

        async def h3():
            order.append(3)

        for h in (h1, h2, h3):
            coordinator.on_pre_shutdown(h)
        await coordinator._execute_pre_shutdown_hooks()
        assert order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_hook_timeout(self):
        coordinator = ShutdownCoordinator()
        completed = False

        async def slow():
            nonlocal completed
            await asyncio.sleep(10.0)
            completed = True

        coordinator.on_pre_shutdown(slow, timeout=0.1)
        start = asyncio.get_event_loop().time()
        await coordinator._execute_pre_shutdown_hooks()
        assert not completed
        assert asyncio.get_event_loop().time() - start < 0.5

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        coordinator = ShutdownCoordinator()
        success = AsyncMock()
        coordinator.on_pre_shutdown(_failing_hook)
        coordinator.on_pre_shutdown(success)
        await coordinator._execute_pre_shutdown_hooks()
        success.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hooks_execute_before_stopping_phase(self):
        coordinator = ShutdownCoordinator()
        phase_at_hook = None

        async def capture_phase():
            nonlocal phase_at_hook
            phase_at_hook = coordinator.phase

        coordinator.on_pre_shutdown(capture_phase)
        await coordinator.shutdown(stop_func=_noop_hook)
        assert phase_at_hook == ShutdownPhase.RUNNING

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
        coordinator = ShutdownCoordinator()
        pre_hook = AsyncMock()
        coordinator.on_pre_shutdown(pre_hook)
        await coordinator.shutdown(stop_func=_noop_hook)
        pre_hook.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hook_failure_does_not_prevent_shutdown(self):
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        coordinator.on_pre_shutdown(_failing_hook)
        result = await coordinator.shutdown(stop_func=stop_func)
        stop_func.assert_awaited_once()
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_hook_timeout_does_not_prevent_shutdown(self):
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        coordinator.on_pre_shutdown(_slow_hook, timeout=0.1)
        result = await coordinator.shutdown(stop_func=stop_func)
        stop_func.assert_awaited_once()
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_multiple_hooks_with_different_timeouts(self):
        coordinator = ShutdownCoordinator()
        results: list[str] = []

        async def fast_hook():
            results.append("fast")

        async def slow_hook():
            await asyncio.sleep(10.0)
            results.append("slow")

        async def medium_hook():
            await asyncio.sleep(0.05)
            results.append("medium")

        coordinator.on_pre_shutdown(fast_hook, timeout=1.0)
        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)
        coordinator.on_pre_shutdown(medium_hook, timeout=1.0)
        await coordinator._execute_pre_shutdown_hooks()
        assert results == ["fast", "medium"]

    @pytest.mark.asyncio
    async def test_hooks_execute_before_drain_and_checkpoint(self):
        coordinator = ShutdownCoordinator()
        times: dict[str, float] = {}

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
